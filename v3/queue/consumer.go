package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/worker"
)

var (
	ErrNoHandlerFound = errors.New("no handler registered for topic")
)

// Consumer handles polling messages from the driver and executing them via worker.Dispatcher.
type Consumer struct {
	driver           Driver
	router           *Router
	workerDispatcher worker.Dispatcher
	batchSize        int
	pollInterval     time.Duration
	retryPolicy      RetryPolicy
	logger           Logger
	quit             chan struct{}
	wg               sync.WaitGroup
	closed           bool
	mu               sync.Mutex
}

// ConsumerOption configures parameters of a Consumer.
type ConsumerOption func(*Consumer)

func WithBatchSize(size int) ConsumerOption {
	return func(c *Consumer) {
		if size > 0 {
			c.batchSize = size
		}
	}
}

func WithPollInterval(interval time.Duration) ConsumerOption {
	return func(c *Consumer) {
		if interval > 0 {
			c.pollInterval = interval
		}
	}
}

func WithRetryPolicy(policy RetryPolicy) ConsumerOption {
	return func(c *Consumer) {
		c.retryPolicy = policy
	}
}

func WithLogger(l Logger) ConsumerOption {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}

func WithWorkerDispatcher(disp worker.Dispatcher) ConsumerOption {
	return func(c *Consumer) {
		if disp != nil {
			c.workerDispatcher = disp
		}
	}
}

// NewConsumer constructs a new queue consumer for a specific driver.
func NewConsumer(d Driver, r *Router, disp worker.Dispatcher, opts ...ConsumerOption) *Consumer {
	if disp == nil {
		disp = worker.GetDispatcher()
	}
	c := &Consumer{
		driver:           d,
		router:           r,
		workerDispatcher: disp,
		batchSize:        50,
		pollInterval:     50 * time.Millisecond,
		retryPolicy:      DefaultRetryPolicy(),
		logger:           DefaultLogger(),
		quit:             make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start launches the consumer polling loop in a background goroutine.
func (c *Consumer) Start() {
	c.wg.Add(1)
	go c.pollLoop()
	c.logger.Info("queue consumer started",
		"driver", c.driver.Name(),
		"poll_interval", c.pollInterval,
	)
}

func (c *Consumer) pollLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			// Adaptive drain loop: if a batch is full, immediately pull the next batch
			// without waiting for the next timer tick. Only sleep when empty or partial.
			for {
				select {
				case <-c.quit:
					return
				default:
				}

				count := c.fetchAndProcess()
				if count < c.batchSize {
					break
				}
			}
		}
	}
}

func (c *Consumer) fetchAndProcess() int {
	ctx := context.Background()
	req := DequeueRequest{
		BatchSize: c.batchSize,
	}
	deliveries, err := c.driver.Dequeue(ctx, req)
	if err != nil {
		c.logger.Error("queue consumer dequeue error", "error", err, "driver", c.driver.Name())
		return 0
	}
	if len(deliveries) == 0 {
		return 0
	}

	// Group deliveries by GroupID
	grouped := make(map[string][]*Delivery)
	var independent []*Delivery

	for _, del := range deliveries {
		if del.Message != nil && del.Message.GroupID != "" {
			grouped[del.Message.GroupID] = append(grouped[del.Message.GroupID], del)
		} else {
			independent = append(independent, del)
		}
	}

	// 1. Process Independent Deliveries (Dispatched to worker pool)
	for _, del := range independent {
		d := del
		m := d.Message
		if m == nil {
			continue
		}
		task := worker.NewTaskFunc(m.Topic, func(tCtx context.Context, subPool worker.SubWorkerPool) error {
			handler, exists := c.router.Get(m.Topic)
			if !exists {
				return c.handleFailure(tCtx, d, ErrNoHandlerFound)
			}

			// Execute handler with access to SubWorkerPool for parallel fan-out
			if err := handler(tCtx, m, subPool); err != nil {
				return c.handleFailure(tCtx, d, err)
			}

			return c.driver.Ack(tCtx, d)
		})

		err := c.workerDispatcher.Dispatch(ctx, task)
		if err != nil {
			// If worker engine task queue is full (backpressure), do not drop message!
			// Nack immediately so broker/adapter can redeliver later when worker has capacity.
			_ = c.driver.Nack(ctx, d, err)
		}
	}

	// 2. Process Grouped Deliveries (Coordinated via SubWorkerPool)
	for groupID, dels := range grouped {
		gID := groupID
		groupDeliveries := dels

		groupTask := worker.NewTaskFunc("group:"+gID, func(tCtx context.Context, subPool worker.SubWorkerPool) error {
			for _, item := range groupDeliveries {
				d := item
				m := d.Message
				if m == nil {
					continue
				}
				subPool.Go(func(sCtx context.Context) error {
					handler, exists := c.router.Get(m.Topic)
					if !exists {
						return c.handleFailure(sCtx, d, ErrNoHandlerFound)
					}

					if err := handler(sCtx, m, subPool); err != nil {
						return c.handleFailure(sCtx, d, err)
					}

					return c.driver.Ack(sCtx, d)
				})
			}
			return subPool.Wait()
		})

		err := c.workerDispatcher.Dispatch(ctx, groupTask)
		if err != nil {
			for _, item := range groupDeliveries {
				_ = c.driver.Nack(ctx, item, err)
			}
		}
	}

	return len(deliveries)
}

func (c *Consumer) handleFailure(ctx context.Context, d *Delivery, reason error) error {
	m := d.Message
	if m == nil {
		return c.driver.Nack(ctx, d, reason)
	}

	maxAttempts := m.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = c.retryPolicy.MaxAttempts
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	// 1. Non-retryable error check
	if !IsRetryable(reason) {
		c.logger.Warn("non-retryable error encountered, routing message directly to DLQ",
			"topic", m.Topic,
			"msg_id", m.ID,
			"error", reason,
		)
		return c.routeToDLQ(ctx, d, reason)
	}

	// 2. Increment core-owned attempt count
	m.Attempt++

	// 3. Exceeded max attempts -> Route to DLQ
	if m.Attempt >= maxAttempts {
		c.logger.Error("message exceeded max attempts, routing to DLQ",
			"topic", m.Topic,
			"msg_id", m.ID,
			"attempts", m.Attempt,
			"max_attempts", maxAttempts,
			"error", reason,
		)
		return c.routeToDLQ(ctx, d, reason)
	}

	// 4. Eligible for retry: calculate backoff
	backoff := c.retryPolicy.Backoff(m.Attempt)
	c.logger.Info("retrying failed message with exponential backoff",
		"topic", m.Topic,
		"msg_id", m.ID,
		"attempt", m.Attempt,
		"max_attempts", maxAttempts,
		"backoff", backoff,
		"error", reason,
	)

	// Acknowledge current delivery to prevent double-delivery by driver
	_ = c.driver.Ack(ctx, d)

	// Re-enqueue: check if adapter supports DelayedEnqueuer
	if delayedAdapter, ok := c.driver.(DelayedEnqueuer); ok {
		return delayedAdapter.EnqueueAt(ctx, m, time.Now().Add(backoff))
	}

	// Core delayed dispatch fallback
	time.AfterFunc(backoff, func() {
		_ = c.driver.Enqueue(context.Background(), m)
	})

	return reason
}

func (c *Consumer) routeToDLQ(ctx context.Context, d *Delivery, reason error) error {
	m := d.Message
	dlqTopic := DLQTopic(m.Topic)

	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers["dlq_reason"] = reason.Error()
	m.Headers["dlq_failed_at"] = time.Now().Format(time.RFC3339)
	m.Headers["original_topic"] = m.Topic

	dlqMsg := &Message{
		ID:          m.ID,
		Topic:       dlqTopic,
		Payload:     m.Payload,
		Priority:    PriorityLow,
		GroupID:     m.GroupID,
		GroupOrder:  m.GroupOrder,
		Headers:     m.Headers,
		CreatedAt:   time.Now(),
		Attempt:     m.Attempt,
		MaxAttempts: m.MaxAttempts,
		Driver:      m.Driver,
	}

	err := c.driver.Enqueue(ctx, dlqMsg)
	if err != nil {
		c.logger.Error("failed to enqueue message to DLQ", "dlq_topic", dlqTopic, "error", err)
		return c.driver.Nack(ctx, d, reason)
	}

	return c.driver.Ack(ctx, d)
}

// Stop gracefully shuts down the consumer loop and driver.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	close(c.quit)
	c.wg.Wait()
	_ = c.driver.Close()
	c.logger.Info("queue consumer stopped gracefully")
}
