package queue

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

var (
	ErrNoHandlerFound = errors.New("no handler registered for topic")
)

type groupLockEntry struct {
	mu   sync.Mutex
	refs int
}

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
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	closed           bool
	mu               sync.Mutex
	closeOnce        sync.Once
	groupLocks       map[string]*groupLockEntry
	groupMu          sync.Mutex
	retryTimers      map[*time.Timer]*Message
	inFlight         sync.WaitGroup
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
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		driver:           d,
		router:           r,
		workerDispatcher: disp,
		batchSize:        50,
		pollInterval:     50 * time.Millisecond,
		retryPolicy:      DefaultRetryPolicy(),
		logger:           DefaultLogger(),
		quit:             make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
		groupLocks:       make(map[string]*groupLockEntry),
		retryTimers:      make(map[*time.Timer]*Message),
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
	ctx := c.ctx
	req := DequeueRequest{
		BatchSize: c.batchSize,
	}
	deliveries, err := c.driver.Dequeue(ctx, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			c.logger.Error("queue consumer dequeue error", "error", err, "driver", c.driver.Name())
		}
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

	// dispatched counts deliveries successfully handed off to the worker engine.
	// Backpressure rejects (ErrQueueFull) are Nacked and NOT counted, so the
	// adaptive drain loop stops pulling new batches when the engine is saturated
	// instead of hot-looping on immediately-redelivered Nacked messages.
	dispatched := 0

	// 1. Process Independent Deliveries (Dispatched to worker pool)
	for _, del := range independent {
		d := del
		m := d.Message
		if m == nil {
			continue
		}
		c.inFlight.Add(1)
		task := worker.NewTaskFunc(m.Topic, func(tCtx context.Context, subPool worker.SubWorkerPool) error {
			defer c.inFlight.Done()
			handler, exists := c.router.Get(m.Topic)
			if !exists {
				return c.handleFailure(tCtx, d, ErrNoHandlerFound)
			}

			// Execute handler with access to SubWorkerPool for parallel fan-out.
			// A handler panic must flow through the failure path (retry/DLQ) so the
			// delivery is never silently leaked as stuck in-flight.
			var handlerErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.logger.Error("handler panic recovered",
							"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID,
							"panic", fmt.Sprint(r), "stack", string(debug.Stack()),
						)
						handlerErr = fmt.Errorf("handler panic on topic %q: %v", m.Topic, r)
					}
				}()
				handlerErr = handler(tCtx, m, subPool)
			}()

			// Join the handler's fan-out before settling the delivery. The
			// handler may spawn sub-workers and (mistakenly) return without
			// calling pool.Wait(); settling first would Ack (or retry) the
			// delivery while its sub-workers are still running, so a crash in
			// between would lose the message's work (at-least-once violation).
			if waitErr := subPool.Wait(); waitErr != nil && handlerErr == nil {
				handlerErr = waitErr
			}

			if handlerErr != nil {
				return c.handleFailure(tCtx, d, handlerErr)
			}

			return c.driver.Ack(tCtx, d)
		})

		err := c.workerDispatcher.Dispatch(ctx, task)
		if err != nil {
			c.inFlight.Done()
			// If worker engine task queue is full (backpressure), do not drop message!
			// Nack immediately so broker/adapter can redeliver later when worker has capacity.
			_ = c.driver.Nack(ctx, d, err)
			continue
		}
		dispatched++
	}

	// 2. Process Grouped Deliveries in strict GroupOrder (FIFO per group).
	// Members of a group are executed sequentially in ascending GroupOrder and a
	// per-group lock serializes the group across consecutive polls, so a later
	// batch can never overtake an in-flight one.
	for groupID, dels := range grouped {
		gID := groupID
		groupDeliveries := dels

		entry := c.acquireGroupLock(gID)

		// The engine's per-task pool is intentionally unused here: grouped
		// members get a fresh pool each (see below) so a sub-worker failure in
		// one member cannot cancel the fan-out of the next.
		c.inFlight.Add(1)
		groupTask := worker.NewTaskFunc("group:"+gID, func(tCtx context.Context, _ worker.SubWorkerPool) error {
			defer c.inFlight.Done()
			// Acquire group lock inside the worker task to serialize processing across batches
			// without blocking the main poller loop (avoids head-of-line blocking).
			entry.mu.Lock()
			defer c.releaseGroupLock(gID, entry)

			ordered := make([]*Delivery, len(groupDeliveries))
			copy(ordered, groupDeliveries)
			sort.SliceStable(ordered, func(i, j int) bool {
				mi, mj := ordered[i].Message, ordered[j].Message
				if mi == nil {
					return false
				}
				if mj == nil {
					return true
				}
				return mi.GroupOrder < mj.GroupOrder
			})

			for _, item := range ordered {
				d := item
				m := d.Message
				if m == nil {
					continue
				}
				handler, exists := c.router.Get(m.Topic)
				if !exists {
					if err := c.handleFailure(tCtx, d, ErrNoHandlerFound); err != nil {
						c.logger.Error("failed to settle message after no-handler failure",
							"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID, "error", err,
						)
					}
					continue
				}

				// A fresh sub-worker pool per member keeps one member's fan-out
				// isolated: its failure/cancellation cannot bleed into the next
				// member, and its sub-workers can be joined before that member is
				// settled (Acked/retried/DLQ'd).
				memberPool := worker.NewSubWorkerPool(tCtx, 0, m.Topic, c.logger)

				// Panics are converted to failures so retry/DLQ logic still runs.
				// Each member is settled (Ack/retry/DLQ) before the next member starts.
				var handlerErr error
				func() {
					defer func() {
						if r := recover(); r != nil {
							c.logger.Error("handler panic recovered",
								"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID,
								"panic", fmt.Sprint(r), "stack", string(debug.Stack()),
							)
							handlerErr = fmt.Errorf("handler panic on topic %q: %v", m.Topic, r)
						}
					}()
					handlerErr = handler(tCtx, m, memberPool)
				}()

				// Join this member's fan-out before settling it, mirroring the
				// independent-delivery path: a handler that spawns sub-workers
				// and forgets pool.Wait() must not have its delivery Acked (or
				// retried) while the sub-workers are still running.
				if waitErr := memberPool.Wait(); waitErr != nil && handlerErr == nil {
					handlerErr = waitErr
				}

				if handlerErr != nil {
					if err := c.handleFailure(tCtx, d, handlerErr); err != nil {
						c.logger.Error("failed to settle message after handler failure",
							"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID, "error", err,
						)
					}
					continue
				}
				if err := c.driver.Ack(tCtx, d); err != nil {
					c.logger.Error("failed to ack message",
						"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID, "error", err,
					)
				}
			}
			return nil
		})

		err := c.workerDispatcher.Dispatch(ctx, groupTask)
		if err != nil {
			c.inFlight.Done()
			c.groupMu.Lock()
			entry.refs--
			if entry.refs <= 0 {
				delete(c.groupLocks, gID)
			}
			c.groupMu.Unlock()

			for _, item := range groupDeliveries {
				_ = c.driver.Nack(ctx, item, err)
			}
			continue
		}
		dispatched += len(groupDeliveries)
	}

	return dispatched
}

// acquireGroupLock fetches or allocates a group lock entry and increments its reference count.
func (c *Consumer) acquireGroupLock(gid string) *groupLockEntry {
	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	if c.groupLocks == nil {
		c.groupLocks = make(map[string]*groupLockEntry)
	}
	l, ok := c.groupLocks[gid]
	if !ok {
		l = &groupLockEntry{}
		c.groupLocks[gid] = l
	}
	l.refs++
	return l
}

// releaseGroupLock unlocks the group mutex and cleans up the map entry when all referencing tasks finish.
func (c *Consumer) releaseGroupLock(gid string, entry *groupLockEntry) {
	entry.mu.Unlock()

	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	entry.refs--
	if entry.refs <= 0 {
		delete(c.groupLocks, gid)
	}
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

	// Reschedule the delivery. Adapters implementing Reenqueuer fold the
	// reschedule into the current delivery itself, eliminating the crash window
	// between Acking the delivery and enqueuing the retry copy.
	if reenqueuer, ok := c.driver.(Reenqueuer); ok {
		return reenqueuer.Reenqueue(ctx, d, m, time.Now().Add(backoff))
	}

	// Re-enqueue before Acking: the retry copy is scheduled first, so a failure
	// or crash after this point at worst redelivers the original too
	// (at-least-once) rather than losing it (at-most-once).
	if delayedAdapter, ok := c.driver.(DelayedEnqueuer); ok {
		if err := delayedAdapter.EnqueueAt(ctx, m, time.Now().Add(backoff)); err != nil {
			return err
		}
		if err := c.driver.Ack(ctx, d); err != nil {
			c.logger.Error("failed to ack delivery after retry enqueue (retry copy already scheduled)",
				"topic", m.Topic, "msg_id", m.ID, "error", err,
			)
			return err
		}
		return nil
	}

	// Core delayed dispatch fallback: the adapter supports no delayed enqueue, so
	// schedule an immediate in-process re-enqueue and then Ack. This mirrors the
	// old behaviour but enqueues before dropping the original.
	var timer *time.Timer
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return c.driver.Nack(ctx, d, reason)
	}
	timer = time.AfterFunc(backoff, func() {
		c.mu.Lock()
		delete(c.retryTimers, timer)
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		_ = c.driver.Enqueue(context.Background(), m)
	})
	c.retryTimers[timer] = m
	c.mu.Unlock()

	if err := c.driver.Ack(ctx, d); err != nil {
		c.logger.Error("failed to ack delivery after retry scheduled",
			"topic", m.Topic, "msg_id", m.ID, "error", err,
		)
		return err
	}
	return nil
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
	var pending []*Message
	for t, m := range c.retryTimers {
		t.Stop()
		if m != nil {
			pending = append(pending, m)
		}
	}
	c.retryTimers = nil
	c.mu.Unlock()

	// Flush any pending retry messages that were already Acked from the broker
	// so they are not permanently lost on consumer shutdown.
	for _, m := range pending {
		_ = c.driver.Enqueue(context.Background(), m)
	}

	if c.cancel != nil {
		c.cancel()
	}
	close(c.quit)
	c.wg.Wait()
	c.inFlight.Wait()
	c.closeOnce.Do(func() {
		_ = c.driver.Close()
	})
	c.logger.Info("queue consumer stopped gracefully")
}
