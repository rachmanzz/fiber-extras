package queuerabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rachmanzz/fiber-extras/v3/queue"
)

var (
	// errClosed is returned by channel operations once the adapter is closed.
	errClosed = errors.New("queue-rabbitmq: adapter is closed")

	// ErrReconnecting is returned by operations while the supervisor is
	// restoring a lost AMQP connection or channel. It is temporary: retrying
	// later succeeds once the connection is re-established.
	ErrReconnecting = errors.New("queue-rabbitmq: connection lost, reconnecting")

	// errStaleDelivery reports that a delivery token belongs to a previous
	// connection. The broker requeued every unacked delivery when the
	// connection dropped, so the message will be redelivered; acknowledging a
	// stale tag on the fresh channel would kill it with an
	// "unknown delivery tag" channel exception.
	errStaleDelivery = errors.New("queue-rabbitmq: delivery belongs to a previous connection")
)

// deliveryToken pins a delivery tag to the channel generation that produced
// it, so Ack/Nack after a reconnect can be recognised as stale instead of
// corrupting the new channel.
type deliveryToken struct {
	gen uint64
	tag uint64
}

// RabbitMQAdapter implements queue.Adapter for RabbitMQ using AMQP 0.9.1.
// Supports priority queues (x-max-priority = 10), manual message
// acknowledgments, and automatic reconnection: a dedicated supervisor watches
// the connection and channel and re-dials with exponential backoff. Deliveries
// that were unacked when the connection dropped are requeued by the broker and
// redelivered (at-least-once).
type RabbitMQAdapter struct {
	mu          sync.RWMutex
	opMu        sync.Mutex
	cfg         Config
	conn        *amqp.Connection
	ch          *amqp.Channel
	gen         uint64 // channel generation; bumped on every (re)connect
	queue       string
	closed      bool
	retryTimers map[*time.Timer]struct{}
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// Config holds configuration parameters for the RabbitMQ adapter.
type Config struct {
	URL              string
	QueueName        string
	MaxPriority      uint8
	Prefetch         int
	ReconnectInitial time.Duration // first reconnect delay (default 100ms)
	ReconnectMax     time.Duration // reconnect backoff cap (default 10s)
}

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		URL:              "amqp://guest:guest@localhost:5672/",
		QueueName:        "fiber_priority_queue",
		MaxPriority:      10,
		Prefetch:         100,
		ReconnectInitial: 100 * time.Millisecond,
		ReconnectMax:     10 * time.Second,
	}
}

// Option configures RabbitMQAdapter.
type Option func(*Config)

// WithQueueName sets the RabbitMQ queue name.
func WithQueueName(name string) Option {
	return func(c *Config) {
		if name != "" {
			c.QueueName = name
		}
	}
}

// WithMaxPriority sets the maximum priority level declared on the queue.
func WithMaxPriority(max uint8) Option {
	return func(c *Config) {
		if max > 0 {
			c.MaxPriority = max
		}
	}
}

// WithPrefetch sets QoS prefetch count.
func WithPrefetch(prefetch int) Option {
	return func(c *Config) {
		if prefetch > 0 {
			c.Prefetch = prefetch
		}
	}
}

// WithReconnectBackoff sets the initial delay and the cap of the exponential
// backoff used by the reconnect supervisor.
func WithReconnectBackoff(initial, max time.Duration) Option {
	return func(c *Config) {
		if initial > 0 {
			c.ReconnectInitial = initial
		}
		if max > 0 {
			c.ReconnectMax = max
		}
	}
}

// New establishes connection, declares topology, and returns an initialized
// RabbitMQAdapter. A supervisor goroutine keeps the connection alive for the
// adapter's lifetime; stop it with Close.
func New(url string, opts ...Option) (*RabbitMQAdapter, error) {
	cfg := DefaultConfig()
	if url != "" {
		cfg.URL = url
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	conn, ch, err := dial(cfg)
	if err != nil {
		return nil, err
	}

	adapter := &RabbitMQAdapter{
		cfg:    cfg,
		conn:   conn,
		ch:     ch,
		gen:    1,
		queue:  cfg.QueueName,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go adapter.supervise()
	return adapter, nil
}

// dial establishes the AMQP connection, opens the channel, re-declares the
// durable priority queue and applies QoS.
func dial(cfg Config) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("queue-rabbitmq: failed to dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("queue-rabbitmq: failed to open channel: %w", err)
	}

	// Declare durable priority queue with x-max-priority
	args := amqp.Table{
		"x-max-priority": int32(cfg.MaxPriority),
	}
	_, err = ch.QueueDeclare(
		cfg.QueueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		args,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("queue-rabbitmq: failed to declare priority queue: %w", err)
	}

	_ = ch.Qos(cfg.Prefetch, 0, false)

	return conn, ch, nil
}

// Name returns the driver identifier.
func (r *RabbitMQAdapter) Name() string {
	return "rabbitmq"
}

// Capabilities declares capabilities of the RabbitMQ adapter.
func (r *RabbitMQAdapter) Capabilities() queue.Capabilities {
	return queue.Capabilities{
		Durable:           true,
		Persistent:        true,
		NativeRedelivery:  true,
		NativePriority:    queue.PriorityHard,
		DelayedEnqueue:    false,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             false, // No deduplication mechanism; duplicate IDs are delivered separately.
		MaxBatchSize:      500,
		Guarantee:         queue.AtLeastOnce,
	}
}

// mapPriority converts queue.Priority (0-100) to RabbitMQ priority (0-10).
func mapPriority(p queue.Priority) uint8 {
	val := int(p) / 10
	if val > 10 {
		return 10
	}
	if val < 0 {
		return 0
	}
	return uint8(val)
}

// channelState returns the live channel and its generation, or a terminal
// (errClosed) or temporary (ErrReconnecting) failure. Callers must hold opMu.
func (r *RabbitMQAdapter) channelState() (*amqp.Channel, uint64, error) {
	r.mu.RLock()
	closed, ch, gen := r.closed, r.ch, r.gen
	r.mu.RUnlock()
	if closed || ch == nil {
		if closed {
			return nil, 0, errClosed
		}
		return nil, 0, ErrReconnecting
	}
	return ch, gen, nil
}

// withChannel serializes access to the shared AMQP channel. amqp091-go
// channels are explicitly documented as not safe for concurrent use, yet the
// queue consumer Acks from worker goroutines while the poll loop Dequeues, and
// producers may Enqueue concurrently. All channel I/O must funnel through here
// or the wire protocol desynchronizes (403/504/505 errors and lost messages).
func (r *RabbitMQAdapter) withChannel(fn func(ch *amqp.Channel) error) error {
	return r.withChannelGen(func(ch *amqp.Channel, _ uint64) error { return fn(ch) })
}

// withChannelGen is withChannel plus the current channel generation. A channel
// that the broker has already closed is reported as ErrReconnecting: the
// supervisor restores it momentarily, so callers can simply retry.
func (r *RabbitMQAdapter) withChannelGen(fn func(ch *amqp.Channel, gen uint64) error) error {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	ch, gen, err := r.channelState()
	if err != nil {
		return err
	}
	if ch.IsClosed() {
		return ErrReconnecting
	}
	if err := fn(ch, gen); err != nil {
		if ch.IsClosed() {
			return ErrReconnecting
		}
		return err
	}
	return nil
}

// withDeliveryChannel runs fn only when the delivery still belongs to the
// live channel generation. Stale deliveries are dropped with
// errStaleDelivery: the broker already requeued them during the disconnect,
// so they are redelivered rather than lost.
func (r *RabbitMQAdapter) withDeliveryChannel(tok deliveryToken, fn func(ch *amqp.Channel) error) error {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	ch, gen, err := r.channelState()
	if err != nil {
		return err
	}
	if tok.gen != gen {
		return errStaleDelivery
	}
	if ch.IsClosed() {
		return ErrReconnecting
	}
	if err := fn(ch); err != nil {
		if ch.IsClosed() {
			return ErrReconnecting
		}
		return err
	}
	return nil
}

// settleError maps the terminal/temporary/stale cases of Ack and Nack to nil.
// In every one of them the broker has requeued (or will redeliver) the
// message, so returning an error would only make callers duplicate work.
func settleError(err error) error {
	if errors.Is(err, errClosed) || errors.Is(err, ErrReconnecting) || errors.Is(err, errStaleDelivery) {
		return nil
	}
	return err
}

// Enqueue publishes a message into RabbitMQ with persistent delivery mode and priority.
func (r *RabbitMQAdapter) Enqueue(ctx context.Context, msg *queue.Message) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue-rabbitmq: failed to encode message: %w", err)
	}

	headers := amqp.Table{
		"x-msg-id": msg.ID,
		"x-topic":  msg.Topic,
	}
	for k, v := range msg.Headers {
		headers[k] = v
	}

	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Priority:     mapPriority(msg.Priority),
		Timestamp:    msg.CreatedAt,
		MessageId:    msg.ID,
		Type:         msg.Topic,
		Headers:      headers,
		Body:         raw,
	}

	return r.withChannel(func(ch *amqp.Channel) error {
		return ch.PublishWithContext(ctx,
			"",      // exchange
			r.queue, // routing key
			false,   // mandatory
			false,   // immediate
			pub,
		)
	})
}

// Dequeue polls messages from RabbitMQ using basic get without auto-ack.
func (r *RabbitMQAdapter) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	deliveries := make([]*queue.Delivery, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		var (
			msg amqp.Delivery
			ok  bool
			gen uint64
		)
		err := r.withChannelGen(func(ch *amqp.Channel, g uint64) error {
			m, o, e := ch.Get(r.queue, false)
			msg, ok, gen = m, o, g
			return e
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		var m queue.Message
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			// Publish a poison copy before acknowledging the undecodable
			// original: if this fails the original stays unacked and is
			// requeued by RabbitMQ instead of being silently dropped.
			poisonID := msg.MessageId
			if poisonID == "" {
				poisonID = queue.NewUUID()
			}
			poison := queue.NewPoisonMessage(poisonID, msg.Body, err, r.Name())
			if pErr := r.Enqueue(ctx, poison); pErr != nil {
				return nil, fmt.Errorf("queue-rabbitmq: failed to surface undecodable message to DLQ: %w", pErr)
			}
			if aErr := r.withDeliveryChannel(deliveryToken{gen: gen, tag: msg.DeliveryTag}, func(*amqp.Channel) error { return msg.Ack(false) }); aErr != nil {
				return nil, aErr
			}
			continue
		}

		deliveries = append(deliveries, &queue.Delivery{
			Message: &m,
			Token:   deliveryToken{gen: gen, tag: msg.DeliveryTag},
		})
	}

	return deliveries, nil
}

// Reenqueue republishes the retry copy at the target time and only then
// acknowledges the current delivery. Publishing before the Ack removes the
// ack-then-enqueue crash window: if the process dies in between, the original
// delivery remains unacked and is requeued by RabbitMQ instead of being lost.
func (r *RabbitMQAdapter) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	if d == nil || d.Token == nil {
		return nil
	}

	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("queue-rabbitmq: adapter is closed")
	}
	r.mu.RUnlock()

	delay := time.Until(at)
	if delay > 0 {
		var timer *time.Timer
		timer = time.AfterFunc(delay, func() {
			r.mu.Lock()
			delete(r.retryTimers, timer)
			closed := r.closed
			r.mu.Unlock()
			if closed {
				return
			}
			// Detach from the original request context's cancellation/deadline
			// but keep its values, so a cancelled caller does not abort the
			// deferred re-enqueue and ack.
			cbCtx := context.WithoutCancel(ctx)
			_ = r.Enqueue(cbCtx, msg)
			_ = r.Ack(cbCtx, d)
		})

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			timer.Stop()
			return fmt.Errorf("queue-rabbitmq: adapter is closed")
		}
		if r.retryTimers == nil {
			r.retryTimers = make(map[*time.Timer]struct{})
		}
		r.retryTimers[timer] = struct{}{}
		r.mu.Unlock()
		return nil
	}
	if err := r.Enqueue(ctx, msg); err != nil {
		return err
	}
	return r.Ack(ctx, d)
}

// Ack confirms successful message processing. Deliveries whose connection was
// lost are treated as settled: the broker requeued them and will redeliver.
func (r *RabbitMQAdapter) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	tok, ok := d.Token.(deliveryToken)
	if !ok {
		return fmt.Errorf("queue-rabbitmq: invalid delivery token: %T", d.Token)
	}

	return settleError(r.withDeliveryChannel(tok, func(ch *amqp.Channel) error { return ch.Ack(tok.tag, false) }))
}

// Nack rejects message and requeues it for redelivery.
func (r *RabbitMQAdapter) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	tok, ok := d.Token.(deliveryToken)
	if !ok {
		return fmt.Errorf("queue-rabbitmq: invalid delivery token: %T", d.Token)
	}

	return settleError(r.withDeliveryChannel(tok, func(ch *amqp.Channel) error { return ch.Nack(tok.tag, false, true) }))
}

// supervise restores the connection or channel whenever the broker (or a
// network failure) closes either, re-dialing with exponential backoff until it
// succeeds or the adapter is closed.
func (r *RabbitMQAdapter) supervise() {
	defer close(r.doneCh)
	for {
		r.mu.RLock()
		conn, ch, closed := r.conn, r.ch, r.closed
		r.mu.RUnlock()
		if closed || conn == nil {
			return
		}
		if _, _, err := r.channelState(); err != nil && !errors.Is(err, errClosed) {
			// Channel already gone (e.g. swapped mid-iteration); reconnect now.
			if !r.reconnect() {
				return
			}
			continue
		}

		connClosed := make(chan *amqp.Error, 1)
		chClosed := make(chan *amqp.Error, 1)
		conn.NotifyClose(connClosed)
		if ch != nil {
			ch.NotifyClose(chClosed)
		}

		select {
		case <-r.stopCh:
			return
		case <-connClosed:
		case <-chClosed:
		}

		if !r.reconnect() {
			return
		}
	}
}

// reconnect re-establishes the AMQP connection and channel, reusing the
// current connection when only the channel died. It blocks with exponential
// backoff until the broker is reachable again or the adapter is closed.
func (r *RabbitMQAdapter) reconnect() bool {
	backoff := r.cfg.ReconnectInitial
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}
	maxBackoff := r.cfg.ReconnectMax
	if maxBackoff < backoff {
		maxBackoff = 10 * time.Second
	}

	for {
		r.mu.RLock()
		closed, conn := r.closed, r.conn
		r.mu.RUnlock()
		if closed {
			return false
		}

		// Channel-only failure: the connection may still be usable.
		if conn != nil {
			if ch, err := conn.Channel(); err == nil {
				if ok := r.swapChannel(conn, ch); ok {
					return true
				}
				_ = ch.Close()
				return false
			}
		}

		// Full redial.
		newConn, newCh, err := dial(r.cfg)
		if err == nil {
			if ok := r.swapChannel(newConn, newCh); ok {
				return true
			}
			_ = newCh.Close()
			_ = newConn.Close()
			return false
		}

		select {
		case <-r.stopCh:
			return false
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// swapChannel installs the new channel (and connection) under opMu, bumping
// the generation so stale delivery tags can be rejected. It reports whether
// the swap happened; if the adapter was closed meanwhile the caller must tear
// the fresh connection down itself.
func (r *RabbitMQAdapter) swapChannel(conn *amqp.Connection, ch *amqp.Channel) bool {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	oldCh := r.ch
	r.conn = conn
	r.ch = ch
	r.gen++
	if oldCh != nil && oldCh != ch {
		_ = oldCh.Close()
	}
	return true
}

// Close closes channel and connection. It drains in-flight channel operations
// before tearing the channel down so it never closes mid-request, and it
// stops the reconnect supervisor first so a channel is never re-opened after
// Close.
func (r *RabbitMQAdapter) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for t := range r.retryTimers {
		t.Stop()
	}
	r.retryTimers = nil
	ch, conn := r.ch, r.conn
	if r.stopCh != nil {
		close(r.stopCh)
	}
	r.mu.Unlock()

	// Let the supervisor exit (and abort any in-flight redial) before
	// closing the live channel.
	if r.doneCh != nil {
		select {
		case <-r.doneCh:
		case <-time.After(5 * time.Second):
		}
	}

	r.opMu.Lock()
	defer r.opMu.Unlock()
	var err error
	if ch != nil {
		err = ch.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	return err
}
