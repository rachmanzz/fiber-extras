package queuerabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rachmanzz/fiber-extras/v3/queue"
)

// RabbitMQAdapter implements queue.Adapter for RabbitMQ using AMQP 0.9.1.
// Supports priority queues (x-max-priority = 10) and manual message acknowledgments.
type RabbitMQAdapter struct {
	mu     sync.RWMutex
	conn   *amqp.Connection
	ch     *amqp.Channel
	queue  string
	closed bool
}

// Config holds configuration parameters for the RabbitMQ adapter.
type Config struct {
	URL         string
	QueueName   string
	MaxPriority uint8
	Prefetch    int
}

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		URL:         "amqp://guest:guest@localhost:5672/",
		QueueName:   "fiber_priority_queue",
		MaxPriority: 10,
		Prefetch:    100,
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

// New establishes connection, declares topology, and returns an initialized RabbitMQAdapter.
func New(url string, opts ...Option) (*RabbitMQAdapter, error) {
	cfg := DefaultConfig()
	if url != "" {
		cfg.URL = url
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("queue-rabbitmq: failed to dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("queue-rabbitmq: failed to open channel: %w", err)
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
		return nil, fmt.Errorf("queue-rabbitmq: failed to declare priority queue: %w", err)
	}

	_ = ch.Qos(cfg.Prefetch, 0, false)

	return &RabbitMQAdapter{
		conn:  conn,
		ch:    ch,
		queue: cfg.QueueName,
	}, nil
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
		Dedup:             true,
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

// Enqueue publishes a message into RabbitMQ with persistent delivery mode and priority.
func (r *RabbitMQAdapter) Enqueue(ctx context.Context, msg *queue.Message) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return fmt.Errorf("queue-rabbitmq: adapter is closed")
	}
	ch := r.ch
	r.mu.RUnlock()

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

	return ch.PublishWithContext(ctx,
		"",      // exchange
		r.queue, // routing key
		false,   // mandatory
		false,   // immediate
		pub,
	)
}

// Dequeue polls messages from RabbitMQ using basic get without auto-ack.
func (r *RabbitMQAdapter) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, fmt.Errorf("queue-rabbitmq: adapter is closed")
	}
	ch := r.ch
	r.mu.RUnlock()

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	deliveries := make([]*queue.Delivery, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		msg, ok, err := ch.Get(r.queue, false)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}

		var m queue.Message
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			_ = msg.Reject(false)
			continue
		}

		deliveries = append(deliveries, &queue.Delivery{
			Message: &m,
			Token:   msg.DeliveryTag,
		})
	}

	return deliveries, nil
}

// Ack confirms successful message processing.
func (r *RabbitMQAdapter) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	tag, ok := d.Token.(uint64)
	if !ok {
		return fmt.Errorf("queue-rabbitmq: invalid delivery token: %T", d.Token)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}
	return r.ch.Ack(tag, false)
}

// Nack rejects message and requeues it for redelivery.
func (r *RabbitMQAdapter) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	tag, ok := d.Token.(uint64)
	if !ok {
		return fmt.Errorf("queue-rabbitmq: invalid delivery token: %T", d.Token)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}
	return r.ch.Nack(tag, false, true)
}

// Close closes channel and connection.
func (r *RabbitMQAdapter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var err error
	if r.ch != nil {
		err = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	return err
}
