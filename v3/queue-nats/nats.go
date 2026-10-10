package queuenats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rachmanzz/fiber-extras/v3/queue"
)

// NATSAdapter implements queue.Adapter for NATS JetStream.
type NATSAdapter struct {
	mu         sync.RWMutex
	nc         *nats.Conn
	js         nats.JetStreamContext
	stream     string
	critSub    *nats.Subscription
	highSub    *nats.Subscription
	defaultSub *nats.Subscription
	lowSub     *nats.Subscription
	closed     bool
}

// Config holds parameters for JetStream queue adapter.
type Config struct {
	Stream        string
	Storage       string // "file" or "memory"
	AckWait       time.Duration
	MaxAckPending int
}

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		Stream:        "FIBER_QUEUE",
		Storage:       "file",
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
	}
}

// Option configures NATSAdapter.
type Option func(*Config)

// WithStream sets the stream name.
func WithStream(stream string) Option {
	return func(c *Config) {
		if stream != "" {
			c.Stream = stream
		}
	}
}

// WithStorage sets the storage type ("file" or "memory").
func WithStorage(storage string) Option {
	return func(c *Config) {
		if storage != "" {
			c.Storage = storage
		}
	}
}

// WithAckWait sets the ack wait timeout.
func WithAckWait(wait time.Duration) Option {
	return func(c *Config) {
		if wait > 0 {
			c.AckWait = wait
		}
	}
}

// New constructs a NATSAdapter from a connection and options.
func New(nc *nats.Conn, opts ...Option) (*NATSAdapter, error) {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewWithConfig(nc, cfg)
}

// NewWithConfig instantiates a NATS JetStream queue adapter with explicit config.
func NewWithConfig(nc *nats.Conn, cfg Config) (*NATSAdapter, error) {
	if cfg.Stream == "" {
		cfg.Stream = "FIBER_QUEUE"
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = 1000
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("queue-nats: failed to obtain jetstream context: %w", err)
	}

	storageType := nats.FileStorage
	if strings.ToLower(cfg.Storage) == "memory" {
		storageType = nats.MemoryStorage
	}

	// Ensure stream exists with WorkQueue retention
	streamSubject := fmt.Sprintf("%s.>", cfg.Stream)
	streamCfg := &nats.StreamConfig{
		Name:       cfg.Stream,
		Subjects:   []string{streamSubject},
		Storage:    storageType,
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardOld,
		Duplicates: 2 * time.Minute,
	}

	_, err = js.AddStream(streamCfg)
	if err != nil && !strings.Contains(err.Error(), "stream name already in use") {
		return nil, fmt.Errorf("queue-nats: failed to create or verify jetstream stream: %w", err)
	}

	// Create prioritized pull consumers
	setupConsumer := func(name, filter string) (*nats.Subscription, error) {
		cCfg := &nats.ConsumerConfig{
			Durable:       name,
			FilterSubject: filter,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       cfg.AckWait,
			MaxAckPending: cfg.MaxAckPending,
		}
		_, cErr := js.AddConsumer(cfg.Stream, cCfg)
		if cErr != nil && !strings.Contains(cErr.Error(), "consumer already exists") {
			return nil, cErr
		}
		return js.PullSubscribe(filter, name, nats.Bind(cfg.Stream, name))
	}

	cPrefix := cfg.Stream + "_"
	critSub, err := setupConsumer(cPrefix+"CRIT", fmt.Sprintf("%s.critical.>", cfg.Stream))
	if err != nil {
		return nil, err
	}
	highSub, err := setupConsumer(cPrefix+"HIGH", fmt.Sprintf("%s.high.>", cfg.Stream))
	if err != nil {
		return nil, err
	}
	defSub, err := setupConsumer(cPrefix+"DEF", fmt.Sprintf("%s.default.>", cfg.Stream))
	if err != nil {
		return nil, err
	}
	lowSub, err := setupConsumer(cPrefix+"LOW", fmt.Sprintf("%s.low.>", cfg.Stream))
	if err != nil {
		return nil, err
	}

	return &NATSAdapter{
		nc:         nc,
		js:         js,
		stream:     cfg.Stream,
		critSub:    critSub,
		highSub:    highSub,
		defaultSub: defSub,
		lowSub:     lowSub,
	}, nil
}

// Name returns the adapter identifier.
func (n *NATSAdapter) Name() string {
	return "nats"
}

// Capabilities declares the capabilities of NATS JetStream.
func (n *NATSAdapter) Capabilities() queue.Capabilities {
	return queue.Capabilities{
		Durable:           true,
		Persistent:        true,
		NativeRedelivery:  true,
		NativePriority:    queue.PrioritySoft,
		DelayedEnqueue:    false,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             true,
		MaxBatchSize:      1000,
		Guarantee:         queue.EffectivelyOnce,
	}
}

func (n *NATSAdapter) subjectFor(msg *queue.Message) string {
	prioStr := "default"
	switch {
	case msg.Priority >= queue.PriorityCritical:
		prioStr = "critical"
	case msg.Priority >= queue.PriorityHigh:
		prioStr = "high"
	case msg.Priority <= queue.PriorityLow:
		prioStr = "low"
	}

	groupStr := "general"
	if msg.GroupID != "" {
		groupStr = msg.GroupID
	}

	safeTopic := strings.ReplaceAll(msg.Topic, ":", ".")
	return fmt.Sprintf("%s.%s.%s.%s", n.stream, prioStr, groupStr, safeTopic)
}

// Enqueue publishes a message to NATS JetStream.
func (n *NATSAdapter) Enqueue(ctx context.Context, msg *queue.Message) error {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return fmt.Errorf("queue-nats: adapter is closed")
	}
	n.mu.RUnlock()

	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue-nats: failed to marshal message: %w", err)
	}

	natsMsg := &nats.Msg{
		Subject: n.subjectFor(msg),
		Data:    raw,
		Header:  nats.Header{},
	}
	natsMsg.Header.Set("Nats-Msg-Id", msg.ID)

	for k, v := range msg.Headers {
		natsMsg.Header.Set(k, v)
	}

	_, err = n.js.PublishMsg(natsMsg)
	return err
}

// Dequeue drains messages from prioritized pull consumers.
func (n *NATSAdapter) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return nil, fmt.Errorf("queue-nats: adapter is closed")
	}
	n.mu.RUnlock()

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	subs := []*nats.Subscription{n.critSub, n.highSub, n.defaultSub, n.lowSub}
	deliveries := make([]*queue.Delivery, 0, batchSize)

	for _, sub := range subs {
		if sub == nil || len(deliveries) >= batchSize {
			break
		}

		remaining := batchSize - len(deliveries)
		msgs, err := sub.Fetch(remaining, nats.MaxWait(20*time.Millisecond))
		if err != nil {
			if errorsIsTimeout(err) {
				continue
			}
			return nil, err
		}

		for _, rawMsg := range msgs {
			var m queue.Message
			if err := json.Unmarshal(rawMsg.Data, &m); err != nil {
				_ = rawMsg.Term()
				continue
			}

			deliveries = append(deliveries, &queue.Delivery{
				Message: &m,
				Token:   rawMsg,
			})
		}
	}

	return deliveries, nil
}

// Ack confirms successful processing of a delivery.
func (n *NATSAdapter) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	natsMsg, ok := d.Token.(*nats.Msg)
	if !ok {
		return fmt.Errorf("queue-nats: invalid delivery token: %T", d.Token)
	}
	return natsMsg.Ack()
}

// Nack informs the broker that message processing failed.
func (n *NATSAdapter) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	natsMsg, ok := d.Token.(*nats.Msg)
	if !ok {
		return fmt.Errorf("queue-nats: invalid delivery token: %T", d.Token)
	}
	return natsMsg.Nak()
}

// Close unsubscribes and cleans up resources.
func (n *NATSAdapter) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	subs := []*nats.Subscription{n.critSub, n.highSub, n.defaultSub, n.lowSub}
	for _, s := range subs {
		if s != nil {
			_ = s.Unsubscribe()
		}
	}
	return nil
}

func errorsIsTimeout(err error) bool {
	return err == nats.ErrTimeout || strings.Contains(err.Error(), "timeout")
}
