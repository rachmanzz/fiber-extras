package queuenats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rachmanzz/gosuite/v3/queue"
)

// NATSAdapter implements queue.Adapter for NATS JetStream.
type NATSAdapter struct {
	mu          sync.RWMutex
	nc          *nats.Conn
	js          nats.JetStreamContext
	stream      string
	critSub     *nats.Subscription
	highSub     *nats.Subscription
	defaultSub  *nats.Subscription
	lowSub      *nats.Subscription
	closed      bool
	retryTimers map[*time.Timer]struct{}
}

// Config holds parameters for JetStream queue adapter.
type Config struct {
	Stream        string
	Storage       string // "file" or "memory"
	AckWait       time.Duration
	MaxAckPending int
	// DedupWindow is how long JetStream retains message IDs for duplicate
	// suppression (the stream's Duplicates window). Defaults to 2 minutes.
	DedupWindow time.Duration
}

// ErrNilMessage is returned when attempting to enqueue a nil message.
var ErrNilMessage = errors.New("queue-nats: cannot enqueue nil message")

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		Stream:        "GOSUITE_QUEUE",
		Storage:       "file",
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
		DedupWindow:   2 * time.Minute,
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

// WithDedupWindow sets how long JetStream suppresses duplicate message IDs.
func WithDedupWindow(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.DedupWindow = d
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
		cfg.Stream = "GOSUITE_QUEUE"
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 30 * time.Second
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = 1000
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 2 * time.Minute
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
		Duplicates: cfg.DedupWindow,
	}

	_, err = js.AddStream(streamCfg)
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
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
		if cErr != nil && !errors.Is(cErr, nats.ErrConsumerNameAlreadyInUse) {
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

// Wire format v2 (split transport): the JetStream message body carries the
// user payload as raw bytes and all adapter metadata travels in NATS headers
// under reserved "fiber-*" keys (user headers get the "fiber-h-" prefix so
// they can never collide with reserved metadata). The payload never passes
// through a text encoding, so the base64 tax of the v1 JSON envelope is gone.
// Format v1 (a single JSON envelope in the body) is still decoded for
// messages written by older producers (rolling deploys).
const (
	hdrEnvelope   = "fiber-envelope"
	hdrID         = "fiber-id"
	hdrTopic      = "fiber-topic"
	hdrPriority   = "fiber-priority"
	hdrCreatedAt  = "fiber-created-at"
	hdrAttempt    = "fiber-attempt"
	hdrMaxAttempt = "fiber-max-attempts"
	hdrGroupID    = "fiber-group-id"
	hdrGroupOrder = "fiber-group-order"
	hdrDriver     = "fiber-driver"
	hdrUserPrefix = "fiber-h-"
)

// wireVersion marks messages that use the v2 split transport.
const wireVersion = "2"

// buildNATSMsg renders a queue.Message as a v2 split-transport nats.Msg:
// raw payload in Data, metadata in reserved headers. The payload is passed
// through zero-copy.
func buildNATSMsg(subject string, msg *queue.Message) *nats.Msg {
	m := &nats.Msg{
		Subject: subject,
		Data:    msg.Payload,
		Header:  nats.Header{},
	}
	m.Header.Set("Nats-Msg-Id", msg.ID)
	m.Header.Set(hdrEnvelope, wireVersion)
	m.Header.Set(hdrID, msg.ID)
	m.Header.Set(hdrTopic, msg.Topic)
	m.Header.Set(hdrPriority, strconv.Itoa(int(msg.Priority)))
	m.Header.Set(hdrCreatedAt, msg.CreatedAt.UTC().Format(time.RFC3339Nano))
	m.Header.Set(hdrAttempt, strconv.Itoa(msg.Attempt))
	m.Header.Set(hdrMaxAttempt, strconv.Itoa(msg.MaxAttempts))
	if msg.GroupID != "" {
		m.Header.Set(hdrGroupID, msg.GroupID)
	}
	if msg.GroupOrder != 0 {
		m.Header.Set(hdrGroupOrder, strconv.Itoa(msg.GroupOrder))
	}
	if msg.Driver != "" {
		m.Header.Set(hdrDriver, msg.Driver)
	}
	for k, v := range msg.Headers {
		m.Header.Set(hdrUserPrefix+k, v)
	}
	return m
}

// decodeNATSMsg rebuilds a queue.Message from a v2 split-transport nats.Msg.
// Corrupt or missing reserved metadata yields an error (surfaced as a poison
// delivery by the caller).
func decodeNATSMsg(rawMsg *nats.Msg) (*queue.Message, error) {
	if rawMsg.Header.Get(hdrEnvelope) != wireVersion {
		return nil, fmt.Errorf("missing v2 envelope marker")
	}
	id := rawMsg.Header.Get(hdrID)
	if id == "" {
		return nil, fmt.Errorf("missing %s header", hdrID)
	}
	topic := rawMsg.Header.Get(hdrTopic)
	if topic == "" {
		return nil, fmt.Errorf("missing %s header", hdrTopic)
	}
	priority, err := strconv.Atoi(rawMsg.Header.Get(hdrPriority))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", hdrPriority, err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, rawMsg.Header.Get(hdrCreatedAt))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", hdrCreatedAt, err)
	}
	attempt, err := strconv.Atoi(rawMsg.Header.Get(hdrAttempt))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", hdrAttempt, err)
	}
	if meta, mErr := rawMsg.Metadata(); mErr == nil && meta.NumDelivered > 1 {
		// When JetStream redelivers via Nak/NakWithDelay, the header reflects the
		// initial attempt (0). Derive current attempt from JetStream delivery metadata.
		attempt = int(meta.NumDelivered) - 1
	}
	maxAttempts, err := strconv.Atoi(rawMsg.Header.Get(hdrMaxAttempt))
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", hdrMaxAttempt, err)
	}
	groupOrder := 0
	if rawMsg.Header.Get(hdrGroupOrder) != "" {
		if groupOrder, err = strconv.Atoi(rawMsg.Header.Get(hdrGroupOrder)); err != nil {
			return nil, fmt.Errorf("invalid %s: %w", hdrGroupOrder, err)
		}
	}

	headers := make(map[string]string)
	for k, vs := range rawMsg.Header {
		if len(k) <= len(hdrUserPrefix) || !strings.HasPrefix(k, hdrUserPrefix) || len(vs) == 0 {
			continue
		}
		headers[k[len(hdrUserPrefix):]] = vs[0]
	}

	return &queue.Message{
		ID:          id,
		Topic:       topic,
		Payload:     rawMsg.Data,
		Priority:    queue.Priority(priority),
		GroupID:     rawMsg.Header.Get(hdrGroupID),
		GroupOrder:  groupOrder,
		Headers:     headers,
		CreatedAt:   createdAt,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		Driver:      rawMsg.Header.Get(hdrDriver),
	}, nil
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
	if msg == nil {
		return ErrNilMessage
	}
	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return fmt.Errorf("queue-nats: adapter is closed")
	}
	if n.js == nil {
		n.mu.RUnlock()
		return fmt.Errorf("queue-nats: jetstream context is not initialized")
	}
	n.mu.RUnlock()

	_, err := n.js.PublishMsg(buildNATSMsg(n.subjectFor(msg), msg))
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
			m, err := decodeNATSMsg(rawMsg)
			if err != nil {
				// Undecodable or non-split-transport record: publish a poison
				// copy before terminating the original -- if this fails the
				// original stays unacked and is redelivered by JetStream
				// instead of being silently lost.
				poisonID := rawMsg.Header.Get("Nats-Msg-Id")
				if poisonID == "" {
					poisonID = queue.NewUUID()
				}
				poison := queue.NewPoisonMessage(poisonID, rawMsg.Data, err, n.Name())
				if pErr := n.Enqueue(ctx, poison); pErr != nil {
					return nil, fmt.Errorf("queue-nats: failed to surface undecodable message to DLQ: %w", pErr)
				}
				_ = rawMsg.Term()
				continue
			}

			deliveries = append(deliveries, &queue.Delivery{
				Message: m,
				Token:   rawMsg,
			})
		}
	}

	return deliveries, nil
}

// Reenqueue republishes the retry copy at the target time and only then
// acknowledges the current delivery. Publishing before the Ack removes the
// ack-then-enqueue crash window: if the process dies in between, the original
// delivery is still unacked and is redelivered by JetStream instead of being lost.
// Reenqueue reschedules the message using JetStream native NakWithDelay if delay > 0,
// or republishes and acknowledges. Using native NakWithDelay delegates delay scheduling
// directly to the JetStream server, avoiding in-process timers and AckWait expiration.
func (n *NATSAdapter) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	if d == nil || d.Token == nil {
		return nil
	}

	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return fmt.Errorf("queue-nats: adapter is closed")
	}
	n.mu.RUnlock()

	delay := time.Until(at)
	if delay > 0 {
		if natsMsg, ok := d.Token.(*nats.Msg); ok {
			return natsMsg.NakWithDelay(delay)
		}

		// Fallback for non-JetStream or mock tokens: track timer and schedule delayed re-enqueue
		var timer *time.Timer
		timer = time.AfterFunc(delay, func() {
			n.mu.Lock()
			delete(n.retryTimers, timer)
			closed := n.closed
			n.mu.Unlock()
			if closed {
				return
			}
			cbCtx := context.WithoutCancel(ctx)
			if err := n.Enqueue(cbCtx, msg); err == nil {
				_ = n.Ack(cbCtx, d)
			}
		})

		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			timer.Stop()
			return fmt.Errorf("queue-nats: adapter is closed")
		}
		if n.retryTimers == nil {
			n.retryTimers = make(map[*time.Timer]struct{})
		}
		n.retryTimers[timer] = struct{}{}
		n.mu.Unlock()
		return nil
	}
	if err := n.Enqueue(ctx, msg); err != nil {
		return err
	}
	return n.Ack(ctx, d)
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
	for t := range n.retryTimers {
		t.Stop()
	}
	n.retryTimers = nil
	subs := []*nats.Subscription{n.critSub, n.highSub, n.defaultSub, n.lowSub}
	for _, s := range subs {
		if s != nil {
			_ = s.Unsubscribe()
		}
	}
	return nil
}

func errorsIsTimeout(err error) bool {
	return errors.Is(err, nats.ErrTimeout) || strings.Contains(err.Error(), "timeout")
}
