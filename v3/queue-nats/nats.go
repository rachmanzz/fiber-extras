package queuenats

import (
	"context"
	"crypto/sha256"
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
	hdrNotBefore  = "fiber-not-before"
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
	m.Header.Set(nats.MsgIdHdr, publicationID(msg))
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

// Logical IDs remain stable across retries. A new handler-failure attempt
// needs its own dedup identity, while a repeated publish of that attempt must
// still deduplicate (including after a lost publish response or original Ack).
func publicationID(msg *queue.Message) string {
	if msg.Attempt == 0 {
		return msg.ID
	}
	key := fmt.Sprintf("%d:%s%d:%s:%d", len(msg.Topic), msg.Topic, len(msg.ID), msg.ID, msg.Attempt)
	return fmt.Sprintf("gosuite-retry:%x", sha256.Sum256([]byte(key)))
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
	// Attempt is core-owned handler-failure state, not the broker delivery count.
	// Admission, worker rejection, group deferral and lease recovery must leave
	// it unchanged. Reenqueue publishes an updated envelope after a failure.
	if attempt < 0 {
		return nil, fmt.Errorf("invalid %s: negative attempt", hdrAttempt)
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

// sanitizeNATSToken replaces whitespace, NATS wildcard tokens (*, >), and separators
// so arbitrary group IDs and topic strings form valid, publishable NATS subjects.
func sanitizeNATSToken(token string) string {
	if token == "" {
		return ""
	}
	var sb strings.Builder
	sb.Grow(len(token))
	for _, r := range token {
		switch {
		case r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == '*' || r == '>':
			sb.WriteRune('_')
		case r == ':':
			sb.WriteRune('.')
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
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
		groupStr = sanitizeNATSToken(msg.GroupID)
	}

	safeTopic := sanitizeNATSToken(msg.Topic)
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

	var opts []nats.PubOpt
	if ctx != nil {
		opts = append(opts, nats.Context(ctx))
	}
	_, err := n.js.PublishMsg(buildNATSMsg(n.subjectFor(msg), msg), opts...)
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

	if ctx == nil {
		ctx = context.Background()
	}
	for i, sub := range subs {
		if len(deliveries) >= batchSize {
			break
		}
		if sub == nil {
			continue
		}
		if ctx.Err() != nil {
			return deliveries, ctx.Err()
		}

		remaining := batchSize - len(deliveries)
		// An empty priority must not use the caller's entire deadline. Bound
		// each Fetch and share short deadlines with priorities still to poll,
		// leaving an extra slice for request/processing overhead.
		wait := 20 * time.Millisecond
		if deadline, ok := ctx.Deadline(); ok {
			left := time.Until(deadline)
			if left <= 0 {
				return deliveries, context.DeadlineExceeded
			}
			if share := left / time.Duration(len(subs)-i+1); share < wait {
				wait = share
			}
		}
		fetchCtx, cancel := context.WithTimeout(ctx, wait)
		msgs, err := sub.Fetch(remaining, nats.Context(fetchCtx))
		cancel()
		if err != nil {
			if parentErr := ctx.Err(); parentErr != nil {
				return deliveries, parentErr
			}
			if errorsIsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
				// Only this priority's time slice ended; keep polling the rest.
				continue
			}
			return deliveries, err
		}

		for j, rawMsg := range msgs {
			m, err := decodeNATSMsg(rawMsg)
			var readyAt time.Time
			if err == nil && rawMsg.Header.Get(hdrNotBefore) != "" {
				readyAt, err = time.Parse(time.RFC3339Nano, rawMsg.Header.Get(hdrNotBefore))
			}
			if err != nil {
				// Undecodable or non-split-transport record: publish a poison
				// copy before terminating the original. On failure, release
				// this record and the unvisited tail, preserving the valid
				// prefix for the caller to settle.
				// Poison publication must not deduplicate against the corrupt source.
				poison := queue.NewPoisonMessage(queue.NewUUID(), rawMsg.Data, err, n.Name())
				poison.Headers["source_message_id"] = rawMsg.Header.Get(hdrID)
				poison.Headers["source_publication_id"] = rawMsg.Header.Get(nats.MsgIdHdr)
				if pErr := n.Enqueue(ctx, poison); pErr != nil {
					return deliveries, releaseFetchedMessages(msgs[j:], fmt.Errorf("queue-nats: failed to surface undecodable message to DLQ: %w", pErr))
				}
				if err := rawMsg.Term(); err != nil {
					return deliveries, releaseFetchedMessages(msgs[j:], err)
				}
				continue
			}

			if delay := time.Until(readyAt); delay > 0 {
				// The retry is already durable. Keep it on the server until due,
				// without exposing it to a handler or incrementing Attempt.
				if err := rawMsg.NakWithDelay(delay); err != nil {
					return deliveries, releaseFetchedMessages(msgs[j:], err)
				}
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

// releaseFetchedMessages releases claims that cannot be returned to the caller.
// Try every record even if one NAK fails; retain both the original and cleanup
// errors so callers can diagnose records left for JetStream lease recovery.
func releaseFetchedMessages(msgs []*nats.Msg, cause error) error {
	for _, msg := range msgs {
		if err := msg.Nak(); err != nil {
			cause = errors.Join(cause, fmt.Errorf("queue-nats: failed to release fetched message: %w", err))
		}
	}
	return cause
}

// Reenqueue persists a handler retry before acknowledging its source. Delayed
// retries carry their due time in a reserved header; Dequeue defers them on the
// server until due. No process-local timer holds the only updated attempt state.
// An unchanged Attempt is a group deferral, which uses native NAK instead.
func (n *NATSAdapter) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	if d == nil || d.Token == nil {
		return nil
	}
	if msg == nil {
		return ErrNilMessage
	}
	raw, ok := d.Token.(*nats.Msg)
	if !ok {
		return fmt.Errorf("queue-nats: invalid delivery token: %T", d.Token)
	}
	n.mu.RLock()
	closed, js := n.closed, n.js
	n.mu.RUnlock()
	if closed {
		return fmt.Errorf("queue-nats: adapter is closed")
	}
	base, err := strconv.Atoi(raw.Header.Get(hdrAttempt))
	if err != nil || base < 0 || msg.Attempt < base {
		return fmt.Errorf("queue-nats: invalid retry attempt (published=%q, retry=%d)", raw.Header.Get(hdrAttempt), msg.Attempt)
	}
	if msg.Attempt == base {
		if delay := time.Until(at); delay > 0 {
			return raw.NakWithDelay(delay)
		}
		return raw.Nak()
	}
	if js == nil {
		return fmt.Errorf("queue-nats: jetstream context is not initialized")
	}
	retry := buildNATSMsg(n.subjectFor(msg), msg)
	if at.After(time.Now()) {
		retry.Header.Set(hdrNotBefore, at.UTC().Format(time.RFC3339Nano))
	}
	var opts []nats.PubOpt
	if ctx != nil {
		opts = append(opts, nats.Context(ctx))
	}
	ack, err := js.PublishMsg(retry, opts...)
	if err != nil {
		return err
	}
	if ack == nil {
		return errors.New("queue-nats: retry publication was not confirmed")
	}
	// Duplicate now identifies this retry attempt, never the original publish.
	return raw.Ack()
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
	return errors.Is(err, nats.ErrTimeout) || strings.Contains(err.Error(), "timeout")
}
