package queue

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rachmanzz/gosuite/v3/worker"
)

// Priority represents message execution order priority.
type Priority int

const (
	PriorityLow      Priority = 25
	PriorityDefault  Priority = 50
	PriorityHigh     Priority = 75
	PriorityCritical Priority = 100
)

// Message represents an enqueued unit of work.
type Message struct {
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Payload     []byte            `json:"payload"`
	Priority    Priority          `json:"priority"`
	GroupID     string            `json:"group_id,omitempty"`    // Optional group grouping
	GroupOrder  int               `json:"group_order,omitempty"` // Sequential ordering index within group
	Headers     map[string]string `json:"headers,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	Attempt     int               `json:"attempt"`          // Core-owned attempt count
	MaxAttempts int               `json:"max_attempts"`     // Core-owned max attempts before DLQ
	Driver      string            `json:"driver,omitempty"` // Explicit driver target (optional)

	codec Codec // Payload codec selected via WithCodec; never serialized on the wire.
}

// Handler defines the processing function for a topic, receiving Worker SubWorkerPool.
type Handler func(ctx context.Context, msg *Message, pool worker.SubWorkerPool) error

// HandlerOption configures registration options for a topic handler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	driverName     string
	allowOverwrite bool
}

// WithHandlerDriver specifies which queue driver this topic handler should bind to.
// If omitted, the topic handler will listen on the default driver.
func WithHandlerDriver(driverName string) HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.driverName = driverName
	}
}

// WithAllowOverwrite allows registering a handler for a topic that was already registered,
// overriding the previously registered handler without panicking.
func WithAllowOverwrite() HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.allowOverwrite = true
	}
}

// DispatchOption allows configuring message parameters during dispatch.
type DispatchOption func(*Message)

func WithDriver(driverName string) DispatchOption {
	return func(m *Message) {
		m.Driver = driverName
	}
}

func WithPriority(p Priority) DispatchOption {
	return func(m *Message) {
		m.Priority = p
	}
}

func WithGroup(groupID string, order ...int) DispatchOption {
	return func(m *Message) {
		m.GroupID = groupID
		if len(order) > 0 {
			m.GroupOrder = order[0]
		}
	}
}

func WithHeaders(headers map[string]string) DispatchOption {
	return func(m *Message) {
		if m.Headers == nil {
			m.Headers = make(map[string]string)
		}
		for k, v := range headers {
			m.Headers[k] = v
		}
	}
}

func WithMaxAttempts(max int) DispatchOption {
	return func(m *Message) {
		m.MaxAttempts = max
	}
}

// NewMessage constructs a message with default values and options applied.
func NewMessage(topic string, payload []byte, opts ...DispatchOption) *Message {
	msg := &Message{
		ID:          NewUUID(),
		Topic:       topic,
		Payload:     payload,
		Priority:    PriorityDefault,
		CreatedAt:   time.Now(),
		Attempt:     0,
		MaxAttempts: 3,
		Headers:     make(map[string]string),
	}
	for _, opt := range opts {
		opt(msg)
	}
	return msg
}

// NewUUID generates a RFC4122 v4 UUID string, powered by github.com/google/uuid.
func NewUUID() string {
	return uuid.NewString()
}
