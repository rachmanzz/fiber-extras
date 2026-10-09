package queue

import (
	"context"
	"time"
)

// Guarantee represents the delivery guarantee level.
type Guarantee string

const (
	AtMostOnce      Guarantee = "at-most-once"
	AtLeastOnce     Guarantee = "at-least-once"
	EffectivelyOnce Guarantee = "effectively-once"
)

// PriorityStrength represents how strictly a broker enforces priority ordering.
type PriorityStrength int

const (
	PriorityNone PriorityStrength = iota // Not supported
	PrioritySoft                         // Preference / best-effort order
	PriorityHard                         // Strict priority ordering
)

// Capabilities declares the feature set and durability guarantees of a queue adapter.
type Capabilities struct {
	Durable           bool             // Broker survives restart without message loss
	Persistent        bool             // Broker redelivers unacknowledged messages upon consumer crash
	NativeRedelivery  bool             // Broker automatically manages redelivery timeouts
	NativePriority    PriorityStrength // Priority guarantee strength
	DelayedEnqueue    bool             // Supports scheduled future enqueue
	StrictGlobalOrder bool             // Supports global FIFO ordering
	Replay            bool             // Supports replay by sequence or timestamp
	Dedup             bool             // Supports deduplication window
	MaxBatchSize      int              // Maximum recommended batch size
	Guarantee         Guarantee        // Delivery guarantee level
}

// Delivery carries the message along with a broker-opaque delivery token.
// The Token holds broker-native handles (*nats.Msg, Redis ZSet member, PG row id, memory ID)
// eliminating stateful in-memory handle maps and avoiding handle leaks.
type Delivery struct {
	Message *Message
	Token   any
}

// DequeueRequest encapsulates request parameters for pulling messages.
type DequeueRequest struct {
	BatchSize         int
	Priorities        []Priority
	Groups            []string
	VisibilityTimeout time.Duration
}

// Adapter defines the transport and storage contract that any message broker
// must conform to in order to plug into the Queue Core.
type Adapter interface {
	// Name returns the adapter identifier (e.g. "redis", "postgres", "nats", "memory").
	Name() string

	// Capabilities returns the declared capability set of this adapter.
	Capabilities() Capabilities

	// Enqueue pushes a message into the broker queue following priority.
	Enqueue(ctx context.Context, msg *Message) error

	// Dequeue fetches messages using the DequeueRequest specification.
	Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error)

	// Ack marks the message as successfully processed in the broker using its delivery token.
	Ack(ctx context.Context, d *Delivery) error

	// Nack reports a failure and triggers broker-specific retry/requeue.
	Nack(ctx context.Context, d *Delivery, reason error) error

	// Close gracefully shuts down the adapter connection.
	Close() error
}

// Driver is an alias of Adapter for backwards compatibility.
type Driver = Adapter

// Optional Capability Interfaces:

// Replayer allows consumers to replay messages from a given cursor or timestamp.
type Replayer interface {
	Replay(ctx context.Context, from time.Time) (<-chan *Delivery, error)
}

// DelayedEnqueuer allows enqueuing messages with execution delayed to a future timestamp.
type DelayedEnqueuer interface {
	EnqueueAt(ctx context.Context, msg *Message, at time.Time) error
}

// Stats represents operational metrics of the queue.
type Stats struct {
	PendingCount int64
	InFlight     int64
	DeadLetter   int64
}

// StatsProvider exposes operational statistics from the broker.
type StatsProvider interface {
	Stats(ctx context.Context) (Stats, error)
}

// Pauser allows temporarily pausing and resuming dequeue operations.
type Pauser interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
}
