package queue

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryDriver implements Adapter using an in-memory priority slice.
// Used as the default zero-config driver for testing and lightweight local development.
type MemoryDriver struct {
	name     string
	mu       sync.Mutex
	messages []*Message
	closed   bool
}

// NewMemoryDriver constructs a new in-memory queue driver with default name "memory".
func NewMemoryDriver() *MemoryDriver {
	return NewNamedMemoryDriver("memory")
}

// NewNamedMemoryDriver constructs a named in-memory queue driver.
func NewNamedMemoryDriver(name string) *MemoryDriver {
	if name == "" {
		name = "memory"
	}
	return &MemoryDriver{
		name:     name,
		messages: make([]*Message, 0, 100),
	}
}

func (m *MemoryDriver) Name() string {
	return m.name
}

func (m *MemoryDriver) Capabilities() Capabilities {
	return Capabilities{
		Durable:           false,
		Persistent:        false,
		NativeRedelivery:  false,
		NativePriority:    PriorityHard,
		DelayedEnqueue:    true,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             false,
		MaxBatchSize:      1000,
		Guarantee:         AtMostOnce,
	}
}

func (m *MemoryDriver) Enqueue(ctx context.Context, msg *Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = append(m.messages, msg)
	// Sort by Priority DESC, then GroupOrder ASC if in same group, then CreatedAt ASC
	sort.SliceStable(m.messages, func(i, j int) bool {
		if m.messages[i].Priority != m.messages[j].Priority {
			return m.messages[i].Priority > m.messages[j].Priority
		}
		if m.messages[i].GroupID != "" && m.messages[i].GroupID == m.messages[j].GroupID {
			return m.messages[i].GroupOrder < m.messages[j].GroupOrder
		}
		return m.messages[i].CreatedAt.Before(m.messages[j].CreatedAt)
	})
	return nil
}

func (m *MemoryDriver) Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) == 0 {
		return nil, nil
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	count := batchSize
	if len(m.messages) < count {
		count = len(m.messages)
	}

	deliveries := make([]*Delivery, count)
	for i := 0; i < count; i++ {
		deliveries[i] = &Delivery{
			Message: m.messages[i],
			Token:   m.messages[i].ID,
		}
	}
	m.messages = m.messages[count:]

	return deliveries, nil
}

func (m *MemoryDriver) Ack(ctx context.Context, d *Delivery) error {
	return nil
}

func (m *MemoryDriver) Nack(ctx context.Context, d *Delivery, reason error) error {
	if d == nil || d.Message == nil {
		return nil
	}
	return m.Enqueue(ctx, d.Message)
}

func (m *MemoryDriver) EnqueueAt(ctx context.Context, msg *Message, at time.Time) error {
	delay := time.Until(at)
	if delay <= 0 {
		return m.Enqueue(ctx, msg)
	}
	time.AfterFunc(delay, func() {
		_ = m.Enqueue(context.Background(), msg)
	})
	return nil
}

func (m *MemoryDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.messages = nil
	return nil
}
