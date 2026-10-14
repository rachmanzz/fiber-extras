package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryDriver is an in-process queue adapter backed by a sorted slice.
type MemoryDriver struct {
	mu       sync.Mutex
	messages []*Message
	timers   map[*time.Timer]struct{}
	closed   bool

	name string
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

func (m *MemoryDriver) Name() string { return m.name }

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

// memoryLess reports whether a should be dequeued before b: higher priority
// first, then ascending GroupOrder within the same group, then FIFO by CreatedAt.
func memoryLess(a, b *Message) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if a.GroupID != "" && a.GroupID == b.GroupID {
		return a.GroupOrder < b.GroupOrder
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

func (m *MemoryDriver) Enqueue(ctx context.Context, msg *Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("queue-memory: driver is closed")
	}

	// Insert into the already-sorted slice at the boundary found by binary
	// search (O(log n) lookup + O(n) shift) instead of re-sorting the whole
	// slice on every Enqueue.
	idx := sort.Search(len(m.messages), func(i int) bool {
		return memoryLess(msg, m.messages[i])
	})
	m.messages = append(m.messages, nil)
	copy(m.messages[idx+1:], m.messages[idx:])
	m.messages[idx] = msg
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

// Reenqueue reschedules a failed delivery to a future time. The in-memory slice
// already removed the delivery at Dequeue time, so inserting one copy at the
// target time cannot create duplicates or lose the message.
func (m *MemoryDriver) Reenqueue(ctx context.Context, d *Delivery, msg *Message, at time.Time) error {
	if d == nil || d.Message == nil {
		return nil
	}
	return m.EnqueueAt(ctx, msg, at)
}

func (m *MemoryDriver) EnqueueAt(ctx context.Context, msg *Message, at time.Time) error {
	if msg == nil {
		return ErrNilMessage
	}
	delay := time.Until(at)
	if delay <= 0 {
		return m.Enqueue(ctx, msg)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("queue-memory: driver is closed")
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		m.mu.Lock()
		delete(m.timers, timer)
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return
		}
		_ = m.Enqueue(context.Background(), msg)
	})
	if m.timers == nil {
		m.timers = make(map[*time.Timer]struct{})
	}
	m.timers[timer] = struct{}{}
	m.mu.Unlock()
	return nil
}

func (m *MemoryDriver) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for t := range m.timers {
		t.Stop()
	}
	m.timers = nil
	m.messages = nil
	return nil
}
