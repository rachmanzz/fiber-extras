package queue

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryDriver selects priority/FIFO among per-group heads using heaps.
type MemoryDriver struct {
	mu       sync.Mutex
	buckets  map[string]*memoryBucket
	heads    memoryHeads
	sequence uint64
	pending  int
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
		name:    name,
		buckets: make(map[string]*memoryBucket),
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

// memoryLess is a transitive priority/FIFO comparator between eligible heads.
// GroupOrder is handled separately inside a group's heap.
func memoryLess(a, b *Message) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

type memoryEntry struct {
	msg      *Message
	sequence uint64
}

func memoryEntryLess(a, b memoryEntry) bool {
	if memoryLess(a.msg, b.msg) {
		return true
	}
	if memoryLess(b.msg, a.msg) {
		return false
	}
	return a.sequence < b.sequence
}

type memoryMessages struct {
	grouped bool
	entries []memoryEntry
}

func (h memoryMessages) Len() int { return len(h.entries) }
func (h memoryMessages) Less(i, j int) bool {
	a, b := h.entries[i], h.entries[j]
	if a.msg.Priority != b.msg.Priority {
		return a.msg.Priority > b.msg.Priority
	}
	if h.grouped && a.msg.GroupOrder != b.msg.GroupOrder {
		return a.msg.GroupOrder < b.msg.GroupOrder
	}
	return memoryEntryLess(a, b)
}
func (h memoryMessages) Swap(i, j int) { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *memoryMessages) Push(v any)   { h.entries = append(h.entries, v.(memoryEntry)) }
func (h *memoryMessages) Pop() any {
	n := len(h.entries) - 1
	v := h.entries[n]
	h.entries[n] = memoryEntry{}
	h.entries = h.entries[:n]
	return v
}

type memoryBucket struct {
	group    string
	index    int
	messages memoryMessages
}
type memoryHeads []*memoryBucket

func (h memoryHeads) Len() int { return len(h) }
func (h memoryHeads) Less(i, j int) bool {
	return memoryEntryLess(h[i].messages.entries[0], h[j].messages.entries[0])
}
func (h memoryHeads) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *memoryHeads) Push(v any)   { b := v.(*memoryBucket); b.index = len(*h); *h = append(*h, b) }
func (h *memoryHeads) Pop() any {
	n := len(*h) - 1
	b := (*h)[n]
	(*h)[n] = nil
	*h = (*h)[:n]
	b.index = -1
	return b
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

	if m.buckets == nil {
		m.buckets = make(map[string]*memoryBucket)
	}
	bucket := m.buckets[msg.GroupID]
	fresh := bucket == nil
	if fresh {
		bucket = &memoryBucket{group: msg.GroupID, messages: memoryMessages{grouped: msg.GroupID != ""}}
		m.buckets[msg.GroupID] = bucket
	}
	m.sequence++
	m.pending++
	heap.Push(&bucket.messages, memoryEntry{msg: msg, sequence: m.sequence})
	if fresh {
		heap.Push(&m.heads, bucket)
	} else {
		heap.Fix(&m.heads, bucket.index)
	}
	return nil
}

func (m *MemoryDriver) Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.heads) == 0 {
		return nil, nil
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}
	capacity := batchSize
	if capacity > m.pending {
		capacity = m.pending
	}
	deliveries := make([]*Delivery, 0, capacity)
	for len(deliveries) < batchSize && len(m.heads) > 0 {
		bucket := m.heads[0]
		item := heap.Pop(&bucket.messages).(memoryEntry)
		m.pending--
		if bucket.messages.Len() == 0 {
			heap.Pop(&m.heads)
			delete(m.buckets, bucket.group)
		} else {
			heap.Fix(&m.heads, 0)
		}
		deliveries = append(deliveries, &Delivery{Message: item.msg, Token: item.msg.ID})
	}
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
	m.heads = nil
	m.pending = 0
	m.buckets = nil
	return nil
}
