package queue

import (
	"context"
	"errors"
	"sync"

	"github.com/rachmanzz/gosuite/v3/worker"
)

var (
	ErrMemoryBudgetExceeded      = errors.New("queue: memory budget exhausted")
	ErrMemoryClaimTooLarge       = errors.New("queue: memory claim exceeds budget limit")
	ErrInvalidMemoryClaim        = errors.New("queue: memory size must be positive and release must not exceed the claim")
	ErrMemoryReservationClosed   = errors.New("queue: memory reservation closed")
	ErrMemoryBudgetNotConfigured = errors.New("queue: no memory budget in handler context")
)

// MemoryBudget bounds explicitly reserved bytes, not heap size or process RSS.
// A budget can be shared by multiple consumers. Use NewMemoryBudget to initialize it.
type MemoryBudget struct {
	mu          sync.Mutex
	limit, used int64
	changed     chan struct{}
}

// NewMemoryBudget creates a budget in bytes. It panics if limit is not positive.
func NewMemoryBudget(limit int64) *MemoryBudget {
	if limit <= 0 {
		panic(ErrInvalidMemoryClaim)
	}
	return &MemoryBudget{limit: limit, changed: make(chan struct{})}
}

// SetLimit changes the budget. Shrinking below current claims is rejected.
func (b *MemoryBudget) SetLimit(limit int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit <= 0 {
		return ErrInvalidMemoryClaim
	}
	if limit < b.used {
		return ErrMemoryBudgetExceeded
	}
	b.limit = limit
	b.signal()
	return nil
}
func (b *MemoryBudget) Limit() int64     { b.mu.Lock(); defer b.mu.Unlock(); return b.limit }
func (b *MemoryBudget) Used() int64      { b.mu.Lock(); defer b.mu.Unlock(); return b.used }
func (b *MemoryBudget) Available() int64 { b.mu.Lock(); defer b.mu.Unlock(); return b.limit - b.used }
func (b *MemoryBudget) signal()          { close(b.changed); b.changed = make(chan struct{}) }

// Claim waits for capacity or context cancellation. Claims larger than the
// entire limit fail immediately. Waiting claims have no FIFO priority.
func (b *MemoryBudget) Claim(ctx context.Context, bytes int64) (*MemoryReservation, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.mu.Lock()
		r, err := b.claim(bytes)
		changed := b.changed
		b.mu.Unlock()
		if !errors.Is(err, ErrMemoryBudgetExceeded) {
			return r, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// TryClaim returns immediately, rejecting work when capacity is unavailable.
func (b *MemoryBudget) TryClaim(bytes int64) (*MemoryReservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.claim(bytes)
}
func (b *MemoryBudget) claim(bytes int64) (*MemoryReservation, error) {
	if bytes <= 0 {
		return nil, ErrInvalidMemoryClaim
	}
	if bytes > b.limit {
		return nil, ErrMemoryClaimTooLarge
	}
	if bytes > b.limit-b.used {
		return nil, ErrMemoryBudgetExceeded
	}
	b.used += bytes
	return &MemoryReservation{budget: b, bytes: bytes}, nil
}

// MemoryReservation owns a claim. Release reclaims part of it, Close reclaims
// the rest. All methods are concurrency-safe; Close is idempotent.
type MemoryReservation struct {
	budget *MemoryBudget
	bytes  int64
	closed bool
}

func (r *MemoryReservation) Bytes() int64 {
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	return r.bytes
}

// TryGrow reserves additional bytes without waiting. Prefer this inside active
// handlers: waiting while holding other claims can deadlock competing handlers.
func (r *MemoryReservation) TryGrow(bytes int64) error {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.closed {
		return ErrMemoryReservationClosed
	}
	if bytes <= 0 {
		return ErrInvalidMemoryClaim
	}
	if bytes > b.limit-r.bytes {
		return ErrMemoryClaimTooLarge
	}
	if bytes > b.limit-b.used {
		return ErrMemoryBudgetExceeded
	}
	b.used += bytes
	r.bytes += bytes
	return nil
}
func (r *MemoryReservation) Release(bytes int64) error {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.closed {
		return ErrMemoryReservationClosed
	}
	if bytes <= 0 || bytes > r.bytes {
		return ErrInvalidMemoryClaim
	}
	r.bytes -= bytes
	b.used -= bytes
	b.signal()
	return nil
}
func (r *MemoryReservation) Close() error {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !r.closed {
		r.closed = true
		b.used -= r.bytes
		r.bytes = 0
		b.signal()
	}
	return nil
}

type memoryScopeKey struct{}
type memoryScope struct {
	mu          sync.Mutex
	finishOnce  sync.Once
	reservation *MemoryReservation
	claims      []*MemoryReservation
	refs        int
}

func newMemoryScope(r *MemoryReservation) *memoryScope {
	return &memoryScope{reservation: r, claims: []*MemoryReservation{r}, refs: 1}
}
func (s *memoryScope) release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.refs--
	if s.refs != 0 {
		s.mu.Unlock()
		return
	}
	claims := s.claims
	s.claims = nil
	s.mu.Unlock()
	for _, r := range claims {
		_ = r.Close()
	}
}
func (s *memoryScope) retain() func() {
	if s == nil {
		return func() {}
	}
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(s.release) }
}
func scopeFromContext(ctx context.Context) *memoryScope {
	s, _ := ctx.Value(memoryScopeKey{}).(*memoryScope)
	return s
}

// MemoryReservationFromContext returns the consumer's admission claim. It is
// automatically closed after execution, sub-workers and core retry timers finish.
// Release only bytes no longer retained; TryGrow before allocating extra buffers.
func MemoryReservationFromContext(ctx context.Context) *MemoryReservation {
	if s := scopeFromContext(ctx); s != nil {
		return s.reservation
	}
	return nil
}

// ClaimMemory makes an additional claim attached to a handler's lifecycle.
// Close it early when possible; otherwise the consumer closes it automatically.
// Do not wait while holding claims that other waiting handlers need to release.
func ClaimMemory(ctx context.Context, bytes int64) (*MemoryReservation, error) {
	return claimMemory(ctx, bytes, false)
}

// TryClaimMemory is the non-blocking version of ClaimMemory.
func TryClaimMemory(ctx context.Context, bytes int64) (*MemoryReservation, error) {
	return claimMemory(ctx, bytes, true)
}
func claimMemory(ctx context.Context, bytes int64, try bool) (*MemoryReservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := scopeFromContext(ctx)
	if s == nil {
		return nil, ErrMemoryBudgetNotConfigured
	}
	var r *MemoryReservation
	var err error
	if try {
		r, err = s.reservation.budget.TryClaim(bytes)
	} else {
		r, err = s.reservation.budget.Claim(ctx, bytes)
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.refs == 0 {
		s.mu.Unlock()
		_ = r.Close()
		return nil, ErrMemoryReservationClosed
	}
	s.claims = append(s.claims, r)
	s.mu.Unlock()
	return r, nil
}

// WithMemoryBudget enables optional admission control. Nil disables it. While
// enabled, each Dequeue requests one message; adapters may still prefetch internally.
func WithMemoryBudget(b *MemoryBudget) ConsumerOption {
	return func(c *Consumer) { c.memoryBudget = b }
}

// WithMemoryEstimate reserves bytes before dequeue, including anticipated handler
// allocations. The default (64 KiB) is capped at the budget limit, but an
// explicit estimate is never reduced: if it exceeds the limit, polling pauses
// with an admission error logged, leaving messages in the broker. Actual message storage
// (payload capacity, strings, headers, and fixed envelope overhead) is also counted.
func WithMemoryEstimate(bytes int64) ConsumerOption {
	return func(c *Consumer) {
		if bytes > 0 {
			c.memoryEstimate = bytes
			c.memoryEstimateExplicit = true
		}
	}
}

func (s *memoryScope) finish() {
	if s != nil {
		s.finishOnce.Do(s.release)
	}
}
func memoryContext(ctx context.Context, s *memoryScope) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, memoryScopeKey{}, s)
}
func groupMemoryContext(ctx context.Context, d *Delivery, scopes []map[*Delivery]*memoryScope) context.Context {
	if len(scopes) > 0 {
		return memoryContext(ctx, scopes[0][d])
	}
	return ctx
}

// messageMemorySize is conservative accounting for storage owned by a delivery.
// Adapter-owned tokens, prefetch buffers and arbitrary handler allocations need
// an explicit estimate or additional claims; no reflection or heap sampling is used.
func messageMemorySize(m *Message) int64 {
	size := int64(512) + int64(cap(m.Payload))
	for _, s := range []string{m.ID, m.Topic, m.GroupID, m.Driver} {
		size += int64(len(s))
	}
	for k, v := range m.Headers {
		size += int64(len(k)) + int64(len(v)) + 64
	}
	return size
}

type scopedMemoryPool struct {
	worker.SubWorkerPool
	scope *memoryScope
	ctx   context.Context
}

func (p scopedMemoryPool) Go(fn worker.SubTaskFunc) {
	p.SubWorkerPool.Go(func(ctx context.Context) error {
		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(p.ctx, cancel)
		defer stop()
		return fn(memoryContext(childCtx, p.scope))
	})
}
func memoryPool(ctx context.Context, pool worker.SubWorkerPool, s *memoryScope) worker.SubWorkerPool {
	if s == nil {
		return pool
	}
	return scopedMemoryPool{SubWorkerPool: pool, scope: s, ctx: ctx}
}

// Only the implicit default adapts to small budgets. An explicit working-set
// declaration must fit in full, including after SetLimit changes the limit.
func (c *Consumer) admissionMemoryEstimate() int64 {
	estimate := c.memoryEstimate
	if !c.memoryEstimateExplicit {
		if limit := c.memoryBudget.Limit(); estimate > limit {
			estimate = limit
		}
	}
	return estimate
}
