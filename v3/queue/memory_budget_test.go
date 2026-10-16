package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

func TestMemoryBudgetReservations(t *testing.T) {
	b := NewMemoryBudget(100)
	r, err := b.TryClaim(80)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.TryClaim(21); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatal(err)
	}
	if _, err = b.Claim(context.Background(), 101); !errors.Is(err, ErrMemoryClaimTooLarge) {
		t.Fatal(err)
	}
	if err = r.Release(81); !errors.Is(err, ErrInvalidMemoryClaim) {
		t.Fatal(err)
	}
	if err = b.SetLimit(79); !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatal(err)
	}
	if err = r.Release(30); err != nil {
		t.Fatal(err)
	}
	if err = r.TryGrow(40); err != nil {
		t.Fatal(err)
	}
	if b.Used() != 90 || b.Available() != 10 {
		t.Fatalf("used=%d available=%d", b.Used(), b.Available())
	}
	r.Close()
	r.Close()
	if b.Used() != 0 {
		t.Fatal("double close leaked or over-released")
	}
	if err = r.TryGrow(1); !errors.Is(err, ErrMemoryReservationClosed) {
		t.Fatal(err)
	}
}
func TestMemoryBudgetWaitCancellationAndResize(t *testing.T) {
	b := NewMemoryBudget(100)
	r, _ := b.TryClaim(100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Claim(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	result := make(chan *MemoryReservation, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := b.Claim(ctx, 20)
		if err != nil {
			result <- nil
			return
		}
		result <- r
	}()
	if err := b.SetLimit(120); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got == nil {
			t.Fatal("claim did not wake")
		}
		got.Close()
	case <-time.After(time.Second):
		t.Fatal("resize did not wake claimant")
	}
	r.Close()
}
func TestMemoryBudgetConcurrentClaims(t *testing.T) {
	b := NewMemoryBudget(100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				r, err := b.Claim(ctx, 10)
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				if b.Used() > b.Limit() {
					t.Error("budget exceeded")
				}
				r.Release(5)
				r.Close()
			}
		}()
	}
	wg.Wait()
	if b.Used() != 0 {
		t.Fatalf("leak %d", b.Used())
	}
}

type budgetDispatcher struct {
	tasks  []worker.Task
	reject bool
}

func (d *budgetDispatcher) Dispatch(_ context.Context, task worker.Task) error {
	if d.reject {
		return worker.ErrQueueFull
	}
	d.tasks = append(d.tasks, task)
	return nil
}
func (d *budgetDispatcher) ExecuteSync(context.Context, worker.Task) error { return nil }
func runBudgetTasks(t *testing.T, d *budgetDispatcher) {
	t.Helper()
	e := worker.New(worker.WithLogger(worker.NopLogger()))
	defer e.Shutdown()
	for _, task := range d.tasks {
		if err := e.ExecuteSync(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	d.tasks = nil
}
func newBudgetConsumer(d Driver, r *Router, disp worker.Dispatcher, b *MemoryBudget, opts ...ConsumerOption) *Consumer {
	base := []ConsumerOption{WithMemoryBudget(b), WithMemoryEstimate(1024), WithLogger(worker.NopLogger())}
	return NewConsumer(d, r, disp, append(base, opts...)...)
}
func TestConsumerMemoryBudgetClaimsSubWorkers(t *testing.T) {
	b := NewMemoryBudget(4096)
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(ctx context.Context, _ *Message, pool worker.SubWorkerPool) error {
		if r := MemoryReservationFromContext(ctx); r == nil || r.Bytes() != 1024 {
			t.Error("missing admission reservation")
		}
		r, err := TryClaimMemory(ctx, 1024)
		if err != nil {
			return err
		}
		if err = r.Release(512); err != nil {
			return err
		}
		pool.Go(func(ctx context.Context) error { _, err := TryClaimMemory(ctx, 1024); return err })
		return nil // Engine must join this subworker before auto-release.
	})
	c := newBudgetConsumer(d, router, disp, b)
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", nil))
	if c.fetchAndProcess() != 1 || b.Used() != 1024 {
		t.Fatal("queued payload not reserved")
	}
	runBudgetTasks(t, disp)
	if b.Used() != 0 {
		t.Fatalf("handler/subworker claims leaked: %d", b.Used())
	}
}
func TestConsumerMemoryBudgetGroupBacklog(t *testing.T) {
	b := NewMemoryBudget(2048)
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	router := NewRouter()
	var got []int
	router.Register("x", func(ctx context.Context, m *Message, _ worker.SubWorkerPool) error {
		if MemoryReservationFromContext(ctx) == nil {
			t.Error("missing group reservation")
		}
		got = append(got, m.GroupOrder)
		return nil
	})
	c := newBudgetConsumer(d, router, disp, b)
	defer c.Stop()
	for i := 1; i <= 2; i++ {
		d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup("g", i)))
		c.fetchAndProcess()
	}
	if b.Used() != 2048 || len(disp.tasks) != 1 {
		t.Fatal("group backlog is not accounted")
	}
	runBudgetTasks(t, disp)
	if b.Used() != 0 || len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("used=%d order=%v", b.Used(), got)
	}
}

type countingBudgetDriver struct {
	*MemoryDriver
	dequeues atomic.Int32
}

func (d *countingBudgetDriver) Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error) {
	d.dequeues.Add(1)
	return d.MemoryDriver.Dequeue(ctx, req)
}
func TestConsumerMemoryBudgetPauseAndStop(t *testing.T) {
	b := NewMemoryBudget(1024)
	held, _ := b.TryClaim(1024)
	defer held.Close()
	d := &countingBudgetDriver{MemoryDriver: NewMemoryDriver()}
	c := newBudgetConsumer(d, NewRouter(), &budgetDispatcher{}, b, WithPollInterval(time.Millisecond))
	c.Start()
	time.Sleep(20 * time.Millisecond)
	if d.dequeues.Load() != 0 {
		t.Fatal("dequeued while budget exhausted")
	}
	done := make(chan struct{})
	go func() { c.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked on admission")
	}
	if b.Used() != 1024 {
		t.Fatal("consumer released external claim")
	}
}
func TestConsumerMemoryBudgetDispatchRejectAndOversize(t *testing.T) {
	for _, group := range []string{"", "g"} {
		b := NewMemoryBudget(1024)
		d := NewMemoryDriver()
		disp := &budgetDispatcher{reject: true}
		c := newBudgetConsumer(d, NewRouter(), disp, b)
		d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup(group, 1)))
		if c.fetchAndProcess() != 0 || b.Used() != 0 {
			t.Fatal("rejected dispatch leaked")
		}
		ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
		if len(ds) != 1 {
			t.Fatal("rejected message lost")
		}
		c.Stop()
	}
	b := NewMemoryBudget(1024)
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	c := newBudgetConsumer(d, NewRouter(), disp, b)
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", make([]byte, 2048)))
	if c.fetchAndProcess() != 0 || b.Used() != 0 {
		t.Fatal("oversize admitted or leaked")
	}
	runBudgetTasks(t, disp)
	ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
	if len(ds) != 1 || ds[0].Message.Topic != "x:dlq" {
		t.Fatal("oversize not routed to DLQ")
	}
}

// Expose only Driver so retry uses the core timer, rather than MemoryDriver's timer.
type fallbackBudgetDriver struct{ Driver }

func TestConsumerMemoryBudgetRetainsFallbackRetry(t *testing.T) {
	b := NewMemoryBudget(2048)
	d := fallbackBudgetDriver{Driver: NewMemoryDriver()}
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { return errors.New("retry") })
	c := newBudgetConsumer(d, router, disp, b, WithRetryPolicy(RetryPolicy{MaxAttempts: 3, Initial: time.Hour, Max: time.Hour}))
	d.Enqueue(context.Background(), NewMessage("x", nil))
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if b.Used() != 1024 {
		t.Fatalf("retry timer payload not reserved: %d", b.Used())
	}
	c.Stop()
	if b.Used() != 0 {
		t.Fatalf("retry flush leaked: %d", b.Used())
	}
}

func TestConsumerSharedMemoryBudgetResumes(t *testing.T) {
	b := NewMemoryBudget(1024)
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { return nil })
	d1, d2 := NewMemoryDriver(), NewMemoryDriver()
	p1, p2 := &budgetDispatcher{}, &budgetDispatcher{}
	c1, c2 := newBudgetConsumer(d1, router, p1, b), newBudgetConsumer(d2, router, p2, b)
	defer c1.Stop()
	defer c2.Stop()
	d1.Enqueue(context.Background(), NewMessage("x", nil))
	d2.Enqueue(context.Background(), NewMessage("x", nil))
	c1.fetchAndProcess()
	fetched := make(chan int, 1)
	go func() { fetched <- c2.fetchAndProcess() }()
	select {
	case <-fetched:
		t.Fatal("shared budget not enforced")
	case <-time.After(20 * time.Millisecond):
	}
	runBudgetTasks(t, p1)
	select {
	case n := <-fetched:
		if n != 1 {
			t.Fatalf("fetched=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("release did not resume second consumer")
	}
	runBudgetTasks(t, p2)
	if b.Used() != 0 {
		t.Fatal("shared budget leaked")
	}
}
func TestConsumerMemoryBudgetPanicJoinsSubWorkers(t *testing.T) {
	b := NewMemoryBudget(4096)
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	router := NewRouter()
	started, release := make(chan struct{}), make(chan struct{})
	router.Register("x", func(ctx context.Context, _ *Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error {
			_, err := TryClaimMemory(ctx, 1024)
			close(started)
			<-release
			return err
		})
		panic("handler panic")
	})
	c := newBudgetConsumer(d, router, disp, b)
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", nil))
	c.fetchAndProcess()
	done := make(chan error, 1)
	e := worker.New(worker.WithLogger(worker.NopLogger()))
	defer e.Shutdown()
	go func() { done <- e.ExecuteSync(context.Background(), disp.tasks[0]) }()
	<-started
	if b.Used() != 2048 {
		t.Fatalf("claim released before subworker joined: %d", b.Used())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if b.Used() != 0 {
		t.Fatal("panic leaked claims")
	}
}
func TestConsumerMemoryBudgetFallbackTimerCompletes(t *testing.T) {
	b := NewMemoryBudget(2048)
	d := fallbackBudgetDriver{Driver: NewMemoryDriver()}
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { return errors.New("retry") })
	c := newBudgetConsumer(d, router, disp, b, WithRetryPolicy(RetryPolicy{MaxAttempts: 3, Initial: 10 * time.Millisecond, Max: 10 * time.Millisecond}))
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", nil))
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	deadline := time.Now().Add(time.Second)
	for b.Used() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.Used() != 0 {
		t.Fatal("completed timer leaked reservation")
	}
	ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
	if len(ds) != 1 {
		t.Fatal("retry message lost")
	}
}

func TestConsumerMemoryBudgetCancelsWaitingSubWorker(t *testing.T) {
	b := NewMemoryBudget(1024)
	d := NewMemoryDriver()
	router := NewRouter()
	entered := make(chan struct{})
	router.Register("x", func(ctx context.Context, _ *Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error { close(entered); _, err := ClaimMemory(ctx, 1024); return err })
		return pool.Wait()
	})
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	defer engine.Shutdown()
	c := newBudgetConsumer(d, router, engine, b, WithPollInterval(time.Millisecond))
	d.Enqueue(context.Background(), NewMessage("x", nil))
	c.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	done := make(chan struct{})
	go func() { c.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer stop did not cancel subworker claim")
	}
	if b.Used() != 0 {
		t.Fatalf("cancelled subworker leaked %d bytes", b.Used())
	}
}
