package queue_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/rachmanzz/gosuite/v3/worker"
)

// TestConsumer_HandlerPanicRoutesToDLQ verifies that a panic raised by a topic
// handler is routed through the failure path (DLQ here) instead of being
// swallowed by the worker engine, which would leave the delivery stuck.
func TestConsumer_HandlerPanicRoutesToDLQ(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 16})
	engine.Start()
	defer engine.Shutdown()

	var panickedCalls int32
	var dlqCalls int32

	router.Register("app.job", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&panickedCalls, 1)
		panic("boom: unexpected handler failure")
	})
	router.Register(queue.DLQTopic("app.job"), func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&dlqCalls, 1)
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(5*time.Millisecond))
	consumer.Start()
	defer consumer.Stop()

	msg := queue.NewMessage("app.job", []byte("payload"), queue.WithMaxAttempts(1))
	if err := driver.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&dlqCalls) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&dlqCalls) != 1 {
		t.Fatalf("expected DLQ handler to be invoked once, got %d", dlqCalls)
	}
	if atomic.LoadInt32(&panickedCalls) != 1 {
		t.Fatalf("expected handler to panic once, got %d", panickedCalls)
	}
}

// TestConsumer_GroupedHandlerPanicRoutesToDLQ exercises the same guarantee for
// messages delivered inside a GroupID so those deliveries are not leaked.
func TestConsumer_GroupedHandlerPanicRoutesToDLQ(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 16})
	engine.Start()
	defer engine.Shutdown()

	var dlqCalls int32
	router.Register("group.job", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		panic("group boom")
	})
	router.Register(queue.DLQTopic("group.job"), func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&dlqCalls, 1)
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(5*time.Millisecond))
	consumer.Start()
	defer consumer.Stop()

	msg := queue.NewMessage("group.job", []byte("payload"), queue.WithMaxAttempts(1), queue.WithGroup("grp-1"))
	if err := driver.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&dlqCalls) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&dlqCalls) != 1 {
		t.Fatalf("expected DLQ handler to be invoked once, got %d", dlqCalls)
	}
}

// TestConsumer_GroupOrderStrict verifies that members of the same group are
// processed strictly sequentially in ascending GroupOrder: no two members ever
// overlap in time and the recorded order matches the declared order.
func TestConsumer_GroupOrderStrict(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 16})
	engine.Start()
	defer engine.Shutdown()

	type record struct {
		order      int
		start, end time.Time
	}
	var mu sync.Mutex
	var records []record

	router.Register("g.ordered", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		start := time.Now()
		if msg.GroupOrder == 1 {
			time.Sleep(250 * time.Millisecond)
		}
		mu.Lock()
		records = append(records, record{order: msg.GroupOrder, start: start, end: time.Now()})
		mu.Unlock()
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine,
		queue.WithPollInterval(5*time.Millisecond),
		queue.WithBatchSize(10),
	)
	consumer.Start()
	defer consumer.Stop()

	ctx := context.Background()
	for _, order := range []int{3, 1, 2} {
		m := queue.NewMessage("g.ordered", []byte("x"), queue.WithGroup("g-1", order))
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(records)
		mu.Unlock()
		if n == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(records) != 3 {
		t.Fatalf("expected 3 processed records, got %d", len(records))
	}
	for i, r := range records {
		if r.order != i+1 {
			t.Fatalf("expected GroupOrder %d at position %d, got %d", i+1, i, r.order)
		}
		if i > 0 {
			prev := records[i-1]
			if prev.end.After(r.start) {
				t.Fatalf("group members overlapped: order %d ended at %v after order %d started at %v",
					prev.order, prev.end, r.order, r.start)
			}
		}
	}
}

// TestConsumer_RetryPreservesAttemptWithoutDuplicate verifies the Reenqueue retry
// path: a transiently failing message is redelivered exactly once with Attempt
// incremented, instead of being acked-and-re-enqueued (which could lose or duplicate it).
func TestConsumer_RetryPreservesAttemptWithoutDuplicate(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 16})
	engine.Start()
	defer engine.Shutdown()

	var calls int32
	var attempts int32
	router.Register("retry.once", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&calls, 1)
		atomic.StoreInt32(&attempts, int32(msg.Attempt))
		if atomic.LoadInt32(&calls) == 1 {
			return errors.New("transient failure")
		}
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine,
		queue.WithPollInterval(5*time.Millisecond),
		queue.WithRetryPolicy(queue.RetryPolicy{
			MaxAttempts: 3,
			Initial:     15 * time.Millisecond,
			Max:         20 * time.Millisecond,
			Multiplier:  1.0,
			Jitter:      0,
		}),
	)
	consumer.Start()
	defer consumer.Stop()

	m := queue.NewMessage("retry.once", []byte("x"))
	if err := driver.Enqueue(context.Background(), m); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected exactly 2 handler invocations (1 failure + 1 retry), got %d — message duplicated or lost", got)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("expected retry delivery to carry Attempt=1, got %d", got)
	}
}

// TestConsumer_PoisonMessageRoutesToDLQ verifies that an undecodable broker
// payload surfaced as a poison message (queue.NewPoisonMessage) has no handler,
// is treated as non-retryable (ErrNoHandlerFound), and is routed to the DLQ
// for queue.PoisonTopic instead of being dropped silently.
func TestConsumer_PoisonMessageRoutesToDLQ(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 16})
	engine.Start()
	defer engine.Shutdown()

	var dlqCalls int32
	router.Register(queue.DLQTopic(queue.PoisonTopic), func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&dlqCalls, 1)
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(5*time.Millisecond))
	consumer.Start()
	defer consumer.Stop()

	poison := queue.NewPoisonMessage("corrupt-1", []byte("not json"), errors.New("invalid character"), "test")
	if err := driver.Enqueue(context.Background(), poison); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&dlqCalls) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt32(&dlqCalls); got != 1 {
		t.Fatalf("expected poison message routed to %q once, got %d", queue.DLQTopic(queue.PoisonTopic), got)
	}
}

// countingDriver wraps MemoryDriver to count Close invocations.
type countingDriver struct {
	*queue.MemoryDriver
	closes int32
}

func (c *countingDriver) Close() error {
	atomic.AddInt32(&c.closes, 1)
	return c.MemoryDriver.Close()
}

// TestConsumer_StopIsIdempotent verifies that calling Stop more than once closes
// the driver exactly once, so a caller (or a supervising runner) can stop
// repeatedly without double-closing shared driver resources.
func TestConsumer_StopIsIdempotent(t *testing.T) {
	driver := &countingDriver{MemoryDriver: queue.NewMemoryDriver()}
	engine := worker.NewEngine(worker.Config{MaxWorkers: 1, TaskQueueSize: 4})
	engine.Start()
	defer engine.Shutdown()

	consumer := queue.NewConsumer(driver, queue.NewRouter(), engine, queue.WithPollInterval(5*time.Millisecond))
	consumer.Start()
	consumer.Stop()
	consumer.Stop()
	consumer.Stop()

	if got := atomic.LoadInt32(&driver.closes); got != 1 {
		t.Fatalf("expected driver.Close called exactly once, got %d", got)
	}
}

// delayedOnlyDriver implements Adapter + DelayedEnqueuer but deliberately NOT
// Reenqueuer, forcing the consumer down the EnqueueAt-then-Ack fallback path. It
// records the order of EnqueueAt/Ack calls and can fail Ack on demand.
type delayedOnlyDriver struct {
	mu      sync.Mutex
	pending []*queue.Message
	calls   []string
	ackErr  error
}

func (d *delayedOnlyDriver) Name() string { return "delayed-only" }

func (d *delayedOnlyDriver) Capabilities() queue.Capabilities {
	return queue.Capabilities{DelayedEnqueue: true, Guarantee: queue.AtLeastOnce, MaxBatchSize: 100}
}

func (d *delayedOnlyDriver) Enqueue(_ context.Context, msg *queue.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = append(d.pending, msg)
	return nil
}

func (d *delayedOnlyDriver) Dequeue(_ context.Context, _ queue.DequeueRequest) ([]*queue.Delivery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return nil, nil
	}
	out := make([]*queue.Delivery, len(d.pending))
	for i, m := range d.pending {
		out[i] = &queue.Delivery{Message: m, Token: m.ID}
	}
	d.pending = nil
	return out, nil
}

func (d *delayedOnlyDriver) EnqueueAt(_ context.Context, msg *queue.Message, _ time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "EnqueueAt")
	d.pending = append(d.pending, msg)
	return nil
}

func (d *delayedOnlyDriver) Ack(_ context.Context, _ *queue.Delivery) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "Ack")
	return d.ackErr
}

func (d *delayedOnlyDriver) Nack(_ context.Context, _ *queue.Delivery, _ error) error { return nil }
func (d *delayedOnlyDriver) Close() error                                             { return nil }

func (d *delayedOnlyDriver) snapshotCalls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

// TestConsumer_RetryEnqueuesBeforeAck guards M-Q3: when the adapter can only
// schedule via EnqueueAt, the retry copy must be scheduled before the original
// delivery is Acked. If the Ack then fails (or the process dies), the message is
// merely duplicated (at-least-once) instead of being lost (at-most-once).
func TestConsumer_RetryEnqueuesBeforeAck(t *testing.T) {
	driver := &delayedOnlyDriver{ackErr: errors.New("ack backend unavailable")}

	router := queue.NewRouter()
	router.Register("app.retry", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		return errors.New("transient handler failure")
	})

	engine := worker.NewEngine(worker.Config{MaxWorkers: 1, TaskQueueSize: 8})
	engine.Start()
	defer engine.Shutdown()

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(5*time.Millisecond))
	consumer.Start()
	defer consumer.Stop()

	msg := queue.NewMessage("app.retry", []byte("payload"), queue.WithMaxAttempts(5))
	if err := driver.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for len(driver.snapshotCalls()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	calls := driver.snapshotCalls()
	if len(calls) < 2 {
		t.Fatalf("expected at least EnqueueAt+Ack, got %v", calls)
	}
	if calls[0] != "EnqueueAt" || calls[1] != "Ack" {
		t.Fatalf("expected retry copy scheduled before Ack, got order %v", calls)
	}
}

// subWorkerJoinProbe records, for every Ack, whether the delivery's sub-worker
// fan-out had already finished, and counts how many deliveries were Acked.
type subWorkerJoinProbe struct {
	queue.Adapter
	mu     sync.Mutex
	done   map[string]bool
	acked  int64
	before int64
}

func (p *subWorkerJoinProbe) Ack(ctx context.Context, d *queue.Delivery) error {
	p.mu.Lock()
	if d != nil && d.Message != nil {
		p.acked++
		if !p.done[d.Message.ID] {
			p.before++
		}
	}
	p.mu.Unlock()
	return p.Adapter.Ack(ctx, d)
}

func (p *subWorkerJoinProbe) snapshot() (acked, before int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acked, p.before
}

func (p *subWorkerJoinProbe) markDone(id string) {
	p.mu.Lock()
	p.done[id] = true
	p.mu.Unlock()
}

func (p *subWorkerJoinProbe) isDone(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[id]
}

// TestConsumer_AcksAfterForgottenSubWorkers confirms that a handler which fans
// out into sub-workers but forgets pool.Wait() still has those sub-workers
// joined before the Consumer Acks the delivery.
func TestConsumer_AcksAfterForgottenSubWorkers(t *testing.T) {
	const total = 40

	probe := &subWorkerJoinProbe{Adapter: queue.NewMemoryDriver(), done: make(map[string]bool)}
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 8, TaskQueueSize: 256})
	engine.Start()
	defer engine.Shutdown()

	router.Register("app.fanout", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			probe.mu.Lock()
			probe.done[msg.ID] = true
			probe.mu.Unlock()
			return nil
		})
		return nil // deliberately forgets pool.Wait()
	})

	consumer := queue.NewConsumer(probe, router, engine,
		queue.WithPollInterval(2*time.Millisecond),
		queue.WithBatchSize(16),
	)
	consumer.Start()
	defer consumer.Stop()

	for i := 0; i < total; i++ {
		if err := probe.Enqueue(context.Background(), queue.NewMessage("app.fanout", nil)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		acked, _ := probe.snapshot()
		if acked >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries acked", acked, total)
		}
		time.Sleep(5 * time.Millisecond)
	}

	acked, before := probe.snapshot()
	if before != 0 {
		t.Fatalf("%d/%d deliveries Acked before their sub-workers completed", before, acked)
	}
}

// TestConsumer_GroupedAcksAfterForgottenSubWorkers is the grouped counterpart
// of TestConsumer_AcksAfterForgottenSubWorkers: each ordered group member that
// fans out into sub-workers (forgetting pool.Wait) must still be joined before
// that member is settled.
func TestConsumer_GroupedAcksAfterForgottenSubWorkers(t *testing.T) {
	const (
		perGroup = 10
		total    = 4 * perGroup
	)
	groupIDs := []string{"rg-0", "rg-1", "rg-2", "rg-3"}

	probe := &subWorkerJoinProbe{Adapter: queue.NewMemoryDriver(), done: make(map[string]bool)}
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 8, TaskQueueSize: 256})
	engine.Start()
	defer engine.Shutdown()

	router.Register("app.group-fanout", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error {
			time.Sleep(3 * time.Millisecond)
			probe.markDone(msg.ID)
			return nil
		})
		return nil // deliberately forgets pool.Wait()
	})

	consumer := queue.NewConsumer(probe, router, engine,
		queue.WithPollInterval(2*time.Millisecond),
		queue.WithBatchSize(16),
	)
	consumer.Start()
	defer consumer.Stop()

	for _, groupID := range groupIDs {
		for o := 0; o < perGroup; o++ {
			m := queue.NewMessage("app.group-fanout", nil, queue.WithGroup(groupID, o))
			if err := probe.Enqueue(context.Background(), m); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		acked, _ := probe.snapshot()
		if acked >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d grouped deliveries acked", acked, total)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, before := probe.snapshot(); before != 0 {
		t.Fatalf("%d grouped deliveries Acked before their sub-workers completed", before)
	}
}

// TestConsumer_GroupedMemberFanoutIsolation verifies that a sub-worker failure
// in one group member does not cancel the fan-out of later members: each member
// gets its own sub-worker pool. The failing member (order 0) is routed to the
// DLQ, while orders 1 and 2 must still run their sub-workers and be Acked.
func TestConsumer_GroupedMemberFanoutIsolation(t *testing.T) {
	probe := &subWorkerJoinProbe{Adapter: queue.NewMemoryDriver(), done: make(map[string]bool)}
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 4, TaskQueueSize: 64})
	engine.Start()
	defer engine.Shutdown()

	router.Register("app.group-isolation", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		if msg.GroupOrder == 0 {
			pool.Go(func(ctx context.Context) error { return errors.New("sub-worker failed") })
		} else {
			pool.Go(func(ctx context.Context) error {
				probe.markDone(msg.ID)
				return nil
			})
		}
		return nil // deliberately forgets pool.Wait()
	})
	router.Register(queue.DLQTopic("app.group-isolation"), func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		return nil
	})

	consumer := queue.NewConsumer(probe, router, engine,
		queue.WithPollInterval(2*time.Millisecond),
		queue.WithBatchSize(8),
	)
	consumer.Start()
	defer consumer.Stop()

	var ids []string
	for order := 0; order < 3; order++ {
		m := queue.NewMessage("app.group-isolation", nil, queue.WithGroup("iso-1", order), queue.WithMaxAttempts(1))
		ids = append(ids, m.ID)
		if err := probe.Enqueue(context.Background(), m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for !(probe.isDone(ids[1]) && probe.isDone(ids[2])) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: later members' sub-workers did not run (order0 done=%v order1 done=%v order2 done=%v)",
				probe.isDone(ids[0]), probe.isDone(ids[1]), probe.isDone(ids[2]))
		}
		time.Sleep(5 * time.Millisecond)
	}

	if probe.isDone(ids[0]) {
		t.Fatal("order 0's sub-worker should have failed, but it reported completion")
	}
}

func TestConsumer_GroupNoHeadOfLineBlocking(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 4, TaskQueueSize: 32}).Start()
	defer engine.Shutdown()

	groupAStarted := make(chan struct{})
	unblockGroupA := make(chan struct{})
	independentRan := make(chan struct{})

	router.Register("slow.group", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		select {
		case <-groupAStarted:
		default:
			close(groupAStarted)
		}
		<-unblockGroupA // blocks group A
		return nil
	})
	router.Register("fast.independent", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		select {
		case <-independentRan:
		default:
			close(independentRan)
		}
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(2*time.Millisecond))
	consumer.Start()
	defer consumer.Stop()

	// Enqueue message for group A
	_ = driver.Enqueue(context.Background(), queue.NewMessage("slow.group", nil, queue.WithGroup("A", 1)))

	// Wait until group A starts and is blocked
	select {
	case <-groupAStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for group A to start")
	}

	// While group A is actively blocked, enqueue another message for group A, and an independent message!
	_ = driver.Enqueue(context.Background(), queue.NewMessage("slow.group", nil, queue.WithGroup("A", 2)))
	_ = driver.Enqueue(context.Background(), queue.NewMessage("fast.independent", nil))

	// The independent message MUST be processed without being blocked by group A!
	select {
	case <-independentRan:
		// Success! Poller was not blocked by group A lock
	case <-time.After(2 * time.Second):
		t.Fatal("head-of-line blocking detected: independent message was blocked by group A")
	}

	close(unblockGroupA)
}

func TestConsumer_StopWaitsForInFlightTaskAck(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()

	taskStarted := make(chan struct{})
	finishTask := make(chan struct{})
	var ackSucceeded atomic.Bool

	router.Register("in.flight", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		close(taskStarted)
		<-finishTask
		return nil
	})

	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 8})
	engine.Start()
	defer engine.Shutdown()

	consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(2*time.Millisecond))
	consumer.Start()

	_ = driver.Enqueue(context.Background(), queue.NewMessage("in.flight", []byte("data")))

	// Wait for task to be actively running in worker
	<-taskStarted

	// Call consumer.Stop() concurrently in a separate goroutine
	stopDone := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopDone)
	}()

	// Give Stop() a moment to initiate and wait
	time.Sleep(50 * time.Millisecond)

	// Now release the task to finish and call driver.Ack
	close(finishTask)

	// consumer.Stop() should return cleanly
	select {
	case <-stopDone:
		ackSucceeded.Store(true)
	case <-time.After(3 * time.Second):
		t.Fatal("consumer.Stop timed out waiting for in-flight task")
	}

	if !ackSucceeded.Load() {
		t.Fatal("expected in-flight task to complete before driver closed")
	}
}

