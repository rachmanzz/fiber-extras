//go:build stress

package queue_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/rachmanzz/gosuite/v3/worker"
)

// goroutineSignatures counts live goroutine stack signatures, ignoring the
// "goroutine N [state]:" header and any goroutine belonging to the testing
// harness. Two snapshots diffed against each other yield a deterministic leak
// oracle: goroutines whose stacks do not exist in the baseline are new.
func goroutineSignatures() map[string]int {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	sigs := make(map[string]int)
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(block, "testing.") {
			continue
		}
		if _, stack, ok := strings.Cut(block, "\n"); ok {
			sigs[stack]++
		}
	}
	return sigs
}

// checkNoNewGoroutines fails the test if goroutines with stacks absent from
// baseline are still alive after the wait period (a leak). It retries until
// the deadline so in-flight teardown never flakes.
//
// Interpretation: a PASS proves only that no goroutine with a new stack
// signature was alive at check time (a signature-based oracle). It is not a
// formal proof of leak-freedom; a leak parked on a stack identical to a
// baseline goroutine would be indistinguishable from it.
func checkNoNewGoroutines(t *testing.T, baseline map[string]int, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for {
		runtime.GC()
		current := goroutineSignatures()
		leaked := map[string]int{}
		for sig, n := range current {
			if baseline[sig] < n {
				leaked[sig] = n - baseline[sig]
			}
		}
		if len(leaked) == 0 {
			return
		}
		if time.Now().After(deadline) {
			sample := ""
			for sig := range leaked {
				sample = sig
				break
			}
			t.Fatalf("goroutine leak: %d new goroutine stack(s) still alive after %v; sample:\n%s", len(leaked), wait, sample)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStress_MemoryDriver_ConcurrentNoLossNoDup hammers the in-memory driver
// with concurrent producers and consumers and asserts driver-level
// exactly-once: every enqueued message is delivered exactly once, none lost.
func TestStress_MemoryDriver_ConcurrentNoLossNoDup(t *testing.T) {
	const (
		producers   = 8
		perProducer = 1000
		consumers   = 8
		total       = producers * perProducer
	)

	driver := queue.NewMemoryDriver()
	defer func() { _ = driver.Close() }()
	ctx := context.Background()

	var (
		mu       sync.Mutex
		expected = make(map[string]struct{}, total)
		seen     = make(map[string]int, total)
	)
	var consumed int64

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 32})
				if err != nil {
					t.Errorf("dequeue error: %v", err)
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					// Ack first; a failed Ack must not advance the progress
					// counter, so "no loss" cannot be concluded from a leaky ack.
					if err := driver.Ack(ctx, d); err != nil {
						t.Errorf("ack error: %v", err)
						return
					}
					atomic.AddInt64(&consumed, 1)
				}
				if len(ds) == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				m := queue.NewMessage("stress", []byte("x"))
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := driver.Enqueue(context.Background(), m); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}

	deadline := time.Now().Add(60 * time.Second)
	for atomic.LoadInt64(&consumed) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: consumed %d/%d", atomic.LoadInt64(&consumed), total)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Drain anything left, then let consumers exit.
	drainDeadline := time.Now().Add(2 * time.Second)
	for {
		ds, _ := driver.Dequeue(context.Background(), queue.DequeueRequest{BatchSize: 64})
		if len(ds) == 0 || time.Now().After(drainDeadline) {
			break
		}
		for _, d := range ds {
			mu.Lock()
			seen[d.Message.ID]++
			mu.Unlock()
			if err := driver.Ack(context.Background(), d); err != nil {
				t.Errorf("drain ack error: %v", err)
				continue
			}
			atomic.AddInt64(&consumed, 1)
		}
	}
	close(stop)
	wg.Wait()

	for id := range expected {
		if seen[id] == 0 {
			t.Fatalf("message %s was enqueued but never delivered", id)
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s delivered %d times (expected exactly once)", id, n)
		}
	}
	if len(seen) != total {
		t.Fatalf("expected %d distinct messages, got %d", total, len(seen))
	}
}

// TestStress_MemoryDriver_EnqueueAtCloseRace schedules many delayed messages
// while concurrently closing the driver; no timer may fire after Close and the
// race detector must stay quiet.
func TestStress_MemoryDriver_EnqueueAtCloseRace(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		driver := queue.NewMemoryDriver()
		ctx := context.Background()

		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = driver.EnqueueAt(ctx, queue.NewMessage("delayed", nil), time.Now().Add(time.Duration(iter%5)*time.Millisecond))
			}()
		}
		time.Sleep(time.Duration(iter%3) * time.Millisecond)
		_ = driver.Close()
		wg.Wait()
		time.Sleep(2 * time.Millisecond)

		if ds, _ := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 100}); len(ds) != 0 {
			t.Fatalf("iter %d: %d deliveries after Close", iter, len(ds))
		}
	}
}

// ackOrderProbe wraps a queue.Adapter and records, for every Ack, whether the
// delivery's sub-worker fan-out had already finished by the time the Ack fired.
type ackOrderProbe struct {
	queue.Adapter
	mu     sync.Mutex
	done   map[string]bool
	before int64
	after  int64
}

func newAckOrderProbe(base queue.Adapter) *ackOrderProbe {
	return &ackOrderProbe{Adapter: base, done: make(map[string]bool)}
}

func (p *ackOrderProbe) markDone(id string) {
	p.mu.Lock()
	p.done[id] = true
	p.mu.Unlock()
}

func (p *ackOrderProbe) Ack(ctx context.Context, d *queue.Delivery) error {
	p.mu.Lock()
	if d != nil && d.Message != nil {
		if p.done[d.Message.ID] {
			p.after++
		} else {
			p.before++
		}
	}
	p.mu.Unlock()
	return p.Adapter.Ack(ctx, d)
}

func (p *ackOrderProbe) counts() (before, after int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.before, p.after
}

// TestStress_Consumer_AcksOnlyAfterSubWorkersComplete confirms the end-to-end
// claim behind the M-W2/M-W3 fix: a handler that fans out into sub-workers but
// forgets pool.Wait() must still have its sub-workers joined before the
// Consumer settles (Acks) the delivery. Without the join the delivery is Acked
// while its sub-worker is still running, opening a crash window that loses the
// message's work (at-least-once violation). With the join every Ack happens
// strictly after the fan-out completed.
func TestStress_Consumer_AcksOnlyAfterSubWorkersComplete(t *testing.T) {
	const (
		total    = 600
		subSleep = 5 * time.Millisecond
	)

	probe := newAckOrderProbe(queue.NewMemoryDriver())
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 16, TaskQueueSize: 2048})
	engine.Start()
	defer func() { _ = engine.Shutdown() }()

	router.Register("stress.ack-order", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error {
			time.Sleep(subSleep)
			probe.markDone(msg.ID)
			return nil
		})
		return nil // deliberately forgets pool.Wait()
	})

	consumer := queue.NewConsumer(probe, router, engine,
		queue.WithPollInterval(1*time.Millisecond),
		queue.WithBatchSize(64),
	)
	consumer.Start()
	defer consumer.Stop()

	for i := 0; i < total; i++ {
		if err := probe.Enqueue(context.Background(), queue.NewMessage("stress.ack-order", nil)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		before, after := probe.counts()
		if before+after >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries acked", before+after, total)
		}
		time.Sleep(2 * time.Millisecond)
	}

	before, after := probe.counts()
	if before != 0 {
		t.Fatalf("%d/%d deliveries were Acked before their sub-workers completed (at-least-once violation)", before, total)
	}
	if after != total {
		t.Fatalf("expected all %d deliveries Acked after fan-out, got %d", total, after)
	}
}

// TestStress_Consumer_GroupedAcksOnlyAfterSubWorkersComplete is the grouped
// counterpart of the independent-delivery Ack-ordering test: members of a group
// are processed in order, and each member's forgotten sub-worker fan-out must
// still be joined before that member is Acked. It also checks that a fresh pool
// per member keeps one member's fan-out from cancelling the next (all members
// must be Acked after their own fan-out, none skipped).
func TestStress_Consumer_GroupedAcksOnlyAfterSubWorkersComplete(t *testing.T) {
	const (
		groups   = 12
		perGroup = 50
		total    = groups * perGroup
		subSleep = 5 * time.Millisecond
	)

	probe := newAckOrderProbe(queue.NewMemoryDriver())
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 16, TaskQueueSize: 2048})
	engine.Start()
	defer func() { _ = engine.Shutdown() }()

	router.Register("stress.group-ack", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		pool.Go(func(ctx context.Context) error {
			time.Sleep(subSleep)
			probe.markDone(msg.ID)
			return nil
		})
		return nil // deliberately forgets pool.Wait()
	})

	consumer := queue.NewConsumer(probe, router, engine,
		queue.WithPollInterval(1*time.Millisecond),
		queue.WithBatchSize(64),
	)
	consumer.Start()
	defer consumer.Stop()

	for g := 0; g < groups; g++ {
		groupID := fmt.Sprintf("stress-grp-%d", g)
		for o := 0; o < perGroup; o++ {
			m := queue.NewMessage("stress.group-ack", nil, queue.WithGroup(groupID, o))
			if err := probe.Enqueue(context.Background(), m); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
		}
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		before, after := probe.counts()
		if before+after >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries acked", before+after, total)
		}
		time.Sleep(2 * time.Millisecond)
	}

	before, after := probe.counts()
	if before != 0 {
		t.Fatalf("%d/%d grouped deliveries were Acked before their sub-workers completed", before, total)
	}
	if after != total {
		t.Fatalf("expected all %d grouped deliveries Acked after fan-out, got %d", total, after)
	}
}

// TestStress_Consumer_RetryStormThenDLQ floods a failing topic and asserts that
// every message eventually reaches the DLQ: no message may be lost, no message
// reaches the DLQ twice, and each DLQ copy carries the failure reason and
// original topic.
func TestStress_Consumer_RetryStormThenDLQ(t *testing.T) {
	const total = 400

	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 16, TaskQueueSize: 512})
	engine.Start()
	defer engine.Shutdown()

	type dlqRecord struct {
		reason        string
		originalTopic string
	}
	var (
		dlqMu  sync.Mutex
		dlqIDs = make(map[string]int, total)
		dlqRec = make(map[string]dlqRecord, total)
	)
	router.Register("stress.fail", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		return errors.New("permanent failure")
	})
	router.Register(queue.DLQTopic("stress.fail"), func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		dlqMu.Lock()
		originalID := msg.Headers["original_message_id"]
		dlqIDs[originalID]++
		dlqRec[originalID] = dlqRecord{
			reason:        msg.Headers["dlq_reason"],
			originalTopic: msg.Headers["original_topic"],
		}
		dlqMu.Unlock()
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine,
		queue.WithPollInterval(1*time.Millisecond),
		queue.WithBatchSize(64),
		queue.WithRetryPolicy(queue.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 1.0}),
	)
	consumer.Start()
	defer consumer.Stop()

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("stress.fail", []byte("payload"), queue.WithMaxAttempts(3))
		expected[m.ID] = struct{}{}
		if err := driver.Enqueue(context.Background(), m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		dlqMu.Lock()
		n := len(dlqIDs)
		dlqMu.Unlock()
		if n >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d distinct messages reached DLQ", n, total)
		}
		time.Sleep(5 * time.Millisecond)
	}

	dlqMu.Lock()
	defer dlqMu.Unlock()
	for id := range expected {
		switch dlqIDs[id] {
		case 0:
			t.Fatalf("message %s never reached the DLQ (lost)", id)
		case 1:
		default:
			t.Fatalf("message %s reached the DLQ %d times (want exactly once)", id, dlqIDs[id])
		}
		rec := dlqRec[id]
		if rec.reason == "" {
			t.Fatalf("message %s reached the DLQ without a dlq_reason header", id)
		}
		if rec.originalTopic != "stress.fail" {
			t.Fatalf("message %s has original_topic=%q, want %q", id, rec.originalTopic, "stress.fail")
		}
	}
	if len(dlqIDs) != total {
		t.Fatalf("expected %d distinct DLQ messages, got %d", total, len(dlqIDs))
	}
}

// TestStress_Consumer_BackpressureNoSpin drives a saturated engine (tiny task
// queue) with a large backlog and verifies the consumer neither deadlocks nor
// loses messages: every distinct message is eventually handled exactly once.
func TestStress_Consumer_BackpressureNoSpin(t *testing.T) {
	const total = 300

	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 1})
	engine.Start()
	defer engine.Shutdown()

	var (
		doneMu  sync.Mutex
		handled = make(map[string]int, total)
	)
	router.Register("stress.backpressure", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		doneMu.Lock()
		handled[msg.ID]++
		doneMu.Unlock()
		return nil
	})

	consumer := queue.NewConsumer(driver, router, engine,
		queue.WithPollInterval(1*time.Millisecond),
		queue.WithBatchSize(16),
	)
	consumer.Start()
	defer consumer.Stop()

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("stress.backpressure", nil)
		expected[m.ID] = struct{}{}
		if err := driver.Enqueue(context.Background(), m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		doneMu.Lock()
		n := len(handled)
		doneMu.Unlock()
		if n >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d distinct messages handled", n, total)
		}
		time.Sleep(5 * time.Millisecond)
	}

	doneMu.Lock()
	defer doneMu.Unlock()
	for id := range expected {
		switch handled[id] {
		case 0:
			t.Fatalf("message %s was enqueued but never handled", id)
		case 1:
		default:
			t.Fatalf("message %s handled %d times (want exactly once)", id, handled[id])
		}
	}
	if len(handled) != total {
		t.Fatalf("expected %d distinct messages handled, got %d", total, len(handled))
	}
}

// TestStress_Engine_SaturationAndSubWorkers dispatches far more tasks than
// workers exist, each spawning sub-tasks, and asserts the engine drains them
// all without deadlock and without leaking goroutines.
func TestStress_Engine_SaturationAndSubWorkers(t *testing.T) {
	const tasks = 500

	baseline := goroutineSignatures()
	engine := worker.NewEngine(worker.Config{MaxWorkers: 8, TaskQueueSize: 64, SubWorkerLimit: 4})
	engine.Start()

	doneByTask := make([]int32, tasks)
	for i := 0; i < tasks; i++ {
		idx := i
		task := worker.NewTaskFunc("stress.sub", func(ctx context.Context, pool worker.SubWorkerPool) error {
			for j := 0; j < 4; j++ {
				pool.Go(func(ctx context.Context) error { return nil })
			}
			_ = pool.Wait()
			atomic.StoreInt32(&doneByTask[idx], 1)
			return nil
		})
		if err := engine.Dispatch(context.Background(), task); err != nil {
			time.Sleep(time.Millisecond)
			_ = engine.ExecuteSync(context.Background(), task)
		}
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		var done int64
		for i := range doneByTask {
			done += int64(atomic.LoadInt32(&doneByTask[i]))
		}
		if done >= tasks {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %d/%d tasks done", done, tasks)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := range doneByTask {
		if n := atomic.LoadInt32(&doneByTask[i]); n != 1 {
			t.Fatalf("task %d ran %d times (want exactly once)", i, n)
		}
	}

	engine.Shutdown()
	checkNoNewGoroutines(t, baseline, 5*time.Second)
}

// TestStress_Consumer_StartStopChurn starts and stops consumers repeatedly
// while work flows. For every cycle it asserts that each enqueued message is
// actually handled exactly once (not merely that no panic occurred), then it
// verifies the whole churn does not leak goroutines.
func TestStress_Consumer_StartStopChurn(t *testing.T) {
	baseline := goroutineSignatures()

	const (
		cycles        = 30
		perCycle      = 20
		handleTimeout = 10 * time.Second
	)
	for i := 0; i < cycles; i++ {
		driver := queue.NewMemoryDriver()
		router := queue.NewRouter()
		engine := worker.NewEngine(worker.Config{MaxWorkers: 4, TaskQueueSize: 32})
		engine.Start()

		var (
			mu      sync.Mutex
			handled = make(map[string]int, perCycle)
		)
		router.Register("stress.churn", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
			mu.Lock()
			handled[msg.ID]++
			mu.Unlock()
			return nil
		})
		consumer := queue.NewConsumer(driver, router, engine, queue.WithPollInterval(time.Millisecond))
		consumer.Start()

		expected := make(map[string]struct{}, perCycle)
		for j := 0; j < perCycle; j++ {
			m := queue.NewMessage("stress.churn", nil)
			if err := driver.Enqueue(context.Background(), m); err != nil {
				consumer.Stop()
				engine.Shutdown()
				t.Fatalf("cycle %d enqueue: %v", i, err)
			}
			// Only a successfully enqueued ID counts as expected, so a lost
			// message can no longer hide behind an unexpected one.
			expected[m.ID] = struct{}{}
		}

		deadline := time.Now().Add(handleTimeout)
		for {
			mu.Lock()
			n := len(handled)
			mu.Unlock()
			if n >= perCycle {
				break
			}
			if time.Now().After(deadline) {
				consumer.Stop()
				engine.Shutdown()
				t.Fatalf("cycle %d: only %d/%d messages handled", i, n, perCycle)
			}
			time.Sleep(time.Millisecond)
		}

		mu.Lock()
		bad := ""
		for id := range expected {
			if handled[id] == 0 {
				bad = fmt.Sprintf("message %s was enqueued but never handled", id)
				break
			}
			if handled[id] != 1 {
				bad = fmt.Sprintf("message %s handled %d times (want exactly once)", id, handled[id])
				break
			}
		}
		if bad == "" {
			for id := range handled {
				if _, ok := expected[id]; !ok {
					bad = fmt.Sprintf("unexpected message %s was handled", id)
					break
				}
			}
		}
		if bad == "" && len(handled) != perCycle {
			bad = fmt.Sprintf("handled %d distinct messages, want %d", len(handled), perCycle)
		}
		mu.Unlock()
		if bad != "" {
			consumer.Stop()
			engine.Shutdown()
			t.Fatalf("cycle %d: %s", i, bad)
		}

		consumer.Stop()
		engine.Shutdown()
	}

	checkNoNewGoroutines(t, baseline, 5*time.Second)
}
