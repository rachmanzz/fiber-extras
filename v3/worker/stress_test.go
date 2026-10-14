//go:build stress

package worker_test

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

// TestStress_Engine_ForgottenSubWorkersBlockNextTask confirms the M-W2 fix:
// with a single worker, a task that spawns sub-workers but deliberately never
// calls pool.Wait() must still have those sub-workers joined before the engine
// starts the next queued task. Without the engine-side join the next task would
// begin while the previous task's sub-worker was still sleeping, so the
// "no overlap between consecutive tasks" invariant would be violated.
func TestStress_Engine_ForgottenSubWorkersBlockNextTask(t *testing.T) {
	const tasks = 300

	engine := worker.NewEngine(worker.Config{
		MaxWorkers:      1,
		SubWorkerLimit:  4,
		TaskQueueSize:   tasks + 8,
		ShutdownTimeout: 10 * time.Second,
		Logger:          worker.NopLogger(),
	})
	engine.Start()
	defer func() { _ = engine.Shutdown() }()

	var spawned atomic.Int64
	var finished atomic.Int64
	var violations atomic.Int64

	for i := 0; i < tasks; i++ {
		task := worker.NewTaskFunc("stress.forgotten-subworker", func(ctx context.Context, pool worker.SubWorkerPool) error {
			// MaxWorkers=1 => tasks run strictly one at a time. If the engine
			// joins sub-workers, every sub-worker spawned by a previous task has
			// already finished by the time this task body starts.
			if spawned.Load() != finished.Load() {
				violations.Add(1)
			}
			spawned.Add(1)
			pool.Go(func(ctx context.Context) error {
				time.Sleep(2 * time.Millisecond)
				finished.Add(1)
				return nil
			})
			return nil // deliberately forgets pool.Wait()
		})

		for err := engine.Dispatch(context.Background(), task); err != nil; err = engine.Dispatch(context.Background(), task) {
			time.Sleep(200 * time.Microsecond)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for finished.Load() < tasks {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d sub-workers finished", finished.Load(), tasks)
		}
		time.Sleep(time.Millisecond)
	}

	if v := violations.Load(); v != 0 {
		t.Fatalf("engine started %d tasks while a previous task's forgotten sub-worker was still running", v)
	}
}

// TestStress_Engine_ForgottenSubWorkerErrorPropagates confirms the M-W3 fix:
// under heavy concurrency, ExecuteSync must surface an error returned by a
// sub-worker even when the task itself returns nil and never calls Wait.
func TestStress_Engine_ForgottenSubWorkerErrorPropagates(t *testing.T) {
	const total = 1000

	engine := worker.New(worker.WithLogger(worker.NopLogger()))
	defer func() { _ = engine.Shutdown() }()

	var misses atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			want := fmt.Errorf("sub-worker failure #%d", i)
			task := worker.NewTaskFunc("stress.forgotten-error", func(ctx context.Context, pool worker.SubWorkerPool) error {
				pool.Go(func(ctx context.Context) error { return want })
				return nil // deliberately forgets pool.Wait()
			})

			if err := engine.ExecuteSync(context.Background(), task); !errors.Is(err, want) {
				misses.Add(1)
			}
		}()
	}
	wg.Wait()

	if m := misses.Load(); m != 0 {
		t.Fatalf("%d/%d ExecuteSync calls failed to propagate the forgotten sub-worker error", m, total)
	}
}

// TestStress_Engine_ForgottenSubWorkersNoLeak hammers the engine with fan-out
// tasks that forget to call Wait and asserts that no sub-worker goroutines are
// left dangling once every task has run and the engine has shut down.
func TestStress_Engine_ForgottenSubWorkersNoLeak(t *testing.T) {
	const (
		tasks        = 400
		subPerTask   = 4
		expectedSubs = tasks * subPerTask
	)

	baseline := goroutineSignatures()

	engine := worker.NewEngine(worker.Config{
		MaxWorkers:      8,
		SubWorkerLimit:  subPerTask,
		TaskQueueSize:   1024,
		ShutdownTimeout: 15 * time.Second,
		Logger:          worker.NopLogger(),
	})
	engine.Start()

	var finished atomic.Int64
	for i := 0; i < tasks; i++ {
		task := worker.NewTaskFunc("stress.leak", func(ctx context.Context, pool worker.SubWorkerPool) error {
			for j := 0; j < subPerTask; j++ {
				pool.Go(func(ctx context.Context) error {
					time.Sleep(time.Millisecond)
					finished.Add(1)
					return nil
				})
			}
			return nil // deliberately forgets pool.Wait()
		})
		for err := engine.Dispatch(context.Background(), task); err != nil; err = engine.Dispatch(context.Background(), task) {
			time.Sleep(200 * time.Microsecond)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for finished.Load() < expectedSubs {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d sub-workers finished", finished.Load(), expectedSubs)
		}
		time.Sleep(time.Millisecond)
	}

	if err := engine.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Any goroutine whose stack is not present in the baseline (e.g. a
	// dangling sub-worker or engine worker) must be gone.
	checkNoNewGoroutines(t, baseline, 5*time.Second)
}
