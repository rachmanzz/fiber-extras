package worker_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

// TestEngine_StartIsIdempotent guards NEW-C1: a second Start() must not spawn a
// duplicate worker set. With MaxWorkers=2 blocked tasks saturating the workers,
// a third task dispatched after a redundant Start() must stay queued rather than
// being picked up by extra workers.
func TestEngine_StartIsIdempotent(t *testing.T) {
	engine := worker.NewEngine(worker.Config{
		MaxWorkers:      2,
		TaskQueueSize:   16,
		ShutdownTimeout: 2 * time.Second,
		Logger:          worker.NopLogger(),
	}).Start()

	release := make(chan struct{})
	var running int32

	blocker := func() worker.Task {
		return worker.NewTaskFunc("block", func(ctx context.Context, _ worker.SubWorkerPool) error {
			atomic.AddInt32(&running, 1)
			<-release
			return nil
		})
	}

	for i := 0; i < 2; i++ {
		if err := engine.Dispatch(context.Background(), blocker()); err != nil {
			t.Fatalf("dispatch %d failed: %v", i, err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&running) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&running) != 2 {
		t.Fatalf("expected 2 workers busy, got %d", atomic.LoadInt32(&running))
	}

	engine.Start() // redundant call must be a no-op

	var third int32
	if err := engine.Dispatch(context.Background(), worker.NewTaskFunc("third", func(context.Context, worker.SubWorkerPool) error {
		atomic.AddInt32(&third, 1)
		return nil
	})); err != nil {
		t.Fatalf("third dispatch failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(&third); got != 0 {
		t.Fatalf("third task ran while all workers were busy: Start() spawned extra workers")
	}

	close(release)
	if err := engine.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

// TestEngine_ExecuteSyncRecoversPanic guards B11: a panicking task must not
// crash the caller; ExecuteSync mirrors the async runTask recovery and returns
// an error instead.
func TestEngine_ExecuteSyncRecoversPanic(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger()))
	defer func() { _ = engine.Shutdown() }()

	err := engine.ExecuteSync(context.Background(), worker.NewTaskFunc("panic_task", func(context.Context, worker.SubWorkerPool) error {
		panic("boom")
	}))
	if err == nil {
		t.Fatal("expected error from panicking sync task, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected error to mention panic value, got %v", err)
	}
}

// TestGetDispatcher_ConcurrentInitSingleInstance guards B10: concurrent lazy
// initialization must yield exactly one shared engine (no leaked duplicate).
func TestGetDispatcher_ConcurrentInitSingleInstance(t *testing.T) {
	_ = worker.Shutdown() // ensure a clean slate
	defer func() { _ = worker.Shutdown() }()

	const n = 64
	got := make([]worker.Dispatcher, n)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got[i] = worker.GetDispatcher()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 1; i < n; i++ {
		if got[i] != got[0] {
			t.Fatalf("GetDispatcher returned multiple instances concurrently: first=%p, #%d=%p", got[0], i, got[i])
		}
	}
}

type testCustomDispatcher struct {
	dispatchedTasks []worker.Task
	syncTasks       []worker.Task
	mu              sync.Mutex
}

func (m *testCustomDispatcher) Dispatch(ctx context.Context, task worker.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatchedTasks = append(m.dispatchedTasks, task)
	return nil
}

func (m *testCustomDispatcher) ExecuteSync(ctx context.Context, task worker.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncTasks = append(m.syncTasks, task)
	return nil
}

func TestSetGlobalDispatcher_AcceptsCustomMockDispatcher(t *testing.T) {
	_ = worker.Shutdown()
	defer func() { _ = worker.Shutdown() }()

	mock := &testCustomDispatcher{}
	worker.SetGlobalDispatcher(mock)

	if got := worker.GetDispatcher(); got != mock {
		t.Fatalf("GetDispatcher() = %T, want *testCustomDispatcher", got)
	}

	task1 := worker.NewTaskFunc("test.mock.dispatch", func(ctx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})
	if err := worker.Dispatch(context.Background(), task1); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	task2 := worker.NewTaskFunc("test.mock.sync", func(ctx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})
	if err := worker.ExecuteSync(context.Background(), task2); err != nil {
		t.Fatalf("execute sync: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.dispatchedTasks) != 1 || mock.dispatchedTasks[0].Name() != "test.mock.dispatch" {
		t.Fatalf("expected dispatched task in mock, got %v", mock.dispatchedTasks)
	}
	if len(mock.syncTasks) != 1 || mock.syncTasks[0].Name() != "test.mock.sync" {
		t.Fatalf("expected sync task in mock, got %v", mock.syncTasks)
	}
}

