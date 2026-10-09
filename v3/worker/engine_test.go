package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/worker"
)

func TestEngine_Parallel_And_SubWorkers(t *testing.T) {
	cfg := worker.Config{
		MaxWorkers:      4,
		SubWorkerLimit:  3,
		TaskQueueSize:   50,
		ShutdownTimeout: 5 * time.Second,
		Logger:          worker.NopLogger(),
	}

	engine := worker.NewEngine(cfg).Start()
	defer func() {
		_ = engine.Shutdown()
	}()

	var mainTaskRunCount int32
	var subTasksCompleted int32

	doneChan := make(chan struct{})

	// Task that fans out into 6 sub-workers
	task := worker.NewTaskFunc("test:fan_out_task", func(ctx context.Context, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&mainTaskRunCount, 1)

		for i := 0; i < 6; i++ {
			pool.Go(func(subCtx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt32(&subTasksCompleted, 1)
				return nil
			})
		}

		err := pool.Wait()
		close(doneChan)
		return err
	})

	err := engine.Dispatch(context.Background(), task)
	if err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	select {
	case <-doneChan:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for background task and sub-workers to complete")
	}

	if atomic.LoadInt32(&mainTaskRunCount) != 1 {
		t.Errorf("expected mainTaskRunCount 1, got %d", atomic.LoadInt32(&mainTaskRunCount))
	}
	if atomic.LoadInt32(&subTasksCompleted) != 6 {
		t.Errorf("expected subTasksCompleted 6, got %d", atomic.LoadInt32(&subTasksCompleted))
	}
}

func TestEngine_SubWorker_Error_Propagation(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger()))
	defer func() {
		_ = engine.Shutdown()
	}()

	expectedErr := errors.New("sub-worker deliberate failure")

	task := worker.NewTaskFunc("test:error_task", func(ctx context.Context, pool worker.SubWorkerPool) error {
		pool.Go(func(subCtx context.Context) error {
			return expectedErr
		})
		pool.Go(func(subCtx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		})
		return pool.Wait()
	})

	err := engine.ExecuteSync(context.Background(), task)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestEngine_PanicRecovery_SubWorker(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	defer func() {
		_ = engine.Shutdown()
	}()

	done := make(chan struct{})

	// Task with panic inside sub-worker should not crash the program
	task := worker.NewTaskFunc("test:panic_subworker", func(ctx context.Context, pool worker.SubWorkerPool) error {
		defer close(done)
		pool.Go(func(subCtx context.Context) error {
			panic("intentional test panic")
		})
		return pool.Wait()
	})

	err := engine.Dispatch(context.Background(), task)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for panic task to settle")
	}
}

func TestEngine_PanicRecovery_MainTask(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	defer func() {
		_ = engine.Shutdown()
	}()

	// Dispatched task that panics directly in main execution
	task := worker.NewTaskFunc("test:panic_main", func(ctx context.Context, pool worker.SubWorkerPool) error {
		panic("panic in main task execute")
	})

	err := engine.Dispatch(context.Background(), task)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	// Subsequent task should still execute normally without dead workers
	var executed atomic.Bool
	done := make(chan struct{})
	normalTask := worker.NewTaskFunc("test:normal_after_panic", func(ctx context.Context, pool worker.SubWorkerPool) error {
		executed.Store(true)
		close(done)
		return nil
	})

	err = engine.Dispatch(context.Background(), normalTask)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-done:
		if !executed.Load() {
			t.Fatal("expected normalTask to execute successfully after panic")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for normal task")
	}
}

func TestEngine_QueueCapacity_ErrQueueFull(t *testing.T) {
	// Create engine with queue size 1 and unstarted (workers don't consume)
	cfg := worker.Config{
		MaxWorkers:      1,
		TaskQueueSize:   1,
		ShutdownTimeout: 1 * time.Second,
		Logger:          worker.NopLogger(),
	}
	engine := worker.NewEngine(cfg) // unstarted
	defer func() {
		_ = engine.Shutdown()
	}()

	task := worker.NewTaskFunc("t1", func(ctx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})

	// 1st dispatch should succeed
	err := engine.Dispatch(context.Background(), task)
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}

	// 2nd dispatch should return ErrQueueFull because buffer is 1
	err = engine.Dispatch(context.Background(), task)
	if !errors.Is(err, worker.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestEngine_NilTask(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger()))
	defer func() { _ = engine.Shutdown() }()

	if err := engine.Dispatch(context.Background(), nil); !errors.Is(err, worker.ErrNilTask) {
		t.Fatalf("expected ErrNilTask, got %v", err)
	}

	if err := engine.ExecuteSync(context.Background(), nil); !errors.Is(err, worker.ErrNilTask) {
		t.Fatalf("expected ErrNilTask, got %v", err)
	}
}

func TestEngine_DispatchAfterShutdown(t *testing.T) {
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	if err := engine.Shutdown(); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	task := worker.NewTaskFunc("t", func(ctx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})

	err := engine.Dispatch(context.Background(), task)
	if !errors.Is(err, worker.ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got %v", err)
	}
}

func TestEngine_Shutdown_Timeout(t *testing.T) {
	cfg := worker.Config{
		MaxWorkers:      1,
		TaskQueueSize:   5,
		ShutdownTimeout: 50 * time.Millisecond,
		Logger:          worker.NopLogger(),
	}
	engine := worker.NewEngine(cfg).Start()

	// Task that hangs for 500ms
	task := worker.NewTaskFunc("t:hang", func(ctx context.Context, pool worker.SubWorkerPool) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	})

	_ = engine.Dispatch(context.Background(), task)
	time.Sleep(10 * time.Millisecond) // ensure task is picked up

	err := engine.Shutdown()
	if err == nil {
		t.Fatal("expected shutdown timeout error, got nil")
	}
}

func TestGlobalDispatcher(t *testing.T) {
	// Initialize global
	engine := worker.InitGlobal(worker.Config{
		MaxWorkers:      2,
		TaskQueueSize:   10,
		ShutdownTimeout: 1 * time.Second,
		Logger:          worker.NopLogger(),
	})
	if engine == nil {
		t.Fatal("expected non-nil engine from InitGlobal")
	}

	disp := worker.GetDispatcher()
	if disp == nil {
		t.Fatal("expected non-nil dispatcher")
	}

	var called atomic.Bool
	done := make(chan struct{})
	task := worker.NewTaskFunc("global:task", func(ctx context.Context, pool worker.SubWorkerPool) error {
		called.Store(true)
		close(done)
		return nil
	})

	// Use package-level Dispatch
	if err := worker.Dispatch(context.Background(), task); err != nil {
		t.Fatalf("worker.Dispatch failed: %v", err)
	}

	select {
	case <-done:
		if !called.Load() {
			t.Fatal("task was not executed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for global task")
	}

	// Use package-level ExecuteSync
	var syncCalled bool
	syncTask := worker.NewTaskFunc("sync:task", func(ctx context.Context, pool worker.SubWorkerPool) error {
		syncCalled = true
		return nil
	})
	if err := worker.ExecuteSync(context.Background(), syncTask); err != nil {
		t.Fatalf("worker.ExecuteSync failed: %v", err)
	}
	if !syncCalled {
		t.Fatal("syncTask was not executed")
	}

	if err := worker.Shutdown(); err != nil {
		t.Fatalf("worker.Shutdown failed: %v", err)
	}
}

func TestFunctionalOptions(t *testing.T) {
	customLog := worker.NopLogger()
	engine := worker.New(
		worker.WithMaxWorkers(8),
		worker.WithSubWorkerLimit(4),
		worker.WithTaskQueueSize(500),
		worker.WithShutdownTimeout(10*time.Second),
		worker.WithLogger(customLog),
	)
	defer func() { _ = engine.Shutdown() }()

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestCustomLogger(t *testing.T) {
	var logged []string
	logger := worker.NewCustomLogger(
		func(msg string, args ...any) { logged = append(logged, "debug:"+msg) },
		func(msg string, args ...any) { logged = append(logged, "info:"+msg) },
		func(msg string, args ...any) { logged = append(logged, "warn:"+msg) },
		func(msg string, args ...any) { logged = append(logged, "error:"+msg) },
	)

	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Error("e")

	if len(logged) != 4 {
		t.Fatalf("expected 4 logged messages, got %d", len(logged))
	}
}
