package queue_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	"github.com/rachmanzz/fiber-extras/v3/worker"
)

func TestRunner_LifecycleHooksAndContextCancellation(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	workerEngine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 10})
	workerEngine.Start()
	defer workerEngine.Shutdown()

	consumer := queue.NewConsumer(driver, router, workerEngine, queue.WithPollInterval(10*time.Millisecond))
	runner := queue.NewRunner(consumer)

	var beforeStartCalled int32
	var postShutdownCalled int32

	runner.RegisterBeforeStart(func() error {
		atomic.AddInt32(&beforeStartCalled, 1)
		return nil
	})

	runner.RegisterPostShutdown(func() error {
		atomic.AddInt32(&postShutdownCalled, 1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	// Let the runner start
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&beforeStartCalled) != 1 {
		t.Fatalf("expected beforeStart hook to be called once, got %d", beforeStartCalled)
	}

	// Trigger shutdown via context
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected runner error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner timed out waiting for shutdown")
	}

	if atomic.LoadInt32(&postShutdownCalled) != 1 {
		t.Fatalf("expected postShutdown hook to be called once, got %d", postShutdownCalled)
	}
}

func TestRunner_ProgrammaticStop(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	workerEngine := worker.NewEngine(worker.Config{MaxWorkers: 2, TaskQueueSize: 10})
	workerEngine.Start()
	defer workerEngine.Shutdown()

	consumer := queue.NewConsumer(driver, router, workerEngine, queue.WithPollInterval(10*time.Millisecond))
	runner := queue.NewRunner(consumer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run()
	}()

	time.Sleep(50 * time.Millisecond)
	runner.Stop()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected runner error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner timed out waiting for stop")
	}
}

func TestRunner_BeforeStartHookError(t *testing.T) {
	driver := queue.NewMemoryDriver()
	router := queue.NewRouter()
	consumer := queue.NewConsumer(driver, router, nil)

	runner := queue.NewRunner(consumer)
	expectedErr := errors.New("database connection failed")

	runner.RegisterBeforeStart(func() error {
		return expectedErr
	})

	err := runner.Run()
	if err == nil {
		t.Fatal("expected runner to return error when BeforeStart hook fails")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error wrapping %v, got %v", expectedErr, err)
	}
}

func TestRunner_FallbackGlobalConsumer(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	memDriver := queue.NewMemoryDriver()
	queue.SetDriver(memDriver)

	var processed int32
	queue.RegisterHandler("email.welcome", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	runner := queue.NewRunner() // No consumer passed, should fallback to global manager

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run()
	}()

	time.Sleep(50 * time.Millisecond)

	// Dispatch message
	err := queue.Dispatch(context.Background(), "email.welcome", "hello")
	if err != nil {
		t.Fatalf("failed to dispatch message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	runner.Stop()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected runner error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner timed out waiting for stop")
	}

	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("expected message to be processed by global consumer, got %d", processed)
	}
}
