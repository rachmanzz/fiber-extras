package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/worker"
)

type mockRegistrar struct {
	hooks []func() error
}

func (m *mockRegistrar) RegisterBeforeStart(hook func() error) {
	m.hooks = append(m.hooks, hook)
}

type mockHooks struct {
	shutdownHooks []func(error) error
}

func (m *mockHooks) OnPostShutdown(hook func(error) error) {
	m.shutdownHooks = append(m.shutdownHooks, hook)
}

type mockApp struct {
	hooks *mockHooks
}

func (m *mockApp) Hooks() worker.PostShutdownRegistrar {
	return m.hooks
}

func TestRegisterStarterHook(t *testing.T) {
	registrar := &mockRegistrar{}
	app := &mockApp{hooks: &mockHooks{}}

	worker.RegisterStarterHook(registrar, app, worker.Config{
		MaxWorkers:      2,
		TaskQueueSize:   10,
		ShutdownTimeout: 1 * time.Second,
		Logger:          worker.NopLogger(),
	})

	if len(registrar.hooks) != 1 {
		t.Fatalf("expected 1 before-start hook, got %d", len(registrar.hooks))
	}
	if len(app.hooks.shutdownHooks) != 1 {
		t.Fatalf("expected 1 shutdown hook, got %d", len(app.hooks.shutdownHooks))
	}

	// 1. Run PreStart hook
	if err := registrar.hooks[0](); err != nil {
		t.Fatalf("PreStart hook failed: %v", err)
	}

	// 2. Dispatch a task to verify engine is operational
	done := make(chan struct{})
	task := worker.NewTaskFunc("starter:test", func(ctx context.Context, pool worker.SubWorkerPool) error {
		close(done)
		return nil
	})

	if err := worker.Dispatch(context.Background(), task); err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for task")
	}

	// 3. Trigger shutdown hook (as Fiber does on shutdown)
	if err := app.hooks.shutdownHooks[0](nil); err != nil {
		t.Fatalf("shutdown hook failed: %v", err)
	}
}

func TestStarterHook_Closure(t *testing.T) {
	hookFn := worker.StarterHook(worker.Config{
		MaxWorkers: 1,
		Logger:     worker.NopLogger(),
	})

	if err := hookFn(); err != nil {
		t.Fatalf("StarterHook failed: %v", err)
	}

	defer func() {
		_ = worker.Shutdown()
	}()

	disp := worker.GetDispatcher()
	if disp == nil {
		t.Fatal("expected non-nil dispatcher")
	}
}
