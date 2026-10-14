package cron

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestRunner_HooksAndSignalShutdown(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	if _, err := s.AddFunc("0 0 1 * *", func() {}); err != nil {
		t.Fatalf("addfunc: %v", err)
	}

	var before, after atomic.Int64
	r := NewRunner(s,
		WithSignals(syscall.SIGUSR1),
		WithShutdownTimeout(2*time.Second),
	)
	started := make(chan struct{})
	r.RegisterBeforeStart(func() error {
		before.Add(1)
		close(started) // Run() is past the hooks; Start() follows immediately.
		return nil
	})
	r.RegisterPostShutdown(func() error {
		after.Add(1)
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- r.Run() }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not reach the before-start hook in time")
	}
	// Give Start() a moment, then signal termination. An early signal is
	// buffered anyway because signal.Notify is installed before Start().
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not terminate in time")
	}

	if before.Load() != 1 {
		t.Fatalf("before-start hooks ran %d times, want 1", before.Load())
	}
	if after.Load() != 1 {
		t.Fatalf("post-shutdown hooks ran %d times, want 1", after.Load())
	}
}

func TestRunner_FailingBeforeStartHookAbortsAndStillCleansUp(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	want := errors.New("no database")
	r := NewRunner(s, WithSignals(syscall.SIGUSR1))
	r.RegisterBeforeStart(func() error { return want })
	r.RegisterBeforeStart(func() error {
		t.Error("hook after the failing one must not run")
		return nil
	})
	cleaned := false
	r.RegisterPostShutdown(func() error {
		cleaned = true
		return nil
	})

	err := r.Run()
	if !errors.Is(err, want) {
		t.Fatalf("run error = %v, want the before-start failure", err)
	}
	if !cleaned {
		t.Fatal("post-shutdown hooks must run even after startup failure")
	}
}

// TestRunner_ShutdownTimeoutSurfacesError locks the FC-17 contract: when the
// graceful stop exceeds WithShutdownTimeout, Run must return the stop error
// (context.DeadlineExceeded) instead of masking an incomplete shutdown as
// success -- and post-shutdown hooks must still run.
func TestRunner_ShutdownTimeoutSurfacesError(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	var once sync.Once
	started := make(chan struct{})
	if _, err := s.AddFunc("*/1 * * * * *", func() {
		once.Do(func() { close(started) })
		time.Sleep(4 * time.Second) // outlives the shutdown deadline below
	}); err != nil {
		t.Fatalf("addfunc: %v", err)
	}

	var cleaned atomic.Int64
	r := NewRunner(s,
		WithSignals(syscall.SIGUSR1),
		WithShutdownTimeout(time.Second),
	)
	r.RegisterPostShutdown(func() error {
		cleaned.Add(1)
		return nil
	})

	done := make(chan error, 1)
	go func() { done <- r.Run() }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job never became in-flight")
	}
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("signal: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not terminate in time")
	}
	if n := cleaned.Load(); n != 1 {
		t.Fatalf("post-shutdown hooks ran %d times, want 1 even on timeout", n)
	}
}

func TestRunner_ContextCancellation(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	r := NewRunner(s, WithShutdownTimeout(2*time.Second))

	var cleaned atomic.Int64
	r.RegisterPostShutdown(func() error {
		cleaned.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on context cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not terminate upon context cancellation")
	}

	if cleaned.Load() != 1 {
		t.Fatalf("post-shutdown hooks ran %d times, want 1", cleaned.Load())
	}
}

func TestRunner_ProgrammaticStop(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	r := NewRunner(s, WithShutdownTimeout(2*time.Second))

	var cleaned atomic.Int64
	r.RegisterPostShutdown(func() error {
		cleaned.Add(1)
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- r.Run()
	}()

	time.Sleep(50 * time.Millisecond)
	r.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on programmatic stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not terminate upon programmatic Stop()")
	}

	if cleaned.Load() != 1 {
		t.Fatalf("post-shutdown hooks ran %d times, want 1", cleaned.Load())
	}
}

func TestRunner_EmptySignals(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	// Configure with empty signals list.
	// Previously this deadlocked on <-sigCh. Now it safely shuts down via Stop().
	r := NewRunner(s, WithSignals(), WithShutdownTimeout(2*time.Second))

	done := make(chan error, 1)
	go func() {
		done <- r.Run()
	}()

	time.Sleep(50 * time.Millisecond)
	r.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on empty signals stop: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner deadlocked when configured with empty signals")
	}
}

