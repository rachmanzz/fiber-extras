package worker

import (
	"context"
	"testing"
	"time"
)

func TestAuditGlobalShutdownLock(t *testing.T) {
	e := InitGlobal(Config{MaxWorkers: 1, ShutdownTimeout: 100 * time.Millisecond, Logger: NopLogger()})
	ready := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	e.Dispatch(context.Background(), NewTaskFunc("nested", func(context.Context, SubWorkerPool) error {
		close(ready)
		<-release
		defer close(done)
		return Dispatch(context.Background(), NewTaskFunc("child", func(context.Context, SubWorkerPool) error { return nil }))
	}))
	<-ready
	result := make(chan error, 1)
	go func() { result <- Shutdown() }()
	for {
		e.closedMu.RLock()
		closed := e.closed
		e.closedMu.RUnlock()
		if closed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("shutdown blocked nested dispatch: %v", err)
	}
	<-done
	Shutdown()
}

func TestAuditConcurrentStartShutdown(t *testing.T) {
	for i := 0; i < 500; i++ {
		e := NewEngine(Config{MaxWorkers: 4, Logger: NopLogger()})
		started := make(chan struct{})
		go func() { e.Start(); close(started) }()
		if err := e.Shutdown(); err != nil {
			t.Fatal(err)
		}
		<-started
		e.wg.Wait()
	}
}

// A dispatcher may be a non-comparable value and may replace the global
// dispatcher from its shutdown callback.
type auditCloser struct{ shutdown func() error }

func (auditCloser) Dispatch(context.Context, Task) error    { return nil }
func (auditCloser) ExecuteSync(context.Context, Task) error { return nil }
func (d auditCloser) Shutdown() error                       { return d.shutdown() }
func TestAuditShutdownPreservesReplacement(t *testing.T) {
	replacement := New(WithLogger(NopLogger()))
	defer replacement.Shutdown()
	SetGlobalDispatcher(auditCloser{shutdown: func() error { SetGlobalDispatcher(replacement); return nil }})
	if err := Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := GetDispatcher(); got != replacement {
		t.Fatalf("shutdown removed replacement: %T", got)
	}
	SetGlobalDispatcher(nil)
}
