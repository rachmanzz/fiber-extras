package queuenats

import (
	"context"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
)

// TestNATSAdapter_CloseStopsRetryTimers guards B16: internal retry timers spawned
// by Reenqueue are tracked and cancelled on Close instead of leaking.
func TestNATSAdapter_CloseStopsRetryTimers(t *testing.T) {
	a := &NATSAdapter{}

	msg := queue.NewMessage("audit.retry", []byte("x"))
	d := &queue.Delivery{Message: msg, Token: "opaque-token"}

	if err := a.Reenqueue(context.Background(), d, msg, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("reenqueue: %v", err)
	}

	a.mu.RLock()
	tracked := len(a.retryTimers)
	a.mu.RUnlock()
	if tracked != 1 {
		t.Fatalf("expected 1 tracked retry timer, got %d", tracked)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	a.mu.RLock()
	remaining := a.retryTimers
	a.mu.RUnlock()
	if remaining != nil {
		t.Fatalf("expected retry timers cleared after Close, got %d tracked", len(remaining))
	}

	if err := a.Reenqueue(context.Background(), d, msg, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected error reenqueueing on a closed adapter")
	}
}
