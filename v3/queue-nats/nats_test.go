package queuenats_test

import (
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	queuenats "github.com/rachmanzz/gosuite/v3/queue-nats"
)

var _ queue.Adapter = (*queuenats.NATSAdapter)(nil)

// TestNATSAdapter_DedupWindowConfig guards M-N6: the JetStream duplicate-
// suppression window is configurable and defaults to two minutes.
func TestNATSAdapter_DedupWindowConfig(t *testing.T) {
	if got := queuenats.DefaultConfig().DedupWindow; got != 2*time.Minute {
		t.Fatalf("expected default DedupWindow 2m, got %v", got)
	}

	cfg := queuenats.DefaultConfig()
	queuenats.WithDedupWindow(90 * time.Second)(&cfg)
	if cfg.DedupWindow != 90*time.Second {
		t.Fatalf("expected WithDedupWindow to set 90s, got %v", cfg.DedupWindow)
	}

	queuenats.WithDedupWindow(0)(&cfg)
	if cfg.DedupWindow != 90*time.Second {
		t.Fatalf("expected WithDedupWindow(0) to be ignored, got %v", cfg.DedupWindow)
	}
}

func TestNATSAdapter_Capabilities(t *testing.T) {
	adapter := &queuenats.NATSAdapter{}
	caps := adapter.Capabilities()

	if !caps.Durable {
		t.Fatal("expected NATS JetStream to be durable")
	}
	if !caps.Dedup {
		t.Fatal("expected NATS JetStream to support dedup")
	}
	if adapter.Name() != "nats" {
		t.Fatalf("expected name 'nats', got %s", adapter.Name())
	}
}

func TestNATSAdapter_CapabilityConsistency(t *testing.T) {
	adapter := &queuenats.NATSAdapter{}

	_, replayer := any(adapter).(queue.Replayer)
	_, delayed := any(adapter).(queue.DelayedEnqueuer)
	caps := adapter.Capabilities()

	if caps.Replay != replayer {
		t.Fatalf("Replay capability (%v) disagrees with Replayer implementation (%v)", caps.Replay, replayer)
	}
	if caps.DelayedEnqueue != delayed {
		t.Fatalf("DelayedEnqueue capability (%v) disagrees with DelayedEnqueuer implementation (%v)", caps.DelayedEnqueue, delayed)
	}
}
