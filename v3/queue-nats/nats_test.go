package queuenats_test

import (
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuenats "github.com/rachmanzz/fiber-extras/v3/queue-nats"
)

var _ queue.Adapter = (*queuenats.NATSAdapter)(nil)

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
