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
