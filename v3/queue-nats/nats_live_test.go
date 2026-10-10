package queuenats_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuenats "github.com/rachmanzz/fiber-extras/v3/queue-nats"
)

func randSuffix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("failed to generate random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func newLiveMsg(topic string, prio queue.Priority) *queue.Message {
	return queue.NewMessage(topic, []byte(`{"live":true}`), queue.WithPriority(prio))
}

func assertOrder(t *testing.T, deliveries []*queue.Delivery, wantIDs []string) {
	t.Helper()
	for i, want := range wantIDs {
		if deliveries[i].Message.ID != want {
			t.Fatalf("delivery %d: expected message %s, got %s", i, want, deliveries[i].Message.ID)
		}
	}
}

// dequeueUntil polls the adapter until at least want deliveries are available
// or the timeout elapses, returning whatever was fetched on the last call.
func dequeueUntil(t *testing.T, ctx context.Context, adapter queue.Adapter, timeout time.Duration, want int) []*queue.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		out []*queue.Delivery
		err error
	)
	for {
		out, err = adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
		if err != nil {
			t.Fatalf("dequeue failed: %v", err)
		}
		if len(out) >= want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newNATSTestConfig(t *testing.T) queuenats.Config {
	t.Helper()
	cfg := queuenats.DefaultConfig()
	cfg.Stream = "FIBER_TEST_" + randSuffix(t)
	cfg.Storage = "memory"
	return cfg
}

// startNATSForTest connects to the external NATS broker when TEST_NATS_URL is set,
// otherwise it boots an in-process embedded NATS server with JetStream enabled so
// the live test suite runs fully self-contained with zero external dependencies.
func startNATSForTest(t *testing.T) (*nats.Conn, queuenats.Config) {
	t.Helper()

	if url := os.Getenv("TEST_NATS_URL"); url != "" {
		nc, err := nats.Connect(url, nats.Timeout(10*time.Second))
		if err != nil {
			t.Fatalf("queue-nats: failed to connect to external NATS at %s: %v", url, err)
		}
		return nc, newNATSTestConfig(t)
	}

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
	})
	if err != nil {
		t.Fatalf("queue-nats: failed to create embedded NATS server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("queue-nats: embedded NATS server did not become ready in time")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("queue-nats: failed to connect to embedded NATS server: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Shutdown()
	})
	return nc, newNATSTestConfig(t)
}

func TestNATSAdapter_Live_FullLoop(t *testing.T) {
	nc, cfg := startNATSForTest(t)

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Delete the stream so nothing lingers on long-lived brokers.
	t.Cleanup(func() {
		if js, err := nc.JetStream(); err == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()

	low := newLiveMsg("email.send", queue.PriorityLow)
	def := newLiveMsg("email.send", queue.PriorityDefault)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so any ordering advantage can only come from the broker.
	for _, m := range []*queue.Message{low, def, high, crit} {
		if err := adapter.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-nats: enqueue %s failed: %v", m.ID, err)
		}
	}

	deliveries, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-nats: dequeue failed: %v", err)
	}
	if len(deliveries) != 4 {
		t.Fatalf("queue-nats: expected 4 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, def.ID, low.ID})

	// Ack the critical message: it must never be redelivered.
	if err := adapter.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-nats: ack failed: %v", err)
	}

	// Nack the default message: it must be redelivered on the next Dequeue.
	if err := adapter.Nack(ctx, deliveries[2], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-nats: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, adapter, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != def.ID {
		t.Fatalf("queue-nats: expected redelivery of nack'd message %s, got %d delivery(ies)", def.ID, len(redelivered))
	}
	if err := adapter.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-nats: ack after redelivery failed: %v", err)
	}
}

func TestNATSAdapter_Live_Dedup(t *testing.T) {
	nc, cfg := startNATSForTest(t)

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: failed to create adapter: %v", err)
	}
	defer adapter.Close()

	t.Cleanup(func() {
		if js, err := nc.JetStream(); err == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()

	msg := newLiveMsg("dedup.check", queue.PriorityDefault)
	for i := 0; i < 2; i++ {
		if err := adapter.Enqueue(ctx, msg); err != nil {
			t.Fatalf("queue-nats: enqueue attempt %d failed: %v", i+1, err)
		}
	}

	deliveries := dequeueUntil(t, ctx, adapter, 2*time.Second, 1)
	if len(deliveries) != 1 {
		t.Fatalf("queue-nats: dedup window failed: expected exactly 1 delivery, got %d", len(deliveries))
	}
	if deliveries[0].Message.ID != msg.ID {
		t.Fatalf("queue-nats: expected message %s, got %s", msg.ID, deliveries[0].Message.ID)
	}
	if err := adapter.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-nats: ack failed: %v", err)
	}
}
