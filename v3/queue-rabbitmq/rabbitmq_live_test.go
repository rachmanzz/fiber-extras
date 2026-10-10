package queuerabbitmq_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuerabbitmq "github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq"
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

// TestRabbitMQAdapter_Live exercises the full delivery loop against a real
// RabbitMQ broker: Enqueue -> priority Dequeue (x-max-priority) -> Ack ->
// Nack (requeue/redelivery).
func TestRabbitMQAdapter_Live(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL not set; skipping live RabbitMQ integration test (run scripts/test-all-brokers.sh to enable)")
	}

	queueName := "fiber_live_" + randSuffix(t)
	adapter, err := queuerabbitmq.New(url, queuerabbitmq.WithQueueName(queueName))
	if err != nil {
		t.Fatalf("queue-rabbitmq: cannot connect to live broker at %s: %v", url, err)
	}
	defer adapter.Close()

	// Delete the queue so nothing lingers on long-lived brokers.
	t.Cleanup(func() {
		if conn, derr := amqp.Dial(url); derr == nil {
			if ch, cerr := conn.Channel(); cerr == nil {
				_, _ = ch.QueueDelete(queueName, false, false, false)
				_ = ch.Close()
			}
			_ = conn.Close()
		}
	})

	ctx := context.Background()

	low := newLiveMsg("email.send", queue.PriorityLow)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so ordering is only attributable to the broker.
	for _, m := range []*queue.Message{low, high, crit} {
		if err := adapter.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-rabbitmq: enqueue %s failed: %v", m.ID, err)
		}
	}

	// RabbitMQ publishes are asynchronous; give the broker a moment to
	// enqueue all messages before the first priority-ordered pull.
	time.Sleep(250 * time.Millisecond)

	deliveries, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 3})
	if err != nil {
		t.Fatalf("queue-rabbitmq: dequeue failed: %v", err)
	}
	if len(deliveries) != 3 {
		t.Fatalf("queue-rabbitmq: expected 3 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, low.ID})

	// Ack the critical message: it must be permanently removed.
	if err := adapter.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-rabbitmq: ack failed: %v", err)
	}

	// Nack the high message: it must be requeued and redelivered.
	if err := adapter.Nack(ctx, deliveries[1], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-rabbitmq: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, adapter, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != high.ID {
		t.Fatalf("queue-rabbitmq: expected redelivery of nack'd message %s, got %d delivery(ies)", high.ID, len(redelivered))
	}
	if err := adapter.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-rabbitmq: ack after redelivery failed: %v", err)
	}
}
