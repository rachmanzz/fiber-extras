package queuerabbitmq_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"sync/atomic"
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

// TestRabbitMQAdapter_Live_UndecodablePayloadSurfacesPoison verifies that a
// payload that cannot be decoded is not silently rejected: the adapter publishes
// a poison copy (queue.PoisonTopic) before acking the corrupt original so the
// consumer can route it to the DLQ.
func TestRabbitMQAdapter_Live_UndecodablePayloadSurfacesPoison(t *testing.T) {
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

	t.Cleanup(func() {
		if conn, derr := amqp.Dial(url); derr == nil {
			if ch, cerr := conn.Channel(); cerr == nil {
				_, _ = ch.QueueDelete(queueName, false, false, false)
				_ = ch.Close()
			}
			_ = conn.Close()
		}
	})

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("queue-rabbitmq: failed to dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("queue-rabbitmq: failed to open channel: %v", err)
	}
	defer ch.Close()

	ctx := context.Background()
	if err := ch.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("this is not json"),
	}); err != nil {
		t.Fatalf("queue-rabbitmq: failed to publish corrupt payload: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	var poison *queue.Delivery
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && poison == nil {
		batch, dErr := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
		if dErr != nil {
			t.Fatalf("queue-rabbitmq: dequeue failed: %v", dErr)
		}
		for _, d := range batch {
			if d.Message != nil && d.Message.Topic == queue.PoisonTopic {
				poison = d
				break
			}
			_ = adapter.Ack(ctx, d)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if poison == nil {
		t.Fatal("queue-rabbitmq: corrupt payload was not surfaced as a poison message")
	}
	if poison.Message.Headers["decode_error"] == "" {
		t.Fatal("queue-rabbitmq: expected decode_error header on poison message")
	}
	if poison.Message.Headers["decode_adapter"] != adapter.Name() {
		t.Fatalf("queue-rabbitmq: expected decode_adapter=%q, got %q", adapter.Name(), poison.Message.Headers["decode_adapter"])
	}
	if err := adapter.Ack(ctx, poison); err != nil {
		t.Fatalf("queue-rabbitmq: ack of poison failed: %v", err)
	}
}

// TestRabbitMQAdapter_Live_ConcurrentAccessNoLoss guards the S-1 fix: a single
// amqp channel is not safe for concurrent use, so the adapter must serialize
// all channel I/O. Concurrent producers plus concurrent Dequeue/Ack (the shape
// the queue.Consumer produces) previously desynchronized the AMQP stream
// (403/504/505) and dropped messages.
func TestRabbitMQAdapter_Live_ConcurrentAccessNoLoss(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("TEST_RABBITMQ_URL not set; skipping live RabbitMQ integration test (run scripts/test-all-brokers.sh to enable)")
	}

	const (
		producers   = 4
		perProducer = 150
		consumers   = 4
		total       = producers * perProducer
	)

	queueName := "fiber_live_conc_" + randSuffix(t)
	adapter, err := queuerabbitmq.New(url, queuerabbitmq.WithQueueName(queueName), queuerabbitmq.WithPrefetch(500))
	if err != nil {
		t.Fatalf("queue-rabbitmq: cannot connect to live broker at %s: %v", url, err)
	}
	defer adapter.Close()

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
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				if err := adapter.Enqueue(ctx, queue.NewMessage("rmq.conc", []byte("x"))); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}

	var (
		mu   sync.Mutex
		seen = make(map[string]int, total)
		got  int64
	)
	stop := make(chan struct{})
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, dErr := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 16})
				if dErr != nil {
					t.Errorf("dequeue: %v", dErr)
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&got, 1)
					if aErr := adapter.Ack(ctx, d); aErr != nil {
						t.Errorf("ack: %v", aErr)
						return
					}
				}
				if len(ds) == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for atomic.LoadInt64(&got) < total {
		if time.Now().After(deadline) {
			close(stop)
			wg.Wait()
			t.Fatalf("queue-rabbitmq: timeout, got %d/%d (lost messages)", atomic.LoadInt64(&got), total)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	for id, n := range seen {
		if n != 1 {
			t.Fatalf("queue-rabbitmq: message %s delivered %d times (exactly-once violated)", id, n)
		}
	}
	if len(seen) != total {
		t.Fatalf("queue-rabbitmq: expected %d distinct messages, got %d", total, len(seen))
	}
}
