package queueredis_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queueredis "github.com/rachmanzz/fiber-extras/v3/queue-redis"
	"github.com/redis/go-redis/v9"
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

// TestRedisDriver_Live exercises the full delivery loop against a real Redis
// instance: Enqueue -> priority Dequeue -> Ack -> Nack (redelivery) -> delayed EnqueueAt.
func TestRedisDriver_Live(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := "fiber:queue:live:" + randSuffix(t) + ":"
	dataKey := prefix + "messages_hash"
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset").Err()
	})

	low := newLiveMsg("email.send", queue.PriorityLow)
	def := newLiveMsg("email.send", queue.PriorityDefault)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so ordering is only attributable to the broker.
	for _, m := range []*queue.Message{low, def, high, crit} {
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-redis: enqueue %s failed: %v", m.ID, err)
		}
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-redis: dequeue failed: %v", err)
	}
	if len(deliveries) != 4 {
		t.Fatalf("queue-redis: expected 4 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, def.ID, low.ID})

	// Ack the critical message: its payload must be removed from the data hash.
	if err := driver.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
	exists, err := client.HExists(ctx, dataKey, crit.ID).Result()
	if err != nil {
		t.Fatalf("queue-redis: hash existence probe failed: %v", err)
	}
	if exists {
		t.Fatal("queue-redis: acked message payload still present in hash")
	}

	// Nack the default message: it must be redelivered on the next Dequeue.
	if err := driver.Nack(ctx, deliveries[2], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-redis: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != def.ID {
		t.Fatalf("queue-redis: expected redelivery of nack'd message %s, got %d delivery(ies)", def.ID, len(redelivered))
	}
	if err := driver.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-redis: ack after redelivery failed: %v", err)
	}

	// Delayed scheduling: the message must stay invisible until it matures.
	delayed := newLiveMsg("report.generate", queue.PriorityDefault)
	if err := driver.EnqueueAt(ctx, delayed, time.Now().Add(250*time.Millisecond)); err != nil {
		t.Fatalf("queue-redis: delayed enqueue failed: %v", err)
	}

	notYet, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-redis: premature dequeue failed: %v", err)
	}
	for _, d := range notYet {
		if d.Message.ID == delayed.ID {
			t.Fatal("queue-redis: delayed message delivered before maturity")
		}
	}

	time.Sleep(400 * time.Millisecond)

	matured := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	found := false
	for _, d := range matured {
		if d.Message.ID == delayed.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("queue-redis: delayed message not delivered after maturity")
	}
	if err := driver.Ack(ctx, matured[0]); err != nil {
		t.Fatalf("queue-redis: ack of delayed message failed: %v", err)
	}
}

// TestRedisDriver_Live_PrioritySurvivesNack verifies that a nack'd message keeps
// its original priority class when it is restored into the priority ZSet.
func TestRedisDriver_Live_PrioritySurvivesNack(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live Redis integration test (run scripts/test-all-brokers.sh to enable)")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("queue-redis: cannot reach live redis at %s: %v", addr, err)
	}

	prefix := fmt.Sprintf("fiber:queue:live:%s:", randSuffix(t))
	driver := queueredis.New(client, queueredis.WithPrefix(prefix))

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = client.Del(cctx, prefix+"priority_zset", prefix+"messages_hash", prefix+"delayed_zset").Err()
	})

	def := newLiveMsg("invoice.settle", queue.PriorityDefault)
	crit := newLiveMsg("invoice.settle", queue.PriorityCritical)

	if err := driver.Enqueue(ctx, def); err != nil {
		t.Fatalf("queue-redis: enqueue failed: %v", err)
	}

	batch, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("queue-redis: dequeue failed: %v", err)
	}
	if len(batch) != 1 || batch[0].Message.ID != def.ID {
		t.Fatalf("queue-redis: expected message %s, got %d delivery(ies)", def.ID, len(batch))
	}

	if err := driver.Enqueue(ctx, crit); err != nil {
		t.Fatalf("queue-redis: enqueue of critical failed: %v", err)
	}

	if err := driver.Nack(ctx, batch[0], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-redis: nack failed: %v", err)
	}

	round := dequeueUntil(t, ctx, driver, 2*time.Second, 2)
	if len(round) != 2 {
		t.Fatalf("queue-redis: expected 2 deliveries, got %d", len(round))
	}
	assertOrder(t, round, []string{crit.ID, def.ID})

	if err := driver.Ack(ctx, round[0]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
	if err := driver.Ack(ctx, round[1]); err != nil {
		t.Fatalf("queue-redis: ack failed: %v", err)
	}
}
