package queuepostgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuepostgres "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
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

// TestPostgresDriver_Live exercises the full delivery loop against a real
// PostgreSQL instance: EnsureTable -> Enqueue -> priority Dequeue -> Ack ->
// Nack (redelivery) -> Replay -> delayed EnqueueAt.
func TestPostgresDriver_Live(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}

	table := "queue_messages_live_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		pool.Close()
	}()

	low := newLiveMsg("email.send", queue.PriorityLow)
	def := newLiveMsg("email.send", queue.PriorityDefault)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so ordering is only attributable to the broker.
	for _, m := range []*queue.Message{low, def, high, crit} {
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-postgres: enqueue %s failed: %v", m.ID, err)
		}
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(deliveries) != 4 {
		t.Fatalf("queue-postgres: expected 4 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, def.ID, low.ID})

	// Ack the critical message: the row must be deleted.
	if err := driver.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-postgres: ack failed: %v", err)
	}

	// Nack the default message: it must be redelivered while in-flight
	// messages (high, low) stay locked out until they are acked/nacked.
	if err := driver.Nack(ctx, deliveries[2], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-postgres: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != def.ID {
		t.Fatalf("queue-postgres: expected redelivery of nack'd message %s, got %d delivery(ies)", def.ID, len(redelivered))
	}
	if err := driver.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-postgres: ack after redelivery failed: %v", err)
	}

	// Replay: only rows still present (high, low) must be streamed back.
	// Replay orders by created_at ASC (not priority), so compare as a set.
	replayCh, err := driver.Replay(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("queue-postgres: replay failed: %v", err)
	}
	replayed := make(map[string]bool)
	for d := range replayCh {
		replayed[d.Message.ID] = true
	}
	if len(replayed) != 2 || !replayed[high.ID] || !replayed[low.ID] {
		t.Fatalf("queue-postgres: expected replay of [%s %s], got %v", high.ID, low.ID, replayed)
	}

	// Delayed scheduling: the message must stay invisible until it matures.
	delayed := newLiveMsg("report.generate", queue.PriorityDefault)
	if err := driver.EnqueueAt(ctx, delayed, time.Now().Add(250*time.Millisecond)); err != nil {
		t.Fatalf("queue-postgres: delayed enqueue failed: %v", err)
	}

	notYet, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: premature dequeue failed: %v", err)
	}
	for _, d := range notYet {
		if d.Message.ID == delayed.ID {
			t.Fatal("queue-postgres: delayed message delivered before maturity")
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
		t.Fatal("queue-postgres: delayed message not delivered after maturity")
	}
	if err := driver.Ack(ctx, matured[0]); err != nil {
		t.Fatalf("queue-postgres: ack of delayed message failed: %v", err)
	}
}
