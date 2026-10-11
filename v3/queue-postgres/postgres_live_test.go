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
	crit.Headers["content-type"] = "application/msgpack"
	crit.Headers["x-audit"] = "kept"
	crit.Attempt = 1
	crit.MaxAttempts = 7

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

	if got := deliveries[0].Message.Headers["content-type"]; got != "application/msgpack" {
		t.Fatalf("queue-postgres: headers not persisted, content-type = %q", got)
	}
	if got := deliveries[0].Message.Headers["x-audit"]; got != "kept" {
		t.Fatalf("queue-postgres: header x-audit lost, got %q", got)
	}
	if got := deliveries[0].Message.Attempt; got != 1 {
		t.Fatalf("queue-postgres: attempts not persisted, got %d want 1", got)
	}
	if got := deliveries[0].Message.MaxAttempts; got != 7 {
		t.Fatalf("queue-postgres: max_attempts not persisted, got %d want 7", got)
	}

	// Ack the critical message: the row is soft-deleted (status 'acked') so Replay
	// can still surface it; it must not be redelivered.
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

	// Replay: all rows created so far must be streamed back, including the
	// completed (acked) rows crit and def — Ack keeps them for replay until purged.
	// Replay orders by created_at ASC (not priority), so compare as a set.
	replayCh, err := driver.Replay(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("queue-postgres: replay failed: %v", err)
	}
	replayed := make(map[string]bool)
	for d := range replayCh {
		replayed[d.Message.ID] = true
	}
	if len(replayed) != 4 || !replayed[crit.ID] || !replayed[high.ID] || !replayed[def.ID] || !replayed[low.ID] {
		t.Fatalf("queue-postgres: expected replay of [%s %s %s %s], got %v", crit.ID, high.ID, def.ID, low.ID, replayed)
	}

	// Delayed scheduling: the message must stay invisible until it matures.
	delayed := newLiveMsg("report.generate", queue.PriorityDefault)
	delayed.Attempt = 4
	delayed.MaxAttempts = 9
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
			if d.Message.Attempt != 4 || d.Message.MaxAttempts != 9 {
				t.Fatalf("queue-postgres: retry attempt state lost on EnqueueAt redelivery: Attempt=%d MaxAttempts=%d want 4/9",
					d.Message.Attempt, d.Message.MaxAttempts)
			}
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

// TestPostgresDriver_VisibilityTimeoutRedelivery verifies the lease semantics: a
// claimed message is invisible while its lease is active, and is redelivered as
// soon as the lease expires (simulating a worker that crashed mid-processing).
func TestPostgresDriver_VisibilityTimeoutRedelivery(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_lease_" + randSuffix(t)
	driver := queuepostgres.New(pool,
		queuepostgres.WithTableName(table),
		queuepostgres.WithVisibilityTimeout(1*time.Second),
	)

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := newLiveMsg("lease.check", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue failed: %v", err)
	}

	first, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: first dequeue failed: %v", err)
	}
	if len(first) != 1 || first[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected 1 delivery of %s, got %d", m.ID, len(first))
	}

	later, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: locked dequeue failed: %v", err)
	}
	if len(later) != 0 {
		t.Fatalf("queue-postgres: message redelivered while lease still active (got %d)", len(later))
	}

	redelivered := dequeueUntil(t, ctx, driver, 3*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected redelivery of %s after lease expiry, got %d delivery(ies)", m.ID, len(redelivered))
	}
}

// TestPostgresDriver_ReenqueueReschedulesClaimedDelivery verifies Reenqueue: a
// claimed (in-flight) delivery is rescheduled to a future time without being Acked,
// stays invisible until maturity, and is redelivered with attempt state intact.
func TestPostgresDriver_ReenqueueReschedulesClaimedDelivery(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_reenqueue_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := newLiveMsg("retry.reenqueue", queue.PriorityDefault)
	m.Attempt = 4
	m.MaxAttempts = 9
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue failed: %v", err)
	}

	claimed, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected 1 claim of %s, got %d", m.ID, len(claimed))
	}

	if err := driver.Reenqueue(ctx, claimed[0], claimed[0].Message, time.Now().Add(300*time.Millisecond)); err != nil {
		t.Fatalf("queue-postgres: reenqueue failed: %v", err)
	}

	locked, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: locked dequeue failed: %v", err)
	}
	if len(locked) != 0 {
		t.Fatalf("queue-postgres: resent message visible before maturity (got %d)", len(locked))
	}

	redelivered := dequeueUntil(t, ctx, driver, 3*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected redelivery of resent message %s, got %d", m.ID, len(redelivered))
	}
	if redelivered[0].Message.Attempt != 4 || redelivered[0].Message.MaxAttempts != 9 {
		t.Fatalf("queue-postgres: attempt state lost on resent delivery: Attempt=%d MaxAttempts=%d want 4/9",
			redelivered[0].Message.Attempt, redelivered[0].Message.MaxAttempts)
	}
}

// TestPostgresDriver_StaleLeaseCannotClobberReclaim guards NEW-C5/NEW-H1: once a
// lease expires and another worker re-claims the row, an Ack/Reenqueue carrying
// the stale lease must not affect the row that the new owner is working on.
func TestPostgresDriver_StaleLeaseCannotClobberReclaim(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_stale_" + randSuffix(t)
	driver := queuepostgres.New(pool,
		queuepostgres.WithTableName(table),
		queuepostgres.WithVisibilityTimeout(1*time.Second),
	)

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := newLiveMsg("lease.stale", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue failed: %v", err)
	}

	first, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil || len(first) != 1 {
		t.Fatalf("queue-postgres: expected initial claim, got %d (err=%v)", len(first), err)
	}

	// Let the lease expire so the row is reclaimed by a second worker.
	time.Sleep(1200 * time.Millisecond)
	second := dequeueUntil(t, ctx, driver, 2*time.Second, 1)
	if len(second) != 1 || second[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected reclaim of %s, got %d", m.ID, len(second))
	}

	// The stale (first) owner must not be able to Ack the row the second owner holds.
	if err := driver.Ack(ctx, first[0]); err != nil {
		t.Fatalf("queue-postgres: stale ack returned unexpected error: %v", err)
	}
	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1", m.ID).Scan(&status); err != nil {
		t.Fatalf("queue-postgres: status probe failed: %v", err)
	}
	if status != "processing" {
		t.Fatalf("queue-postgres: stale Ack clobbered reclaimed row, status = %q want 'processing'", status)
	}

	// A stale Reenqueue must be rejected rather than rescheduling another owner's row.
	if err := driver.Reenqueue(ctx, first[0], first[0].Message, time.Now().Add(time.Minute)); !errors.Is(err, queuepostgres.ErrDeliveryLost) {
		t.Fatalf("queue-postgres: expected ErrDeliveryLost from stale Reenqueue, got %v", err)
	}

	// The current owner can still settle the row.
	if err := driver.Ack(ctx, second[0]); err != nil {
		t.Fatalf("queue-postgres: current owner ack failed: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1", m.ID).Scan(&status); err != nil {
		t.Fatalf("queue-postgres: status probe failed: %v", err)
	}
	if status != "acked" {
		t.Fatalf("queue-postgres: expected status 'acked' after current-owner Ack, got %q", status)
	}
}

// TestPostgresDriver_CorruptHeadersBecomePoison guards M-P3: a row whose headers
// column holds valid JSON that is not a string map (so it cannot decode) must be
// surfaced as a poison delivery instead of being dropped or failing the batch.
func TestPostgresDriver_CorruptHeadersBecomePoison(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_poison_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	// headers is JSONB, so it always holds valid JSON; an array is valid JSON but
	// cannot unmarshal into map[string]string, reproducing the corruption path.
	const badID = "corrupt-headers-row"
	if _, err := pool.Exec(ctx, "INSERT INTO "+table+
		" (id, topic, payload, priority, status, process_at, headers) VALUES ($1, 't', ''::bytea, 50, 'pending', now(), $2::jsonb)",
		badID, "[1,2,3]"); err != nil {
		t.Fatalf("queue-postgres: seed corrupt row failed: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("queue-postgres: expected 1 poison delivery, got %d", len(deliveries))
	}
	got := deliveries[0].Message
	if got.Topic != queue.PoisonTopic {
		t.Fatalf("queue-postgres: expected poison topic %q, got %q", queue.PoisonTopic, got.Topic)
	}
	if got.ID != badID {
		t.Fatalf("queue-postgres: poison delivery lost original ID, got %q want %q", got.ID, badID)
	}
	if got.Headers["decode_error"] == "" {
		t.Fatal("queue-postgres: poison delivery missing decode_error header")
	}
}

// TestPostgresDriver_AttemptsNeverMoveBackwards guards NEW-H5: re-enqueuing a
// message with a stale (lower) attempt count must not reset the retry counter.
func TestPostgresDriver_AttemptsNeverMoveBackwards(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_attempts_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	high := newLiveMsg("retry.attempts", queue.PriorityDefault)
	high.ID = "attempts-row"
	high.Attempt = 5
	high.MaxAttempts = 9
	if err := driver.Enqueue(ctx, high); err != nil {
		t.Fatalf("queue-postgres: first enqueue failed: %v", err)
	}

	stale := newLiveMsg("retry.attempts", queue.PriorityDefault)
	stale.ID = high.ID
	stale.Attempt = 0
	if err := driver.Enqueue(ctx, stale); err != nil {
		t.Fatalf("queue-postgres: stale re-enqueue failed: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("queue-postgres: expected 1 delivery, got %d", len(deliveries))
	}
	if got := deliveries[0].Message.Attempt; got != 5 {
		t.Fatalf("queue-postgres: attempt counter moved backwards, got %d want 5", got)
	}
}

// TestPostgresDriver_SubSecondLeaseHonored guards M-P1: a sub-second visibility
// timeout must be applied with millisecond precision, so a message stays locked
// immediately after being claimed instead of expiring at once.
func TestPostgresDriver_SubSecondLeaseHonored(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_subsec_" + randSuffix(t)
	driver := queuepostgres.New(pool,
		queuepostgres.WithTableName(table),
		queuepostgres.WithVisibilityTimeout(500*time.Millisecond),
	)

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := newLiveMsg("lease.subsecond", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue failed: %v", err)
	}

	first, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil || len(first) != 1 {
		t.Fatalf("queue-postgres: expected initial claim, got %d (err=%v)", len(first), err)
	}

	// Immediately after claiming, the row must still be locked: with second
	// truncation the 500ms lease would have become 0 and the row would reappear.
	locked, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: locked dequeue failed: %v", err)
	}
	if len(locked) != 0 {
		t.Fatalf("queue-postgres: sub-second lease not honored, row redelivered immediately (%d)", len(locked))
	}
}

// TestPostgresDriver_ReplayReturnsQueryError guards B12: Replay executes its
// query synchronously so a failure is returned to the caller instead of being
// silently swallowed inside the streaming goroutine.
func TestPostgresDriver_ReplayReturnsQueryError(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_replay_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	ch, err := driver.Replay(cancelled, time.Now().Add(-time.Hour))
	if err == nil {
		t.Fatal("expected Replay to return an error for a cancelled context, got nil")
	}
	if ch != nil {
		t.Fatal("expected nil channel when Replay fails synchronously")
	}
}

// TestPostgresDriver_AckIsSoftDeleteAndPurgeable guards B4(c): Acked rows stay
// queryable via Replay (status 'acked') and are only physically removed by
// PurgeAcked.
func TestPostgresDriver_AckIsSoftDeleteAndPurgeable(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_purge_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := newLiveMsg("audit.ack", queue.PriorityDefault)
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue failed: %v", err)
	}

	claimed, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected 1 claim of %s, got %d", m.ID, len(claimed))
	}
	if err := driver.Ack(ctx, claimed[0]); err != nil {
		t.Fatalf("queue-postgres: ack failed: %v", err)
	}

	// The acked row must survive as status 'acked', not be deleted.
	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM "+table+" WHERE id = $1", m.ID).Scan(&status); err != nil {
		t.Fatalf("queue-postgres: acked row missing from table: %v", err)
	}
	if status != "acked" {
		t.Fatalf("queue-postgres: expected status 'acked', got %q", status)
	}

	// Acked rows must not be redelivered.
	if again, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10}); err != nil {
		t.Fatalf("queue-postgres: dequeue after ack failed: %v", err)
	} else if len(again) != 0 {
		t.Fatalf("queue-postgres: acked message redelivered (%d)", len(again))
	}

	// Replay surfaces the acked row.
	replayCh, err := driver.Replay(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("queue-postgres: replay failed: %v", err)
	}
	seen := false
	for d := range replayCh {
		if d.Message.ID == m.ID {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("queue-postgres: replay did not include acked message %s", m.ID)
	}

	// PurgeAcked physically removes it once older than the cutoff.
	purged, err := driver.PurgeAcked(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("queue-postgres: purge failed: %v", err)
	}
	if purged != 1 {
		t.Fatalf("queue-postgres: expected 1 purged row, got %d", purged)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id = $1", m.ID).Scan(&count); err != nil {
		t.Fatalf("queue-postgres: count probe failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("queue-postgres: acked row still present after purge (%d)", count)
	}
}

// TestPostgresDriver_NilPayloadEnqueue guards cross-driver consistency: a
// message with a nil payload is valid for every other driver and must not be
// rejected by the NOT NULL payload column.
func TestPostgresDriver_NilPayloadEnqueue(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration test (run scripts/test-all-brokers.sh to enable)")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: cannot connect to live postgres: %v", err)
	}
	defer pool.Close()

	table := "queue_messages_nil_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))

	ctx := context.Background()
	if err := driver.EnsureTable(ctx); err != nil {
		t.Fatalf("queue-postgres: EnsureTable failed: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	}()

	m := queue.NewMessage("audit.nil", nil)
	if err := driver.Enqueue(ctx, m); err != nil {
		t.Fatalf("queue-postgres: enqueue of nil payload failed: %v", err)
	}

	claimed, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-postgres: dequeue failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Message.ID != m.ID {
		t.Fatalf("queue-postgres: expected 1 claim of %s, got %d", m.ID, len(claimed))
	}
	if len(claimed[0].Message.Payload) != 0 {
		t.Fatalf("queue-postgres: expected empty payload, got %q", claimed[0].Message.Payload)
	}
}
