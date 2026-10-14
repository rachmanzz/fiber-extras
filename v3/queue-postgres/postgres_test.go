package queuepostgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	queuepostgres "github.com/rachmanzz/gosuite/v3/queue-postgres"
)

var (
	_ queue.Adapter         = (*queuepostgres.PostgresDriver)(nil)
	_ queue.DelayedEnqueuer = (*queuepostgres.PostgresDriver)(nil)
	_ queue.Replayer        = (*queuepostgres.PostgresDriver)(nil)
)

func TestPostgresDriver_Capabilities(t *testing.T) {
	driver := queuepostgres.New(nil, queuepostgres.WithTableName("custom_queue"))

	caps := driver.Capabilities()
	if !caps.Durable {
		t.Fatal("expected Postgres queue to be durable")
	}
	if caps.StrictGlobalOrder {
		t.Fatal("expected Postgres StrictGlobalOrder to be false (SKIP LOCKED + parallel dispatch)")
	}
	if caps.NativePriority != queue.PriorityHard {
		t.Fatal("expected Postgres queue to have PriorityHard")
	}
	if driver.Name() != "postgres" {
		t.Fatalf("expected name 'postgres', got %s", driver.Name())
	}
}

func TestPostgresDriver_CapabilityConsistency(t *testing.T) {
	driver := queuepostgres.New(nil, queuepostgres.WithTableName("custom_queue"))

	_, replayer := any(driver).(queue.Replayer)
	_, delayed := any(driver).(queue.DelayedEnqueuer)
	caps := driver.Capabilities()

	if caps.Replay != replayer {
		t.Fatalf("Replay capability (%v) disagrees with Replayer implementation (%v)", caps.Replay, replayer)
	}
	if caps.DelayedEnqueue != delayed {
		t.Fatalf("DelayedEnqueue capability (%v) disagrees with DelayedEnqueuer implementation (%v)", caps.DelayedEnqueue, delayed)
	}
}

// TestPostgresDriver_DedupCapabilityReflectsReality guards B4: PostgreSQL has no
// deduplication mechanism, so the capability must be declared false.
func TestPostgresDriver_DedupCapabilityReflectsReality(t *testing.T) {
	driver := queuepostgres.New(nil, queuepostgres.WithTableName("custom_queue"))
	if driver.Capabilities().Dedup {
		t.Fatal("expected Postgres Dedup capability to be false (no dedup mechanism)")
	}
}

// TestPostgresDriver_NilDeliveryRejected guards M-P4: Ack/Nack/Reenqueue with a
// nil delivery (or one without a token) must surface a caller bug instead of
// silently succeeding.
func TestPostgresDriver_NilDeliveryRejected(t *testing.T) {
	driver := queuepostgres.New(nil, queuepostgres.WithTableName("custom_queue"))
	ctx := context.Background()

	if err := driver.Ack(ctx, nil); !errors.Is(err, queuepostgres.ErrNilDelivery) {
		t.Fatalf("Ack(nil): expected ErrNilDelivery, got %v", err)
	}
	if err := driver.Nack(ctx, nil, nil); !errors.Is(err, queuepostgres.ErrNilDelivery) {
		t.Fatalf("Nack(nil): expected ErrNilDelivery, got %v", err)
	}
	noToken := &queue.Delivery{Message: &queue.Message{ID: "x"}}
	if err := driver.Reenqueue(ctx, noToken, noToken.Message, time.Now()); !errors.Is(err, queuepostgres.ErrNilDelivery) {
		t.Fatalf("Reenqueue(no token): expected ErrNilDelivery, got %v", err)
	}
}

// TestWithTableName_RejectsInvalidIdentifier guards B13: table names are
// interpolated into SQL, so invalid identifiers must be rejected at build time.
func TestWithTableName_RejectsInvalidIdentifier(t *testing.T) {
	for _, bad := range []string{"queue; DROP TABLE users--", "1bad", "queue messages", "schema.queue", `"quoted"`} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("expected panic for invalid table name %q", bad)
				}
			}()
			_ = queuepostgres.New(nil, queuepostgres.WithTableName(bad))
		}()
	}

	valid := queuepostgres.New(nil, queuepostgres.WithTableName("valid_queue_1"))
	if valid == nil {
		t.Fatal("expected valid table name to be accepted")
	}
}

func TestPostgresDriver_NilMessageRejected(t *testing.T) {
	driver := queuepostgres.New(nil, queuepostgres.WithTableName("custom_queue"))
	ctx := context.Background()

	if err := driver.Enqueue(ctx, nil); !errors.Is(err, queuepostgres.ErrNilMessage) {
		t.Fatalf("Enqueue(nil): expected ErrNilMessage, got %v", err)
	}
	if err := driver.EnqueueAt(ctx, nil, time.Now()); !errors.Is(err, queuepostgres.ErrNilMessage) {
		t.Fatalf("EnqueueAt(nil): expected ErrNilMessage, got %v", err)
	}
}
