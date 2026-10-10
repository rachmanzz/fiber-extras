package queuepostgres_test

import (
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuepostgres "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
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
	if !caps.StrictGlobalOrder {
		t.Fatal("expected Postgres queue to have StrictGlobalOrder")
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
