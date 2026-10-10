package queueredis_test

import (
	"context"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"

	queueredis "github.com/rachmanzz/fiber-extras/v3/queue-redis"
	"github.com/redis/go-redis/v9"
)

// Ensure RedisDriver satisfies queue.Adapter and queue.DelayedEnqueuer contracts at compile time.
var (
	_ queue.Adapter         = (*queueredis.RedisDriver)(nil)
	_ queue.DelayedEnqueuer = (*queueredis.RedisDriver)(nil)
)

func TestRedisDriver_ContractAndCapabilities(t *testing.T) {
	// Dummy client with no actual network calls
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	driver := queueredis.New(client, queueredis.WithPrefix("custom:prefix:"))

	if driver.Name() != "redis" {
		t.Fatalf("expected name 'redis', got %s", driver.Name())
	}

	caps := driver.Capabilities()
	if !caps.DelayedEnqueue {
		t.Fatal("expected DelayedEnqueue to be true")
	}
	if caps.NativePriority != queue.PrioritySoft {
		t.Fatalf("expected PrioritySoft, got %v", caps.NativePriority)
	}

	// Test Ack and Nack with nil tokens do not error
	ctx := context.Background()
	if err := driver.Ack(ctx, nil); err != nil {
		t.Fatalf("expected nil on nil delivery ack: %v", err)
	}
	if err := driver.Nack(ctx, nil, nil); err != nil {
		t.Fatalf("expected nil on nil delivery nack: %v", err)
	}

	if err := driver.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}
}

func TestRedisDriver_NewAdapterAlias(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	adapter := queueredis.NewRedisAdapter(client, "test:")
	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}
	if adapter.Name() != "redis" {
		t.Fatalf("expected name 'redis', got %s", adapter.Name())
	}
}

func TestRedisDriver_CapabilityConsistency(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()

	driver := queueredis.New(client)

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
