package queuerabbitmq_test

import (
	"testing"

	"github.com/rachmanzz/gosuite/v3/queue"
	queuerabbitmq "github.com/rachmanzz/gosuite/v3/queue-rabbitmq"
)

var _ queue.Adapter = (*queuerabbitmq.RabbitMQAdapter)(nil)

func TestRabbitMQAdapter_Capabilities(t *testing.T) {
	adapter := &queuerabbitmq.RabbitMQAdapter{}
	caps := adapter.Capabilities()

	if !caps.Durable {
		t.Fatal("expected RabbitMQ to be durable")
	}
	if caps.NativePriority != queue.PriorityHard {
		t.Fatal("expected RabbitMQ to enforce PriorityHard")
	}
	if adapter.Name() != "rabbitmq" {
		t.Fatalf("expected name 'rabbitmq', got %s", adapter.Name())
	}
}

func TestRabbitMQAdapter_CapabilityConsistency(t *testing.T) {
	adapter := &queuerabbitmq.RabbitMQAdapter{}

	_, replayer := any(adapter).(queue.Replayer)
	_, delayed := any(adapter).(queue.DelayedEnqueuer)
	caps := adapter.Capabilities()

	if caps.Replay != replayer {
		t.Fatalf("Replay capability (%v) disagrees with Replayer implementation (%v)", caps.Replay, replayer)
	}
	if caps.DelayedEnqueue != delayed {
		t.Fatalf("DelayedEnqueue capability (%v) disagrees with DelayedEnqueuer implementation (%v)", caps.DelayedEnqueue, delayed)
	}
}

// TestRabbitMQAdapter_DedupCapabilityReflectsReality guards B4: RabbitMQ has no
// deduplication mechanism, so the capability must be declared false.
func TestRabbitMQAdapter_DedupCapabilityReflectsReality(t *testing.T) {
	adapter := &queuerabbitmq.RabbitMQAdapter{}
	if adapter.Capabilities().Dedup {
		t.Fatal("expected RabbitMQ Dedup capability to be false (no dedup mechanism)")
	}
}

func TestRabbitMQAdapter_DefaultQueueName(t *testing.T) {
	if got := queuerabbitmq.DefaultConfig().QueueName; got != "gosuite_priority_queue" {
		t.Fatalf("expected default QueueName 'gosuite_priority_queue', got %q", got)
	}
}

func TestRabbitMQAdapter_NilMessageRejected(t *testing.T) {
	adapter := &queuerabbitmq.RabbitMQAdapter{}
	err := adapter.Enqueue(nil, nil)
	if err == nil || err.Error() != queuerabbitmq.ErrNilMessage.Error() {
		t.Fatalf("expected ErrNilMessage on Enqueue(nil), got %v", err)
	}
}
