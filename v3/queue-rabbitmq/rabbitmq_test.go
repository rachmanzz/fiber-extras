package queuerabbitmq_test

import (
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuerabbitmq "github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq"
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
