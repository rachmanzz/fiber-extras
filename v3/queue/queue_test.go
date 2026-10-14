package queue_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/rachmanzz/gosuite/v3/worker"
)

func TestQueue_MemoryDriver_PriorityOrdering(t *testing.T) {
	driver := queue.NewMemoryDriver()
	defer func() { _ = driver.Close() }()

	ctx := context.Background()

	// Enqueue Low, Critical, Default
	m1 := queue.NewMessage("test:order", []byte("low"), queue.WithPriority(queue.PriorityLow))
	m2 := queue.NewMessage("test:order", []byte("critical"), queue.WithPriority(queue.PriorityCritical))
	m3 := queue.NewMessage("test:order", []byte("default"), queue.WithPriority(queue.PriorityDefault))

	_ = driver.Enqueue(ctx, m1)
	_ = driver.Enqueue(ctx, m2)
	_ = driver.Enqueue(ctx, m3)

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("unexpected dequeue error: %v", err)
	}
	if len(deliveries) != 3 {
		t.Fatalf("expected 3 deliveries, got %d", len(deliveries))
	}

	// Order must be Critical (100) -> Default (50) -> Low (25)
	if deliveries[0].Message.Priority != queue.PriorityCritical {
		t.Errorf("expected 1st delivery Critical, got %d", deliveries[0].Message.Priority)
	}
	if deliveries[1].Message.Priority != queue.PriorityDefault {
		t.Errorf("expected 2nd delivery Default, got %d", deliveries[1].Message.Priority)
	}
	if deliveries[2].Message.Priority != queue.PriorityLow {
		t.Errorf("expected 3rd delivery Low, got %d", deliveries[2].Message.Priority)
	}
}

func TestQueue_EndToEnd_DispatchAndConsume(t *testing.T) {
	queue.Reset()
	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	// Ensure worker engine is initialized
	worker.InitGlobal(worker.Config{
		MaxWorkers: 2,
		Logger:     worker.NopLogger(),
	})

	topic := "events:user:created"
	done := make(chan struct{})
	var receivedPayload string

	queue.RegisterHandler(topic, func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		receivedPayload = string(msg.Payload)
		close(done)
		return nil
	})

	_, err := queue.StartConsumer(
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithLogger(queue.NopLogger()),
	)
	if err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer queue.StopConsumer()

	payload := map[string]string{"name": "Alice"}
	err = queue.Dispatch(context.Background(), topic, payload)
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-done:
		if receivedPayload == "" {
			t.Fatal("empty received payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for consumer handler execution")
	}
}

func TestQueue_GroupFanout(t *testing.T) {
	queue.Reset()
	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	worker.InitGlobal(worker.Config{
		MaxWorkers: 4,
		Logger:     worker.NopLogger(),
	})

	topic := "group:sync:items"
	var processedCount int32
	var wg sync.WaitGroup
	wg.Add(3)

	queue.RegisterHandler(topic, func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&processedCount, 1)
		wg.Done()
		return nil
	})

	_, err := queue.StartConsumer(
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithLogger(queue.NopLogger()),
	)
	if err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer queue.StopConsumer()

	ctx := context.Background()
	groupID := "sync-group-1"
	_ = queue.Dispatch(ctx, topic, "item-1", queue.WithGroup(groupID, 1))
	_ = queue.Dispatch(ctx, topic, "item-2", queue.WithGroup(groupID, 2))
	_ = queue.Dispatch(ctx, topic, "item-3", queue.WithGroup(groupID, 3))

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if atomic.LoadInt32(&processedCount) != 3 {
			t.Fatalf("expected 3 items processed, got %d", atomic.LoadInt32(&processedCount))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for group items")
	}
}

func TestQueue_Retry_And_DLQ_Routing(t *testing.T) {
	queue.Reset()
	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	worker.InitGlobal(worker.Config{MaxWorkers: 2, Logger: worker.NopLogger()})

	topic := "failing:task"
	dlqTopic := queue.DLQTopic(topic)

	var attempts int32
	dlqDone := make(chan struct{})
	var dlqReason string

	// Handler fails on retryable error until max attempts (2)
	queue.RegisterHandler(topic, func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("transient database failure")
	})

	// DLQ Handler
	queue.RegisterHandler(dlqTopic, func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		dlqReason = msg.Headers["dlq_reason"]
		close(dlqDone)
		return nil
	})

	retryPolicy := queue.RetryPolicy{
		MaxAttempts: 2,
		Initial:     10 * time.Millisecond,
		Max:         50 * time.Millisecond,
		Multiplier:  1.5,
		Jitter:      0,
	}

	_, err := queue.StartConsumer(
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithRetryPolicy(retryPolicy),
		queue.WithLogger(queue.NopLogger()),
	)
	if err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer queue.StopConsumer()

	err = queue.Dispatch(context.Background(), topic, "test-data", queue.WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-dlqDone:
		if atomic.LoadInt32(&attempts) != 2 {
			t.Errorf("expected 2 attempts before DLQ, got %d", atomic.LoadInt32(&attempts))
		}
		if dlqReason != "transient database failure" {
			t.Errorf("unexpected DLQ reason: %s", dlqReason)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message to land in DLQ")
	}
}

func TestQueue_NonRetryable_Immediately_DLQ(t *testing.T) {
	queue.Reset()
	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	worker.InitGlobal(worker.Config{MaxWorkers: 2, Logger: worker.NopLogger()})

	topic := "nonretryable:topic"
	dlqTopic := queue.DLQTopic(topic)

	dlqDone := make(chan struct{})

	// No handler registered for topic -> ErrNoHandlerFound (non-retryable)
	queue.RegisterHandler(dlqTopic, func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
		close(dlqDone)
		return nil
	})

	_, err := queue.StartConsumer(
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithLogger(queue.NopLogger()),
	)
	if err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer queue.StopConsumer()

	// Dispatch to unhandled topic
	err = queue.Dispatch(context.Background(), topic, "unhandled-data")
	if err != nil {
		t.Fatalf("dispatch error: %v", err)
	}

	select {
	case <-dlqDone:
		// Succeeded in routing non-retryable message straight to DLQ
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for non-retryable message in DLQ")
	}
}

func TestQueue_MultiDriver_Routing(t *testing.T) {
	queue.Reset()
	driverA := queue.NewNamedMemoryDriver("driver-a")
	driverB := queue.NewNamedMemoryDriver("driver-b")

	queue.RegisterDriver("driver-a", driverA)
	queue.RegisterDriver("driver-b", driverB)
	_ = queue.SetDefaultDriver("driver-a")

	// Message explicitly tagged with driver-b
	mB := queue.NewMessage("topic:b", []byte("b"), queue.WithDriver("driver-b"))
	if err := queue.Enqueue(context.Background(), mB); err != nil {
		t.Fatalf("enqueue mB error: %v", err)
	}

	// Message with default driver
	mA := queue.NewMessage("topic:a", []byte("a"))
	if err := queue.Enqueue(context.Background(), mA); err != nil {
		t.Fatalf("enqueue mA error: %v", err)
	}

	delA, _ := driverA.Dequeue(context.Background(), queue.DequeueRequest{BatchSize: 10})
	delB, _ := driverB.Dequeue(context.Background(), queue.DequeueRequest{BatchSize: 10})

	if len(delA) != 1 || string(delA[0].Message.Payload) != "a" {
		t.Fatal("expected delivery A in driverA")
	}
	if len(delB) != 1 || string(delB[0].Message.Payload) != "b" {
		t.Fatal("expected delivery B in driverB")
	}
}

func TestQueue_MemoryDriver_CapabilityConsistency(t *testing.T) {
	driver := queue.NewMemoryDriver()
	defer func() { _ = driver.Close() }()

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

// TestQueue_MemoryDriver_SortedInsertionFIFO guards the binary-search insertion
// path: messages of equal priority must be dequeued in FIFO order even when
// they are inserted interleaved with higher/lower priority messages.
func TestQueue_MemoryDriver_SortedInsertionFIFO(t *testing.T) {
	driver := queue.NewMemoryDriver()
	defer func() { _ = driver.Close() }()

	ctx := context.Background()
	specs := []struct {
		id   string
		prio queue.Priority
	}{
		{"d1", queue.PriorityDefault},
		{"x", queue.PriorityCritical},
		{"d2", queue.PriorityDefault},
		{"d3", queue.PriorityDefault},
		{"y", queue.PriorityLow},
		{"d4", queue.PriorityDefault},
	}
	for _, s := range specs {
		m := queue.NewMessage("test:fifo", nil, queue.WithPriority(s.prio))
		m.ID = s.id
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	want := []string{"x", "d1", "d2", "d3", "d4", "y"}
	if len(deliveries) != len(want) {
		t.Fatalf("expected %d deliveries, got %d", len(want), len(deliveries))
	}
	for i, w := range want {
		if got := deliveries[i].Message.ID; got != w {
			t.Fatalf("position %d: expected %q, got %q", i, w, got)
		}
	}
}

// TestQueue_MemoryDriver_EnqueueAtAfterClose verifies a delayed message
// scheduled before Close neither errors at schedule time nor fires after close.
func TestQueue_MemoryDriver_EnqueueAtAfterClose(t *testing.T) {
	driver := queue.NewMemoryDriver()
	ctx := context.Background()

	m := queue.NewMessage("test:delayed", []byte("payload"))
	if err := driver.EnqueueAt(ctx, m, time.Now().Add(20*time.Millisecond)); err != nil {
		t.Fatalf("EnqueueAt returned error: %v", err)
	}

	if err := driver.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Scheduling after Close must be rejected.
	if err := driver.EnqueueAt(ctx, queue.NewMessage("test:delayed", nil), time.Now().Add(time.Millisecond)); err == nil {
		t.Fatal("expected error enqueueing on a closed driver")
	}

	time.Sleep(60 * time.Millisecond)
	deliveries, _ := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if len(deliveries) != 0 {
		t.Fatalf("expected no deliveries after close, got %d", len(deliveries))
	}
}

func TestQueue_NilMessageRejected(t *testing.T) {
	ctx := context.Background()

	// 1. queue.Enqueue(nil)
	if err := queue.Enqueue(ctx, nil); !errors.Is(err, queue.ErrNilMessage) {
		t.Fatalf("queue.Enqueue(nil): expected ErrNilMessage, got %v", err)
	}

	// 2. MemoryDriver.Enqueue(nil)
	mem := queue.NewMemoryDriver()
	if err := mem.Enqueue(ctx, nil); !errors.Is(err, queue.ErrNilMessage) {
		t.Fatalf("mem.Enqueue(nil): expected ErrNilMessage, got %v", err)
	}
	if err := mem.EnqueueAt(ctx, nil, time.Now()); !errors.Is(err, queue.ErrNilMessage) {
		t.Fatalf("mem.EnqueueAt(nil): expected ErrNilMessage, got %v", err)
	}
}

func TestQueue_ErrDeliveryLostExported(t *testing.T) {
	if queue.ErrDeliveryLost == nil {
		t.Fatal("expected queue.ErrDeliveryLost to be non-nil")
	}
}

