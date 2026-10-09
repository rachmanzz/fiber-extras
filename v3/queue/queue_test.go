package queue_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	"github.com/rachmanzz/fiber-extras/v3/worker"
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

type mockRegistrar struct {
	hooks []func() error
}

func (m *mockRegistrar) RegisterBeforeStart(hook func() error) {
	m.hooks = append(m.hooks, hook)
}

type mockHooks struct {
	shutdownHooks []func(error) error
}

func (m *mockHooks) OnPostShutdown(hook func(error) error) {
	m.shutdownHooks = append(m.shutdownHooks, hook)
}

type mockApp struct {
	hooks *mockHooks
}

func (m *mockApp) Hooks() queue.PostShutdownRegistrar {
	return m.hooks
}

func TestQueue_StarterHooks(t *testing.T) {
	queue.Reset()
	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	reg := &mockRegistrar{}
	app := &mockApp{hooks: &mockHooks{}}

	queue.RegisterStarterHook(reg, app, queue.WithLogger(queue.NopLogger()))

	if len(reg.hooks) != 1 {
		t.Fatalf("expected 1 prestart hook, got %d", len(reg.hooks))
	}
	if len(app.hooks.shutdownHooks) != 1 {
		t.Fatalf("expected 1 shutdown hook, got %d", len(app.hooks.shutdownHooks))
	}

	// Execute PreStart
	if err := reg.hooks[0](); err != nil {
		t.Fatalf("PreStart failed: %v", err)
	}

	// Execute PostShutdown
	if err := app.hooks.shutdownHooks[0](nil); err != nil {
		t.Fatalf("PostShutdown failed: %v", err)
	}
}
