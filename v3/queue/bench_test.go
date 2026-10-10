package queue_test

import (
	"context"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
)

func BenchmarkQueue_Memory_EnqueueDequeue(b *testing.B) {
	driver := queue.NewMemoryDriver()
	ctx := context.Background()

	msg := queue.NewMessage("bench.topic", []byte("benchmark payload data"), queue.WithPriority(queue.PriorityHigh))
	req := queue.DequeueRequest{BatchSize: 10}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = driver.Enqueue(ctx, msg)
		_, _ = driver.Dequeue(ctx, req)
	}
}

func BenchmarkQueue_Dispatch_JSON(b *testing.B) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	payload := struct {
		Event string `json:"event"`
		ID    int    `json:"id"`
	}{
		Event: "user.login",
		ID:    12345,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = queue.Dispatch(ctx, "bench.dispatch", payload)
	}
}

func BenchmarkQueue_Message_Creation(b *testing.B) {
	payload := []byte("benchmark payload")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = queue.NewMessage("bench.topic", payload,
			queue.WithPriority(queue.PriorityCritical),
			queue.WithMaxAttempts(5),
			queue.WithGroup("group-1", 1),
		)
	}
}
