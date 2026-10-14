package queue

import (
	"context"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

// fullDispatcher always rejects Dispatch with ErrQueueFull, emulating a
// saturated worker engine (backpressure).
type fullDispatcher struct{}

func (fullDispatcher) Dispatch(context.Context, worker.Task) error { return worker.ErrQueueFull }
func (fullDispatcher) ExecuteSync(context.Context, worker.Task) error {
	return nil
}

// TestConsumer_FetchAndProcessBackpressureReturnsZero guards against the B5
// hot-loop regression: when the worker engine is full, every delivery is Nacked
// and fetchAndProcess must report 0 dispatched (not batchSize), so the adaptive
// drain loop in pollLoop falls back to waiting on the ticker.
func TestConsumer_FetchAndProcessBackpressureReturnsZero(t *testing.T) {
	driver := NewMemoryDriver()
	router := NewRouter()
	router.Register("audit.job", func(context.Context, *Message, worker.SubWorkerPool) error {
		return nil
	})

	c := NewConsumer(driver, router, fullDispatcher{},
		WithBatchSize(4),
		WithPollInterval(5*time.Millisecond),
	)

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := driver.Enqueue(ctx, NewMessage("audit.job", []byte("x"))); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	if got := c.fetchAndProcess(); got != 0 {
		t.Fatalf("expected 0 dispatched under backpressure, got %d (hot-loop regression)", got)
	}
}
