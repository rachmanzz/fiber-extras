package queue_test

import (
	"context"
	"testing"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/rachmanzz/gosuite/v3/worker"
)

func TestRouter_DuplicateRegistrationPanics(t *testing.T) {
	r := queue.NewRouter()
	h1 := func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error { return nil }
	h2 := func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error { return nil }

	r.Register("test.topic", h1)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate topic registration, got none")
		}
	}()

	r.Register("test.topic", h2)
}

func TestRouter_DuplicateRegistrationWithAllowOverwriteSucceeds(t *testing.T) {
	r := queue.NewRouter()
	h1 := func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error { return nil }
	h2 := func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error { return nil }

	r.Register("test.topic", h1)
	r.Register("test.topic", h2, queue.WithAllowOverwrite(), queue.WithHandlerDriver("custom"))

	route, ok := r.GetRoute("test.topic")
	if !ok {
		t.Fatal("expected topic to be registered")
	}
	if route.DriverName != "custom" {
		t.Fatalf("expected driver custom, got %q", route.DriverName)
	}
}
