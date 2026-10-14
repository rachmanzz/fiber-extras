//go:build stress

package queuenats_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	queuenats "github.com/rachmanzz/gosuite/v3/queue-nats"
)

// stressErr records the first background error so a failure surfaces
// deterministically after all worker goroutines have been joined.
type stressErr struct {
	mu  sync.Mutex
	err error
}

func (s *stressErr) add(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

func (s *stressErr) fatalIfAny(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		t.Fatalf("background stress error: %v", s.err)
	}
}

// TestStress_NATS_ConcurrentProducersConsumers floods JetStream with concurrent
// producers while a consumer drains and acks, asserting every enqueued ID is
// settled exactly once and none delivered twice.
func TestStress_NATS_ConcurrentProducersConsumers(t *testing.T) {
	const (
		producers   = 8
		perProducer = 250
		total       = producers * perProducer
	)
	nc, cfg := startNATSForTest(t)
	defer nc.Close()

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: %v", err)
	}
	defer adapter.Close()
	t.Cleanup(func() {
		if js, jerr := nc.JetStream(); jerr == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()
	var (
		mu       sync.Mutex
		expected = make(map[string]struct{}, total)
		seen     = make(map[string]int, total)
		settled  = make(map[string]int, total)
	)
	var settledN int64
	sink := &stressErr{}

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				m := queue.NewMessage("nats.stress", []byte("x"))
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := adapter.Enqueue(ctx, m); err != nil {
					sink.add(fmt.Errorf("enqueue %s: %w", m.ID, err))
					return
				}
			}
		}()
	}

	deadline := time.Now().Add(60 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled", atomic.LoadInt64(&settledN), total)
		}
		ds, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 128})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		for _, d := range ds {
			mu.Lock()
			seen[d.Message.ID]++
			mu.Unlock()
			if err := adapter.Ack(ctx, d); err != nil {
				sink.add(fmt.Errorf("ack %s: %w", d.Message.ID, err))
				continue
			}
			mu.Lock()
			settled[d.Message.ID]++
			mu.Unlock()
			atomic.AddInt64(&settledN, 1)
		}
		if len(ds) == 0 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	for id := range expected {
		if settled[id] == 0 {
			t.Fatalf("message %s was enqueued but never settled (lost)", id)
		}
	}
	if len(settled) != len(expected) {
		t.Fatalf("settled %d distinct, want %d", len(settled), len(expected))
	}
	for id, n := range settled {
		if n != 1 {
			t.Fatalf("message %s settled %d times (want exactly once)", id, n)
		}
	}
	for id := range seen {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected message %s was delivered (never enqueued)", id)
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s delivered %d times", id, n)
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("delivered %d distinct, want %d", len(seen), len(expected))
	}
}

// TestStress_NATS_NackRedelivery forces redelivery via Nack and asserts every
// enqueued ID is redelivered EXACTLY once (two deliveries total: one nack, one
// ack) and settled exactly once (JetStream at-least-once, no loss, no
// double-settle, no redelivery storm).
func TestStress_NATS_NackRedelivery(t *testing.T) {
	const total = 300
	nc, cfg := startNATSForTest(t)
	defer nc.Close()

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: %v", err)
	}
	defer adapter.Close()
	t.Cleanup(func() {
		if js, jerr := nc.JetStream(); jerr == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()
	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("nats.nack", []byte("x"))
		expected[m.ID] = struct{}{}
		if err := adapter.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var (
		mu       sync.Mutex
		seen     = make(map[string]int, total)
		settled  = make(map[string]int, total)
		settledN int64
	)
	deadline := time.Now().Add(60 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d messages settled", atomic.LoadInt64(&settledN), total)
		}
		ds, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 128})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		for _, d := range ds {
			mu.Lock()
			first := seen[d.Message.ID] == 0
			seen[d.Message.ID]++
			mu.Unlock()
			if first {
				if err := adapter.Nack(ctx, d, nil); err != nil {
					t.Fatalf("nack %s: %v", d.Message.ID, err)
				}
				continue
			}
			if err := adapter.Ack(ctx, d); err != nil {
				t.Fatalf("ack %s: %v", d.Message.ID, err)
			}
			mu.Lock()
			settled[d.Message.ID]++
			mu.Unlock()
			atomic.AddInt64(&settledN, 1)
		}
		time.Sleep(2 * time.Millisecond)
	}

	for id := range expected {
		if settled[id] == 0 {
			t.Fatalf("message %s was enqueued but never settled (lost)", id)
		}
	}
	if len(settled) != len(expected) {
		t.Fatalf("settled %d distinct, want %d", len(settled), len(expected))
	}
	for id, n := range settled {
		if n != 1 {
			t.Fatalf("message %s settled %d times (want exactly once)", id, n)
		}
	}
	for id, n := range seen {
		if n != 2 {
			t.Fatalf("message %s was delivered %d times (want exactly 2: one nack + one ack)", id, n)
		}
	}
	if len(seen) != len(expected) {
		t.Fatalf("delivered %d distinct, want %d", len(seen), len(expected))
	}
}
