package queuenats_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/rachmanzz/gosuite/v3/queue"
	queuenats "github.com/rachmanzz/gosuite/v3/queue-nats"
	"github.com/rachmanzz/gosuite/v3/worker"
)

func randSuffix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("failed to generate random suffix: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func newLiveMsg(topic string, prio queue.Priority) *queue.Message {
	return queue.NewMessage(topic, []byte(`{"live":true}`), queue.WithPriority(prio))
}

func assertOrder(t *testing.T, deliveries []*queue.Delivery, wantIDs []string) {
	t.Helper()
	for i, want := range wantIDs {
		if deliveries[i].Message.ID != want {
			t.Fatalf("delivery %d: expected message %s, got %s", i, want, deliveries[i].Message.ID)
		}
	}
}

// dequeueUntil polls the adapter until at least want deliveries are available
// or the timeout elapses, returning whatever was fetched on the last call.
func dequeueUntil(t *testing.T, ctx context.Context, adapter queue.Adapter, timeout time.Duration, want int) []*queue.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		out []*queue.Delivery
		err error
	)
	for {
		out, err = adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
		if err != nil {
			t.Fatalf("dequeue failed: %v", err)
		}
		if len(out) >= want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newNATSTestConfig(t *testing.T) queuenats.Config {
	t.Helper()
	cfg := queuenats.DefaultConfig()
	cfg.Stream = "FIBER_TEST_" + randSuffix(t)
	cfg.Storage = "memory"
	return cfg
}

// startNATSForTest connects to the external NATS broker when TEST_NATS_URL is set,
// otherwise it boots an in-process embedded NATS server with JetStream enabled so
// the live test suite runs fully self-contained with zero external dependencies.
func startNATSForTest(t *testing.T) (*nats.Conn, queuenats.Config) {
	t.Helper()

	if url := os.Getenv("TEST_NATS_URL"); url != "" {
		nc, err := nats.Connect(url, nats.Timeout(10*time.Second))
		if err != nil {
			t.Fatalf("queue-nats: failed to connect to external NATS at %s: %v", url, err)
		}
		return nc, newNATSTestConfig(t)
	}

	srv, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
	})
	if err != nil {
		t.Fatalf("queue-nats: failed to create embedded NATS server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("queue-nats: embedded NATS server did not become ready in time")
	}
	nc, err := nats.Connect(srv.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("queue-nats: failed to connect to embedded NATS server: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Shutdown()
	})
	return nc, newNATSTestConfig(t)
}

func TestNATSAdapter_Live_FullLoop(t *testing.T) {
	nc, cfg := startNATSForTest(t)

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: failed to create adapter: %v", err)
	}
	defer adapter.Close()

	// Delete the stream so nothing lingers on long-lived brokers.
	t.Cleanup(func() {
		if js, err := nc.JetStream(); err == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()

	low := newLiveMsg("email.send", queue.PriorityLow)
	def := newLiveMsg("email.send", queue.PriorityDefault)
	high := newLiveMsg("email.send", queue.PriorityHigh)
	crit := newLiveMsg("email.send", queue.PriorityCritical)

	// Enqueue in ascending priority so any ordering advantage can only come from the broker.
	for _, m := range []*queue.Message{low, def, high, crit} {
		if err := adapter.Enqueue(ctx, m); err != nil {
			t.Fatalf("queue-nats: enqueue %s failed: %v", m.ID, err)
		}
	}

	deliveries, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
	if err != nil {
		t.Fatalf("queue-nats: dequeue failed: %v", err)
	}
	if len(deliveries) != 4 {
		t.Fatalf("queue-nats: expected 4 deliveries, got %d", len(deliveries))
	}
	assertOrder(t, deliveries, []string{crit.ID, high.ID, def.ID, low.ID})

	// Ack the critical message: it must never be redelivered.
	if err := adapter.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-nats: ack failed: %v", err)
	}

	// Nack the default message: it must be redelivered on the next Dequeue.
	if err := adapter.Nack(ctx, deliveries[2], errors.New("transient failure")); err != nil {
		t.Fatalf("queue-nats: nack failed: %v", err)
	}

	redelivered := dequeueUntil(t, ctx, adapter, 2*time.Second, 1)
	if len(redelivered) != 1 || redelivered[0].Message.ID != def.ID {
		t.Fatalf("queue-nats: expected redelivery of nack'd message %s, got %d delivery(ies)", def.ID, len(redelivered))
	}
	if err := adapter.Ack(ctx, redelivered[0]); err != nil {
		t.Fatalf("queue-nats: ack after redelivery failed: %v", err)
	}
}

func TestNATSAdapter_Live_Dedup(t *testing.T) {
	nc, cfg := startNATSForTest(t)

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: failed to create adapter: %v", err)
	}
	defer adapter.Close()

	t.Cleanup(func() {
		if js, err := nc.JetStream(); err == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()

	msg := newLiveMsg("dedup.check", queue.PriorityDefault)
	for i := 0; i < 2; i++ {
		if err := adapter.Enqueue(ctx, msg); err != nil {
			t.Fatalf("queue-nats: enqueue attempt %d failed: %v", i+1, err)
		}
	}

	deliveries := dequeueUntil(t, ctx, adapter, 2*time.Second, 1)
	if len(deliveries) != 1 {
		t.Fatalf("queue-nats: dedup window failed: expected exactly 1 delivery, got %d", len(deliveries))
	}
	if deliveries[0].Message.ID != msg.ID {
		t.Fatalf("queue-nats: expected message %s, got %s", msg.ID, deliveries[0].Message.ID)
	}
	if err := adapter.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("queue-nats: ack failed: %v", err)
	}
}

// TestNATSAdapter_Live_UndecodablePayloadSurfacesPoison verifies that a payload
// that cannot be decoded is not silently terminated: it is surfaced back as a
// poison message (queue.PoisonTopic) so the consumer can route it to the DLQ.
func TestNATSAdapter_Live_UndecodablePayloadSurfacesPoison(t *testing.T) {
	nc, cfg := startNATSForTest(t)

	adapter, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatalf("queue-nats: failed to create adapter: %v", err)
	}
	defer adapter.Close()

	t.Cleanup(func() {
		if js, err := nc.JetStream(); err == nil {
			_ = js.DeleteStream(cfg.Stream)
		}
	})

	ctx := context.Background()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("queue-nats: failed to obtain jetstream context: %v", err)
	}
	subject := cfg.Stream + ".default.general.audit"
	sourceID := "corrupt-" + randSuffix(t)
	corrupt := &nats.Msg{Subject: subject, Data: []byte("this is not json"), Header: nats.Header{}}
	corrupt.Header.Set(nats.MsgIdHdr, sourceID)
	corrupt.Header.Set("fiber-id", sourceID)
	if _, err := js.PublishMsg(corrupt); err != nil {
		t.Fatalf("queue-nats: failed to publish corrupt payload: %v", err)
	}

	var poison *queue.Delivery
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && poison == nil {
		batch, dErr := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 10})
		if dErr != nil {
			t.Fatalf("queue-nats: dequeue failed: %v", dErr)
		}
		for _, d := range batch {
			if d.Message != nil && d.Message.Topic == queue.PoisonTopic {
				poison = d
				break
			}
			_ = adapter.Ack(ctx, d)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if poison == nil {
		t.Fatal("queue-nats: corrupt payload was not surfaced as a poison message")
	}
	if poison.Message.ID == sourceID || poison.Message.Headers["source_message_id"] != sourceID || poison.Message.Headers["source_publication_id"] != sourceID || string(poison.Message.Payload) != string(corrupt.Data) {
		t.Fatal("poison copy collided with source dedup identity or lost original metadata/payload")
	}
	if poison.Message.Headers["decode_error"] == "" {
		t.Fatal("queue-nats: expected decode_error header on poison message")
	}
	if poison.Message.Headers["decode_adapter"] != adapter.Name() {
		t.Fatalf("queue-nats: expected decode_adapter=%q, got %q", adapter.Name(), poison.Message.Headers["decode_adapter"])
	}
	if err := adapter.Ack(ctx, poison); err != nil {
		t.Fatalf("queue-nats: ack of poison failed: %v", err)
	}
}

func TestNATSAdapter_Live_ImmediateRetrySurvivesDedup(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	m := queue.NewMessage("retry.immediate", []byte("payload"))
	if err := a.Enqueue(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	ds := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(ds) != 1 {
		t.Fatal("original not received")
	}
	original := ds[0]
	raw := original.Token.(*nats.Msg)
	original.Message.Attempt++
	if err := a.Reenqueue(context.Background(), original, original.Message, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Replay publication after an ambiguous Ack, using a fresh delivery token.
	replay := &nats.Msg{Header: raw.Header, Data: raw.Data, Reply: raw.Reply, Sub: raw.Sub}
	if err := a.Reenqueue(context.Background(), &queue.Delivery{Message: original.Message, Token: replay}, original.Message, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	retry := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(retry) != 1 || retry[0].Message.ID != m.ID || retry[0].Message.Attempt != 1 {
		t.Fatalf("retry lost or incorrect: %+v", retry)
	}
	if retry[0].Token.(*nats.Msg).Header.Get(nats.MsgIdHdr) == raw.Header.Get(nats.MsgIdHdr) {
		t.Fatal("retry shares original dedup identity")
	}
	js, _ := nc.JetStream()
	info, err := js.StreamInfo(cfg.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("replayed publication should deduplicate retry only, stored=%d", info.State.Msgs)
	}
	if err := a.Ack(context.Background(), retry[0]); err != nil {
		t.Fatal(err)
	}
}

func TestNATSAdapter_Live_DelayedRetrySurvivesAdapterRestart(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Enqueue(context.Background(), queue.NewMessage("retry.delayed", []byte("payload"))); err != nil {
		t.Fatal(err)
	}
	ds := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(ds) != 1 {
		t.Fatal("original not received")
	}
	ds[0].Message.Attempt = 1
	due := time.Now().Add(400 * time.Millisecond)
	if err := a.Reenqueue(context.Background(), ds[0], ds[0].Message, due); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	a.Close()
	b, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	early, err := b.Dequeue(context.Background(), queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(early) != 0 {
		t.Fatal("handler retry exposed before due time")
	}
	b.Close()
	c, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	retry := dequeueUntil(t, context.Background(), c, 3*time.Second, 1)
	if len(retry) != 1 || retry[0].Message.Attempt != 1 || string(retry[0].Message.Payload) != "payload" {
		t.Fatalf("durable delayed retry lost: %+v", retry)
	}
	if time.Now().Before(due) {
		t.Fatal("retry delivered early after restart")
	}
	if err := c.Ack(context.Background(), retry[0]); err != nil {
		t.Fatal(err)
	}
}

func TestNATSAdapter_Live_BackpressureDoesNotSpendHandlerAttempts(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	cfg.AckWait = 500 * time.Millisecond
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Enqueue(context.Background(), queue.NewMessage("job", []byte("payload"))); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []error{queue.ErrMemoryBudgetExceeded, worker.ErrQueueFull} {
		ds := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
		if len(ds) != 1 || ds[0].Message.Attempt != 0 {
			t.Fatalf("backpressure consumed attempt: %+v", ds)
		}
		if err := a.Nack(context.Background(), ds[0], reason); err != nil {
			t.Fatal(err)
		}
	}
	held := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(held) != 1 {
		t.Fatal("no delivery to test lease recovery")
	}
	time.Sleep(600 * time.Millisecond)
	recovered := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(recovered) != 1 || recovered[0].Message.Attempt != 0 {
		t.Fatal("lease recovery consumed handler attempt")
	}
	// The group-deferral path leaves the envelope unchanged, including Attempt.
	if err := a.Reenqueue(context.Background(), recovered[0], recovered[0].Message, time.Now().Add(60*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	deferred := dequeueUntil(t, context.Background(), a, 3*time.Second, 1)
	if len(deferred) != 1 || deferred[0].Message.Attempt != 0 {
		t.Fatalf("group deferral result: count=%d deliveries=%+v", len(deferred), deferred)
	}
	if err := a.Nack(context.Background(), deferred[0], worker.ErrQueueFull); err != nil {
		t.Fatal(err)
	}
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	defer engine.Shutdown()
	router := queue.NewRouter()
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	dlq := make(chan struct{}, 1)
	router.Register("job", func(_ context.Context, m *queue.Message, _ worker.SubWorkerPool) error {
		n := calls.Add(1)
		if n == 1 {
			return errors.New("transient handler failure")
		}
		if n == 2 {
			done <- struct{}{}
		}
		return nil
	})
	router.Register("job:dlq", func(context.Context, *queue.Message, worker.SubWorkerPool) error { dlq <- struct{}{}; return nil })
	consumer := queue.NewConsumer(a, router, engine, queue.WithPollInterval(5*time.Millisecond), queue.WithMemoryBudget(queue.NewMemoryBudget(4096)), queue.WithMemoryEstimate(512), queue.WithLogger(worker.NopLogger()), queue.WithRetryPolicy(queue.RetryPolicy{Initial: 10 * time.Millisecond, Max: 10 * time.Millisecond, MaxAttempts: 3}))
	consumer.Start()
	defer consumer.Stop()
	select {
	case <-done:
	case <-dlq:
		t.Fatal("backpressure exhausted retry limit before second handler attempt")
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not recover")
	}
	js, _ := nc.JetStream()
	deadline := time.Now().Add(time.Second)
	for {
		info, err := js.StreamInfo(cfg.Stream)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successful handler was not settled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls=%d, want 2", calls.Load())
	}
}

func TestNATSAdapter_Live_DeadlinePollsReadyLowerPriorities(t *testing.T) {
	for _, tc := range []struct {
		priority queue.Priority
		timeout  time.Duration
	}{
		{queue.PriorityHigh, 250 * time.Millisecond},
		{queue.PriorityDefault, 250 * time.Millisecond},
		{queue.PriorityLow, 250 * time.Millisecond},
		{queue.PriorityLow, 50 * time.Millisecond},
	} {
		t.Run(fmt.Sprintf("%d_%s", tc.priority, tc.timeout), func(t *testing.T) {
			nc, cfg := startNATSForTest(t)
			a, err := queuenats.NewWithConfig(nc, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			m := queue.NewMessage("deadline.ready", []byte("payload"), queue.WithPriority(tc.priority))
			if err := a.Enqueue(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			ds, err := a.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
			if err != nil || len(ds) != 1 || ds[0].Message.ID != m.ID {
				t.Fatalf("ready message not received: err=%v deliveries=%+v", err, ds)
			}
			if err := a.Ack(context.Background(), ds[0]); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNATSAdapter_Live_DeadlineReturnsPartialBatch(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	critical := queue.NewMessage("partial.critical", nil, queue.WithPriority(queue.PriorityCritical))
	low := queue.NewMessage("partial.low", nil, queue.WithPriority(queue.PriorityLow))
	for _, m := range []*queue.Message{critical, low} {
		if err := a.Enqueue(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	ds, err := a.Dequeue(ctx, queue.DequeueRequest{BatchSize: 5})
	if err != nil || len(ds) != 2 {
		t.Fatalf("partial batch lost: err=%v deliveries=%+v", err, ds)
	}
	assertOrder(t, ds, []string{critical.ID, low.ID})
	for _, d := range ds {
		if err := a.Ack(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNATSAdapter_Live_CallerCancellationAndExpiredDeadline(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if _, err := a.Dequeue(expired, queue.DequeueRequest{BatchSize: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired deadline ignored: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := a.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1}); done <- err }()
	// Cancel while the first empty priority fetch is waiting.
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation replaced by internal timeout: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not respond to cancellation")
	}
}

type partialErrorNATSDriver struct {
	queue.Adapter
	injected bool
}

func (d *partialErrorNATSDriver) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	if !d.injected {
		first := req
		first.BatchSize = 1
		ds, err := d.Adapter.Dequeue(ctx, first)
		if len(ds) > 0 {
			d.injected = true
			return ds, errors.Join(err, errors.New("injected later-priority fetch error"))
		}
		return ds, err
	}
	return d.Adapter.Dequeue(ctx, req)
}
func TestNATSAdapter_Live_CoreProcessesPartialBatchWithError(t *testing.T) {
	nc, cfg := startNATSForTest(t)
	a, err := queuenats.NewWithConfig(nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	for i, priority := range []queue.Priority{queue.PriorityCritical, queue.PriorityDefault} {
		m := queue.NewMessage("partial.group", nil, queue.WithGroup("g", i+1), queue.WithPriority(priority))
		if err := a.Enqueue(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	driver := &partialErrorNATSDriver{Adapter: a}
	engine := worker.New(worker.WithLogger(worker.NopLogger())).Start()
	defer engine.Shutdown()
	router := queue.NewRouter()
	ran := make(chan int, 2)
	router.Register("partial.group", func(_ context.Context, m *queue.Message, _ worker.SubWorkerPool) error {
		ran <- m.GroupOrder
		return nil
	})
	b := queue.NewMemoryBudget(4096)
	c := queue.NewConsumer(driver, router, engine, queue.WithMemoryBudget(b), queue.WithMemoryEstimate(512), queue.WithPollInterval(5*time.Millisecond), queue.WithLogger(worker.NopLogger()))
	c.Start()
	defer c.Stop()
	for want := 1; want <= 2; want++ {
		select {
		case got := <-ran:
			if got != want {
				t.Fatalf("partial head bypassed: got=%d want=%d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("partial batch did not make progress")
		}
	}
	js, _ := nc.JetStream()
	deadline := time.Now().Add(time.Second)
	for {
		info, err := js.StreamInfo(cfg.Stream)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial batch deliveries were not settled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Stop()
	if b.Used() != 0 {
		t.Fatalf("partial batch memory leak: %d", b.Used())
	}
}
