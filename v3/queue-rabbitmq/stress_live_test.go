//go:build stress

package queuerabbitmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuerabbitmq "github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq"
)

// requireStressEnv returns the value of a broker env var, failing hard when
// STRESS_REQUIRE_LIVE is set and the var is missing (so a "live" CI job cannot
// pass by silently skipping), otherwise skipping.
func requireStressEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		if os.Getenv("STRESS_REQUIRE_LIVE") != "" {
			t.Fatalf("%s must be set because STRESS_REQUIRE_LIVE is enabled", name)
		}
		t.Skipf("%s not set; skipping live stress test", name)
	}
	return v
}

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

func newRabbitStress(t *testing.T) (*queuerabbitmq.RabbitMQAdapter, string) {
	t.Helper()
	url := requireStressEnv(t, "TEST_RABBITMQ_URL")
	name := "stress_rmq_" + randSuffix(t)
	adapter, err := queuerabbitmq.New(url, queuerabbitmq.WithQueueName(name), queuerabbitmq.WithPrefetch(500))
	if err != nil {
		t.Fatalf("rabbitmq dial: %v", err)
	}
	t.Cleanup(func() {
		_ = adapter.Close()
		if conn, derr := amqp.Dial(url); derr == nil {
			if ch, cerr := conn.Channel(); cerr == nil {
				_, _ = ch.QueueDelete(name, false, false, false)
				_ = ch.Close()
			}
			_ = conn.Close()
		}
	})
	return adapter, name
}

// TestStress_RabbitMQ_ConcurrentProducersConsumers exercises the real consumer
// shape (one Dequeue loop, many concurrent Ack/Nack from worker goroutines) and
// asserts every enqueued ID is Acked exactly once. Multiple concurrent Dequeue
// calls additionally probe whether the shared AMQP channel tolerates
// concurrency.
func TestStress_RabbitMQ_ConcurrentProducersConsumers(t *testing.T) {
	const (
		producers   = 4
		perProducer = 250
		consumers   = 4
		total       = producers * perProducer
	)
	adapter, _ := newRabbitStress(t)
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
				m := queue.NewMessage("rmq.stress", []byte("x"))
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

	stop := make(chan struct{})
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 16})
				if err != nil {
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					// Ack before counting: a failed Ack must not be counted as
					// a settled delivery.
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
		}()
	}

	deadline := time.Now().Add(60 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled (lost messages)", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
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
			t.Fatalf("message %s settled %d times", id, n)
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

// TestStress_RabbitMQ_NackRedeliveryStorm nacks the first delivery of every
// message and asserts each enqueued ID is redelivered EXACTLY once (two
// deliveries total: one nack, one ack) and settled exactly once -- a repeated
// redelivery storm or a double-settle must fail the test.
func TestStress_RabbitMQ_NackRedeliveryStorm(t *testing.T) {
	const total = 400
	adapter, _ := newRabbitStress(t)
	ctx := context.Background()

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("rmq.nack", []byte("x"))
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
		ds, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 32})
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		for _, d := range ds {
			mu.Lock()
			first := seen[d.Message.ID] == 0
			seen[d.Message.ID]++
			mu.Unlock()
			if first {
				// First delivery: reject and requeue to force redelivery.
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

// rabbitMgmtBase resolves the RabbitMQ management API base URL used to inject
// connection failures.
func rabbitMgmtBase(t *testing.T) string {
	t.Helper()
	return requireStressEnv(t, "TEST_RABBITMQ_MGMT_URL")
}

// rabbitMgmtAuth derives management API credentials from the broker URL.
func rabbitMgmtAuth(t *testing.T) (user, pass string) {
	t.Helper()
	broker, err := url.Parse(requireStressEnv(t, "TEST_RABBITMQ_URL"))
	if err != nil {
		t.Fatalf("parse TEST_RABBITMQ_URL: %v", err)
	}
	if broker.User != nil {
		user = broker.User.Username()
		pass, _ = broker.User.Password()
	}
	return user, pass
}

// listRabbitConnections fetches the current AMQP connections from the
// management API.
func listRabbitConnections(t *testing.T, client *http.Client, base, user, pass string) []struct {
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
} {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/api/connections", nil)
	if err != nil {
		t.Fatalf("mgmt list request: %v", err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("mgmt list: %v", err)
	}
	var conns []struct {
		Name     string `json:"name"`
		Protocol string `json:"protocol"`
	}
	err = json.NewDecoder(resp.Body).Decode(&conns)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("mgmt list decode: %v", err)
	}
	return conns
}

// killAllRabbitConnections deletes every AMQP connection through the
// management API, simulating a broker-side connection drop (network outage).
// The management view lags connection events slightly, so it polls until at
// least one AMQP connection shows up before deleting.
func killAllRabbitConnections(t *testing.T, base, user, pass string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}

	var conns []struct {
		Name     string `json:"name"`
		Protocol string `json:"protocol"`
	}
	pollDeadline := time.Now().Add(15 * time.Second)
	for {
		conns = listRabbitConnections(t, client, base, user, pass)
		if len(conns) > 0 || time.Now().After(pollDeadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	amqpCount := 0
	for _, c := range conns {
		// The protocol field is "AMQP 0-9-1" on RabbitMQ >= 3.13 (and
		// "amqp091" on some versions); /api/connections only lists AMQP
		// connections, so match loosely to be version-proof.
		if !strings.Contains(strings.ToLower(c.Protocol), "amqp") {
			continue
		}
		amqpCount++
		dreq, derr := http.NewRequest(http.MethodDelete, base+"/api/connections/"+url.PathEscape(c.Name), nil)
		if derr != nil {
			t.Fatalf("mgmt delete request: %v", derr)
		}
		dreq.SetBasicAuth(user, pass)
		dresp, derr := client.Do(dreq)
		if derr != nil {
			t.Fatalf("mgmt delete %s: %v", c.Name, derr)
		}
		_ = dresp.Body.Close()
		if dresp.StatusCode != http.StatusNoContent && dresp.StatusCode != http.StatusNotFound {
			t.Fatalf("mgmt delete %s: unexpected status %d", c.Name, dresp.StatusCode)
		}
	}
	if amqpCount == 0 {
		t.Fatal("no AMQP connections found to kill; is the adapter connected?")
	}
}

// TestStress_RabbitMQ_ReconnectSurvival kills every AMQP connection twice via
// the management API while consumers drain a queue and asserts that the
// adapter recovers automatically: every enqueued message is settled, none is
// lost, and a publish issued after the second outage is delivered too.
//
// Semantics are at-least-once: deliveries that were unacked when a connection
// died are requeued by the broker and legitimately redelivered, so this test
// asserts "settled at least once per ID" rather than exactly once.
func TestStress_RabbitMQ_ReconnectSurvival(t *testing.T) {
	var total = 200
	adapter, _ := newRabbitStress(t)
	base := rabbitMgmtBase(t)
	user, pass := rabbitMgmtAuth(t)
	ctx := context.Background()

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("rmq.reconnect", []byte("x"))
		expected[m.ID] = struct{}{}
		if err := adapter.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var (
		mu      sync.Mutex
		settled = make(map[string]int, total)
	)
	var settledN int64
	sink := &stressErr{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for c := 0; c < 2; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := adapter.Dequeue(ctx, queue.DequeueRequest{BatchSize: 16})
				if err != nil {
					if errors.Is(err, queuerabbitmq.ErrReconnecting) {
						time.Sleep(2 * time.Millisecond)
						continue
					}
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
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
		}()
	}

	waitFor := func(n int64, phase string) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for atomic.LoadInt64(&settledN) < n {
			if time.Now().After(deadline) {
				t.Fatalf("timeout %s: only %d/%d settled", phase, atomic.LoadInt64(&settledN), n)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	waitFor(20, "before first kill")
	killAllRabbitConnections(t, base, user, pass)
	waitFor(60, "between kills")
	killAllRabbitConnections(t, base, user, pass)

	// Publish through the recovered connection: proves the supervisor restored
	// a channel that accepts new work, not just existing in-flight deliveries.
	recMsg := queue.NewMessage("rmq.reconnect", []byte("x"))
	expected[recMsg.ID] = struct{}{}
	total++
	recDeadline := time.Now().Add(30 * time.Second)
	for {
		err := adapter.Enqueue(ctx, recMsg)
		if err == nil {
			break
		}
		if errors.Is(err, queuerabbitmq.ErrReconnecting) && time.Now().Before(recDeadline) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		t.Fatalf("enqueue after second outage: %v", err)
	}

	waitFor(int64(total), "after second kill")
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	for id := range expected {
		if settled[id] == 0 {
			t.Fatalf("message %s was enqueued but never settled (lost across reconnects)", id)
		}
	}
	if len(settled) != len(expected) {
		t.Fatalf("settled %d distinct messages, want %d", len(settled), len(expected))
	}
}
