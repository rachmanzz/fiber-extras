//go:build stress

package queuepostgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuepostgres "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
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

// assertExactlyOnceSettled proves every enqueued ID was settled exactly once,
// and no unexpected (phantom) ID was settled or delivered.
func assertExactlyOnceSettled(t *testing.T, expected map[string]struct{}, settled, deliveries map[string]int) {
	t.Helper()
	for id := range expected {
		if settled[id] == 0 {
			t.Fatalf("message %s was enqueued but never settled (lost)", id)
		}
	}
	for id := range settled {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected message %s was settled (never enqueued)", id)
		}
	}
	for id := range deliveries {
		if _, ok := expected[id]; !ok {
			t.Fatalf("unexpected message %s was delivered (never enqueued)", id)
		}
	}
	if len(settled) != len(expected) {
		t.Fatalf("settled %d distinct messages, expected %d", len(settled), len(expected))
	}
	for id, n := range settled {
		if n != 1 {
			t.Fatalf("message %s settled %d times (want exactly once)", id, n)
		}
	}
	for id, n := range deliveries {
		if n != 1 {
			t.Fatalf("message %s delivered %d times (want exactly once)", id, n)
		}
	}
}

func newPGStress(t *testing.T, lease time.Duration) (*queuepostgres.PostgresDriver, *pgxpool.Pool, string) {
	t.Helper()
	dsn := requireStressEnv(t, "TEST_POSTGRES_DSN")
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("queue-postgres: pool: %v", err)
	}
	table := "stress_pg_" + randSuffix(t)
	var opts []queuepostgres.Option
	opts = append(opts, queuepostgres.WithTableName(table))
	if lease > 0 {
		opts = append(opts, queuepostgres.WithVisibilityTimeout(lease))
	}
	driver := queuepostgres.New(pool, opts...)
	if err := driver.EnsureTable(context.Background()); err != nil {
		t.Fatalf("queue-postgres: EnsureTable: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		pool.Close()
	})
	return driver, pool, table
}

// TestStress_Postgres_ConcurrentNoLossNoDup drives concurrent producers and
// consumers with SELECT ... FOR UPDATE SKIP LOCKED and asserts exactly-once
// settlement of every enqueued ID.
func TestStress_Postgres_ConcurrentNoLossNoDup(t *testing.T) {
	const (
		producers   = 8
		perProducer = 250
		consumers   = 8
		total       = producers * perProducer
	)
	driver, _, _ := newPGStress(t, 60*time.Second)
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
				m := queue.NewMessage("pg.stress", []byte("x"))
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := driver.Enqueue(ctx, m); err != nil {
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
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 32})
				if err != nil {
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					if err := driver.Ack(ctx, d); err != nil {
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

	deadline := time.Now().Add(90 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}

// TestStress_Postgres_CrashReclaimNoLoss claims everything, lets the lease
// expire, then reclaims and settles concurrently: no loss, no duplicate claim,
// and each ID settled exactly once when many workers race to reclaim rows.
func TestStress_Postgres_CrashReclaimNoLoss(t *testing.T) {
	const total = 2000
	lease := 2 * time.Second
	driver, _, _ := newPGStress(t, lease)
	ctx := context.Background()
	sink := &stressErr{}

	expected := make(map[string]struct{}, total)
	for i := 0; i < total; i++ {
		m := queue.NewMessage("pg.reclaim", nil)
		expected[m.ID] = struct{}{}
		if err := driver.Enqueue(ctx, m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	claimedSet := make(map[string]struct{}, total)
	deadline := time.Now().Add(10 * time.Second)
	for len(claimedSet) < total {
		if time.Now().After(deadline) {
			t.Fatalf("phase1 timeout: distinct claimed %d/%d", len(claimedSet), total)
		}
		ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 256})
		if err != nil {
			t.Fatalf("phase1 dequeue: %v", err)
		}
		for _, d := range ds {
			if _, dup := claimedSet[d.Message.ID]; dup {
				t.Fatalf("phase1: %s claimed twice while lease valid", d.Message.ID)
			}
			claimedSet[d.Message.ID] = struct{}{}
		}
		if len(ds) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	time.Sleep(lease + 300*time.Millisecond)

	var (
		mu      sync.Mutex
		seen    = make(map[string]int, total)
		settled = make(map[string]int, total)
	)
	var settledN int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 32})
				if err != nil {
					sink.add(fmt.Errorf("phase2 dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					if err := driver.Ack(ctx, d); err != nil {
						sink.add(fmt.Errorf("phase2 ack %s: %w", d.Message.ID, err))
						continue
					}
					mu.Lock()
					settled[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&settledN, 1)
				}
				if len(ds) == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}

	deadline = time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("phase2 timeout: only %d/%d reclaimed messages settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}

// TestStress_Postgres_ConcurrentEnsureTable calls EnsureTable from many
// goroutines at once on a fresh table; the memoized DDL must be safe.
func TestStress_Postgres_ConcurrentEnsureTable(t *testing.T) {
	dsn := requireStressEnv(t, "TEST_POSTGRES_DSN")
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	table := "stress_ensure_" + randSuffix(t)
	driver := queuepostgres.New(pool, queuepostgres.WithTableName(table))
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := driver.EnsureTable(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent EnsureTable error: %v", err)
	}
}

// TestStress_Postgres_PurgeAckedDuringTraffic runs PurgeAcked concurrently with
// Dequeue/Enqueue and asserts every enqueued ID is settled exactly once, and
// that neither enqueue, dequeue, ack, nor purge silently error.
func TestStress_Postgres_PurgeAckedDuringTraffic(t *testing.T) {
	const total = 800
	driver, _, _ := newPGStress(t, 60*time.Second)
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
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < total/4; j++ {
				m := queue.NewMessage("pg.purge", []byte("x"))
				mu.Lock()
				expected[m.ID] = struct{}{}
				mu.Unlock()
				if err := driver.Enqueue(ctx, m); err != nil {
					sink.add(fmt.Errorf("enqueue %s: %w", m.ID, err))
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 16})
				if err != nil {
					sink.add(fmt.Errorf("dequeue: %w", err))
					return
				}
				for _, d := range ds {
					mu.Lock()
					seen[d.Message.ID]++
					mu.Unlock()
					if err := driver.Ack(ctx, d); err != nil {
						sink.add(fmt.Errorf("ack %s: %w", d.Message.ID, err))
						continue
					}
					mu.Lock()
					settled[d.Message.ID]++
					mu.Unlock()
					atomic.AddInt64(&settledN, 1)
				}
				if len(ds) == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}
	// Concurrent purge churn.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := driver.PurgeAcked(ctx, time.Now()); err != nil {
				sink.add(fmt.Errorf("purge: %w", err))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	deadline := time.Now().Add(30 * time.Second)
	for atomic.LoadInt64(&settledN) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/%d deliveries settled", atomic.LoadInt64(&settledN), total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	sink.fatalIfAny(t)

	mu.Lock()
	defer mu.Unlock()
	assertExactlyOnceSettled(t, expected, settled, seen)
}
