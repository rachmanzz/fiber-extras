package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

// Hold re-enqueued deliveries until the test explicitly makes them visible.
// This models timer/broker delays extending beyond the advertised backoff.
type heldGroupRetryDriver struct {
	*MemoryDriver
	delayed []*Message
}

func (d *heldGroupRetryDriver) Reenqueue(_ context.Context, _ *Delivery, m *Message, _ time.Time) error {
	d.delayed = append(d.delayed, m)
	return nil
}
func expireGroupBackoff(c *Consumer, group string) {
	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	entry := c.groupLocks[group]
	entry.blockedUntil = time.Now().Add(-time.Second)
	for i := range entry.unsettled {
		if entry.unsettled[i].id == entry.retryHeadID && entry.unsettled[i].topic == entry.retryHeadTopic {
			entry.unsettled[i].blockedUntil = entry.blockedUntil
		}
	}
}
func TestGroupRetryHeadSurvivesExpiredBackoff(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "dlq"}[terminal], func(t *testing.T) {
			d := &heldGroupRetryDriver{MemoryDriver: NewMemoryDriver()}
			router := NewRouter()
			disp := &budgetDispatcher{}
			attempts := 0
			var order []int
			router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
				order = append(order, m.GroupOrder)
				if m.GroupOrder == 1 {
					attempts++
					if attempts == 1 || terminal {
						return errors.New("head failure")
					}
				}
				return nil
			})
			c := NewConsumer(d, router, disp, WithBatchSize(1), WithRetryPolicy(RetryPolicy{MaxAttempts: 2, Initial: time.Hour, Max: time.Hour}), WithLogger(worker.NopLogger()))
			defer c.Stop()
			head := NewMessage("x", nil, WithGroup("g", 1), WithMaxAttempts(2))
			later := NewMessage("x", nil, WithGroup("g", 2))
			d.Enqueue(context.Background(), head)
			c.fetchAndProcess()
			runBudgetTasks(t, disp)
			expireGroupBackoff(c, "g")
			// Head remains invisible while the later delivery returns from the broker.
			d.Enqueue(context.Background(), later)
			c.fetchAndProcess()
			runBudgetTasks(t, disp)
			if len(order) != 1 {
				t.Fatalf("later member overtook retry: %v", order)
			}
			c.groupMu.Lock()
			retryID := c.groupLocks["g"].retryHeadID
			c.groupMu.Unlock()
			if retryID != head.ID {
				t.Fatal("retry barrier expired before head returned")
			}
			d.Enqueue(context.Background(), head)
			c.fetchAndProcess()
			runBudgetTasks(t, disp)
			// Terminal DLQ may itself appear on the same group; settle it first.
			if terminal {
				c.fetchAndProcess()
				runBudgetTasks(t, disp)
			}
			d.Enqueue(context.Background(), later)
			c.fetchAndProcess()
			runBudgetTasks(t, disp)
			if len(order) != 3 || order[0] != 1 || order[1] != 1 || order[2] != 2 {
				t.Fatalf("order=%v", order)
			}
			c.groupMu.Lock()
			entries := len(c.groupLocks)
			c.groupMu.Unlock()
			if entries != 0 {
				t.Fatalf("settled group retained %d entries", entries)
			}
		})
	}
}

type failedDLQGroupDriver struct{ *MemoryDriver }

func (d failedDLQGroupDriver) Enqueue(ctx context.Context, m *Message) error {
	if m.Topic == "x:dlq" {
		return errors.New("DLQ publish failed")
	}
	return d.MemoryDriver.Enqueue(ctx, m)
}
func TestGroupDLQPublishFailureKeepsHead(t *testing.T) {
	d := failedDLQGroupDriver{NewMemoryDriver()}
	router := NewRouter()
	disp := &budgetDispatcher{}
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { return ErrNoHandlerFound })
	c := NewConsumer(d, router, disp, WithLogger(worker.NopLogger()))
	defer c.Stop()
	head := NewMessage("x", nil, WithGroup("g", 1))
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	c.groupMu.Unlock()
	if entry == nil || entry.retryHeadID != head.ID {
		t.Fatal("failed DLQ publication released unsettled head")
	}
}

type failedAckGroupDriver struct {
	*heldGroupRetryDriver
	failed bool
}

func (d *failedAckGroupDriver) Ack(context.Context, *Delivery) error {
	if !d.failed {
		d.failed = true
		return errors.New("ack unavailable")
	}
	return nil
}
func TestGroupAckFailureDoesNotReleaseHead(t *testing.T) {
	d := &failedAckGroupDriver{heldGroupRetryDriver: &heldGroupRetryDriver{MemoryDriver: NewMemoryDriver()}}
	router := NewRouter()
	disp := &budgetDispatcher{}
	var order []int
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		order = append(order, m.GroupOrder)
		return nil
	})
	b := NewMemoryBudget(2048)
	c := newBudgetConsumer(d, router, disp, b)
	defer c.Stop()
	head, later := NewMessage("x", nil, WithGroup("g", 1)), NewMessage("x", nil, WithGroup("g", 2))
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 1 {
		t.Fatalf("later delivery ran after failed ack: %v", order)
	}
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 3 || order[0] != 1 || order[1] != 1 || order[2] != 2 {
		t.Fatalf("order=%v", order)
	}
	if b.Used() != 0 {
		t.Fatalf("retry group leaked %d bytes", b.Used())
	}
}
