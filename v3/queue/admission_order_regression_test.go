package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

type auditAdmissionPollDriver struct {
	*MemoryDriver
	oversizedSeen chan struct{}
	once          sync.Once
}

func (d *auditAdmissionPollDriver) Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error) {
	ds, err := d.MemoryDriver.Dequeue(ctx, req)
	if len(ds) > 0 && len(ds[0].Message.Payload) > 4096 {
		d.once.Do(func() { close(d.oversizedSeen) })
	}
	return ds, err
}
func TestOversizeSettlementDoesNotBlockIndependentPolling(t *testing.T) {
	d := &auditAdmissionPollDriver{MemoryDriver: NewMemoryDriver(), oversizedSeen: make(chan struct{})}
	router := NewRouter()
	entered, release, independent := make(chan struct{}), make(chan struct{}), make(chan struct{})
	router.Register("group", func(ctx context.Context, _ *Message, _ worker.SubWorkerPool) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	router.Register("independent", func(context.Context, *Message, worker.SubWorkerPool) error { close(independent); return nil })
	engine := worker.New(worker.WithLogger(worker.NopLogger()), worker.WithMaxWorkers(2)).Start()
	defer engine.Shutdown()
	c := newBudgetConsumer(d, router, engine, NewMemoryBudget(4096), WithMemoryEstimate(512), WithPollInterval(time.Millisecond))
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("group", nil, WithGroup("g", 1)))
	c.Start()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("group did not start")
	}
	d.Enqueue(context.Background(), NewMessage("oversized", make([]byte, 8192), WithGroup("g", 2)))
	d.Enqueue(context.Background(), NewMessage("independent", nil))
	select {
	case <-d.oversizedSeen:
	case <-time.After(time.Second):
		t.Fatal("oversized not fetched")
	}
	select {
	case <-independent:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("oversized settlement blocked independent polling")
	}
	// The first group's handler is still held here; the independent handler
	// must complete before it is released.
	close(release)

}

type auditMultipleHeadDriver struct {
	*heldGroupRetryDriver
	rejected     []*Message
	dlqAvailable bool
}

func (d *auditMultipleHeadDriver) Enqueue(ctx context.Context, m *Message) error {
	if m.Topic == "x:dlq" && !d.dlqAvailable {
		return errors.New("DLQ unavailable")
	}
	return d.MemoryDriver.Enqueue(ctx, m)
}
func (d *auditMultipleHeadDriver) Nack(_ context.Context, del *Delivery, _ error) error {
	d.rejected = append(d.rejected, del.Message)
	return nil
}
func TestSecondUnsettledAdmissionRemainsBarrier(t *testing.T) {
	d := &auditMultipleHeadDriver{heldGroupRetryDriver: &heldGroupRetryDriver{MemoryDriver: NewMemoryDriver()}}
	router := NewRouter()
	disp := &budgetDispatcher{}
	attempts := 0
	var ran []int
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		ran = append(ran, m.GroupOrder)
		if m.GroupOrder == 1 {
			attempts++
			if attempts == 1 {
				return errors.New("retry")
			}
		}
		return nil
	})
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(1024), WithMemoryEstimate(512))
	defer c.Stop()
	first := NewMessage("x", nil, WithGroup("g", 1))
	second := NewMessage("x", make([]byte, 2048), WithGroup("g", 2))
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(d.rejected) != 1 || d.rejected[0] != second {
		t.Fatal("failed admission not held for redelivery")
	}
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	third := NewMessage("x", nil, WithGroup("g", 3))
	d.Enqueue(context.Background(), third)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(ran) != 2 || ran[0] != 1 || ran[1] != 1 {
		t.Fatalf("member 3 overtook unsettled member 2: %v", ran)
	}
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	promoted := entry != nil && entry.retryHeadID == second.ID
	c.groupMu.Unlock()
	if !promoted {
		t.Fatal("next unsettled head was not promoted")
	}
	d.dlqAvailable = true
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	// Remove the DLQ copy from this in-memory test broker; it is a separate job.
	ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
	if len(ds) != 1 || ds[0].Message.Topic != "x:dlq" {
		t.Fatal("second member was not terminally settled")
	}
	d.Enqueue(context.Background(), third)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(ran) != 3 || ran[2] != 3 {
		t.Fatalf("group did not resume after member 2 settlement: %v", ran)
	}
}

func TestOversizeSettlementSlotIsBoundedAndReusable(t *testing.T) {
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	c := newBudgetConsumer(d, NewRouter(), disp, NewMemoryBudget(1024), WithMemoryEstimate(512))
	defer c.Stop()
	for i := 1; i <= 2; i++ {
		d.Enqueue(context.Background(), NewMessage("x", make([]byte, 2048), WithGroup("g", i)))
		c.fetchAndProcess()
	}
	if len(c.settlementSlots) != 1 || len(disp.tasks) != 1 {
		t.Fatal("oversized retention was not bounded")
	}
	c.groupMu.Lock()
	pending := len(c.groupLocks["g"].unsettled)
	c.groupMu.Unlock()
	if pending != 2 {
		t.Fatalf("unsettled identities=%d want=2", pending)
	}
	runBudgetTasks(t, disp)
	if len(c.settlementSlots) != 0 {
		t.Fatal("settlement did not release slot")
	}
	// The second oversized message was Nacked and remains ahead of the DLQ copy.
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	c.groupMu.Lock()
	groups := len(c.groupLocks)
	c.groupMu.Unlock()
	if len(c.settlementSlots) != 0 || groups != 0 {
		t.Fatal("settlement slot/barrier did not recover")
	}
}
func TestOversizeSettlementDispatchRejectionReleasesSlot(t *testing.T) {
	for _, group := range []string{"", "g"} {
		t.Run(group, func(t *testing.T) {
			d := NewMemoryDriver()
			disp := &budgetDispatcher{reject: true}
			c := newBudgetConsumer(d, NewRouter(), disp, NewMemoryBudget(1024), WithMemoryEstimate(512))
			defer c.Stop()
			d.Enqueue(context.Background(), NewMessage("x", make([]byte, 2048), WithGroup(group, 1)))
			c.fetchAndProcess()
			if len(c.settlementSlots) != 0 {
				t.Fatal("rejected dispatch retained oversized slot")
			}
			ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
			if len(ds) != 1 || ds[0].Message.Topic != "x" {
				t.Fatal("rejected dispatch lost original delivery")
			}
		})
	}
}

type orderedSettlementDriver struct {
	*MemoryDriver
	events *[]int
}

func (d orderedSettlementDriver) Enqueue(ctx context.Context, m *Message) error {
	if m.Topic == "x:dlq" {
		*d.events = append(*d.events, 2)
	}
	return d.MemoryDriver.Enqueue(ctx, m)
}
func TestEarlierQueuedMemberRunsBeforeOversizeSettlement(t *testing.T) {
	var events []int
	d := orderedSettlementDriver{MemoryDriver: NewMemoryDriver(), events: &events}
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { events = append(events, 1); return nil })
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(4096), WithMemoryEstimate(512))
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup("g", 1)))
	c.fetchAndProcess()
	d.Enqueue(context.Background(), NewMessage("x", make([]byte, 8192), WithGroup("g", 2)))
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(events) != 2 || events[0] != 1 || events[1] != 2 {
		t.Fatalf("events=%v", events)
	}
}

func TestDefaultOrderQueuedMemberPrecedesOversizeBarriers(t *testing.T) {
	var events []int
	d := orderedSettlementDriver{MemoryDriver: NewMemoryDriver(), events: &events}
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { events = append(events, 1); return nil })
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(4096), WithMemoryEstimate(512))
	defer c.Stop()
	d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup("g")))
	c.fetchAndProcess()
	// Both later messages use the default GroupOrder=0. One is queued for
	// settlement; the other is Nacked because the bounded slot is occupied.
	for i := 0; i < 2; i++ {
		d.Enqueue(context.Background(), NewMessage("x", make([]byte, 8192), WithGroup("g")))
		c.fetchAndProcess()
	}
	runBudgetTasks(t, disp)
	if len(events) != 2 || events[0] != 1 || events[1] != 2 {
		t.Fatalf("earlier batch was blocked by later barrier: %v", events)
	}
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(events) != 3 || events[2] != 2 {
		t.Fatalf("second settlement did not recover: %v", events)
	}
}
