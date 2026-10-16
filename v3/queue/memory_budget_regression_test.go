package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryExplicitEstimateRequiresFullBudget(t *testing.T) {
	b := NewMemoryBudget(1024)
	driver := &countingBudgetDriver{MemoryDriver: NewMemoryDriver()}
	disp := &budgetDispatcher{}
	c := newBudgetConsumer(driver, NewRouter(), disp, b, WithMemoryEstimate(4096))
	defer c.Stop()
	driver.Enqueue(context.Background(), NewMessage("x", nil))
	if n := c.fetchAndProcess(); n != 0 || b.Used() != 0 || driver.dequeues.Load() != 0 || len(disp.tasks) != 0 {
		t.Fatalf("oversized estimate admitted: count=%d used=%d dequeues=%d", n, b.Used(), driver.dequeues.Load())
	}
	if err := b.SetLimit(4096); err != nil {
		t.Fatal(err)
	}
	if n := c.fetchAndProcess(); n != 1 || b.Used() != 4096 {
		t.Fatalf("admission did not recover: count=%d used=%d", n, b.Used())
	}
	runBudgetTasks(t, disp)
	if err := b.SetLimit(1024); err != nil {
		t.Fatal(err)
	}
	driver.Enqueue(context.Background(), NewMessage("x", nil))
	before := driver.dequeues.Load()
	if n := c.fetchAndProcess(); n != 0 || driver.dequeues.Load() != before {
		t.Fatal("estimate clipped after budget shrink")
	}
}
func TestMemoryDefaultEstimateAdaptsToSmallBudget(t *testing.T) {
	b := NewMemoryBudget(1024)
	driver := NewMemoryDriver()
	disp := &budgetDispatcher{}
	c := NewConsumer(driver, NewRouter(), disp, WithMemoryBudget(b))
	defer c.Stop()
	driver.Enqueue(context.Background(), NewMessage("x", nil))
	if n := c.fetchAndProcess(); n != 1 || b.Used() != 1024 {
		t.Fatal("implicit estimate did not adapt")
	}
	runBudgetTasks(t, disp)
}

type auditReleaseOrderDriver struct {
	Driver
	budget   *MemoryBudget
	enqueued chan struct{}
	once     sync.Once
}

func (d *auditReleaseOrderDriver) Enqueue(ctx context.Context, m *Message) error {
	err := d.Driver.Enqueue(ctx, m)
	d.once.Do(func() { d.budget.mu.Lock(); close(d.enqueued) })
	return err
}
func TestMemoryStopWaitsForReclaim(t *testing.T) {
	b := NewMemoryBudget(1024)
	r, _ := b.TryClaim(1024)
	scope := newMemoryScope(r)
	driver := &auditReleaseOrderDriver{Driver: NewMemoryDriver(), budget: b, enqueued: make(chan struct{})}
	c := newBudgetConsumer(driver, NewRouter(), &budgetDispatcher{}, b)
	del := &Delivery{Message: NewMessage("x", nil)}
	if err := c.delayDelivery(memoryContext(context.Background(), scope), del, time.Millisecond, nil); err != nil {
		t.Fatal(err)
	}
	scope.finish() // Only the timer now owns this claim.
	select {
	case <-driver.enqueued:
	case <-time.After(time.Second):
		t.Fatal("timer did not enqueue")
	}
	stopped := make(chan struct{})
	go func() { c.Stop(); close(stopped) }()

	// Wait until Stop reaches cancellation immediately before its join barriers.
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		b.mu.Unlock()
		t.Fatal("stop did not start")
	}
	select {
	case <-stopped:
		b.mu.Unlock()
		t.Fatal("Stop returned before retry reservation cleanup")
	case <-time.After(30 * time.Millisecond):
	}
	b.mu.Unlock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after reclaim")
	}
	if b.Used() != 0 {
		t.Fatalf("Stop returned with %d bytes still claimed", b.Used())
	}
}
