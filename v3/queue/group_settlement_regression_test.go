package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

type auditDLQOriginalAckDriver struct {
	*MemoryDriver
	originalUnsettled bool
	failOriginalAck   bool
}

func (d *auditDLQOriginalAckDriver) Ack(_ context.Context, del *Delivery) error {
	if del.Message.Topic == "x" && del.Message.GroupOrder == 1 {
		if d.failOriginalAck {
			d.originalUnsettled = true
			return errors.New("original ack failed")
		}
		d.originalUnsettled = false
	}
	return nil
}
func TestDLQCopyCannotReleaseUnsettledOriginal(t *testing.T) {
	d := &auditDLQOriginalAckDriver{MemoryDriver: NewMemoryDriver(), failOriginalAck: true}
	router := NewRouter()
	disp := &budgetDispatcher{}
	laterRan := false
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		if m.GroupOrder == 1 {
			return ErrNoHandlerFound
		}
		laterRan = true
		return nil
	})
	c := NewConsumer(d, router, disp, WithBatchSize(1), WithLogger(worker.NopLogger()))
	defer c.Stop()
	head := NewMessage("x", nil, WithGroup("g", 1))
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if !d.originalUnsettled {
		t.Fatal("original Ack failure not exercised")
	}
	expireGroupBackoff(c, "g")
	// DLQ has the same ID and GroupID, but is a separate delivery.
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	later := NewMessage("x", nil, WithGroup("g", 2))
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if laterRan || !d.originalUnsettled {
		t.Fatal("DLQ copy released original barrier")
	}
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	intact := entry != nil && entry.retryHeadID == head.ID && entry.retryHeadTopic == head.Topic
	c.groupMu.Unlock()
	if !intact {
		t.Fatal("original barrier was replaced by DLQ identity")
	}
	// Once the original actually settles, following members may run.
	d.failOriginalAck = false
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	// Settle the ready DLQ copy before redelivering the held later member.
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if !laterRan || d.originalUnsettled {
		t.Fatal("group did not resume after original settlement")
	}

}
func TestOversizeRetryHeadReleasesBarrierAfterDLQ(t *testing.T) {
	b := NewMemoryBudget(4096)
	d := &heldGroupRetryDriver{MemoryDriver: NewMemoryDriver()}
	disp := &budgetDispatcher{}
	router := NewRouter()
	laterRan := false
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		if m.GroupOrder == 1 {
			return errors.New("retry head")
		}
		laterRan = true
		return nil
	})
	c := newBudgetConsumer(d, router, disp, b, WithMemoryEstimate(512))
	defer c.Stop()
	head := NewMessage("x", make([]byte, 1024), WithGroup("g", 1))
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	expireGroupBackoff(c, "g")
	if err := b.SetLimit(768); err != nil {
		t.Fatal(err)
	}
	d.Enqueue(context.Background(), head)
	if n := c.fetchAndProcess(); n != 0 {
		t.Fatalf("oversize retry admitted: %d", n)
	}
	runBudgetTasks(t, disp)
	ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
	if len(ds) != 1 || ds[0].Message.Topic != "x:dlq" {
		t.Fatal("retry head not transferred to DLQ")
	}
	d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup("g", 2)))
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	c.groupMu.Unlock()
	if !laterRan || entry != nil {
		t.Fatal("terminal admission retained retry barrier")
	}
	if b.Used() != 0 {
		t.Fatalf("unexpected memory leak: %d", b.Used())
	}
}

func TestOversizeAdmissionAckFailureKeepsOriginalBarrier(t *testing.T) {
	b := NewMemoryBudget(4096)
	d := &auditDLQOriginalAckDriver{MemoryDriver: NewMemoryDriver(), failOriginalAck: true}
	disp := &budgetDispatcher{}
	router := NewRouter()
	router.Register("x", func(context.Context, *Message, worker.SubWorkerPool) error { return errors.New("retry") })
	c := newBudgetConsumer(d, router, disp, b, WithMemoryEstimate(512))
	defer c.Stop()
	head := NewMessage("x", make([]byte, 1024), WithGroup("g", 1))
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	// MemoryDriver's retry timer retains the delivery outside the consumer; make
	// the redelivery visible explicitly, with a smaller budget and failed Ack.
	b.SetLimit(768)
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	intact := entry != nil && entry.retryHeadID == head.ID && entry.retryHeadTopic == "x"
	c.groupMu.Unlock()
	if !intact || !d.originalUnsettled {
		t.Fatal("failed terminal admission cleared original barrier")
	}
}

func TestAdmissionSettlementQueuesBehindActiveGroup(t *testing.T) {
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	c := NewConsumer(d, NewRouter(), disp, WithLogger(worker.NopLogger()))
	defer c.Stop()
	head := NewMessage("x", nil, WithGroup("g", 1))
	entry := c.acquireGroupLock("g")
	entry.mu.Lock()
	c.updateGroupSettlement(entry, head, true, time.Hour)
	// Submission does not wait for entry.mu; execution remains serialized.
	accepted := make(chan error, 1)
	go func() {
		accepted <- c.settleAdmissionFailure(context.Background(), &Delivery{Message: head}, ErrMemoryClaimTooLarge)
	}()
	select {
	case err := <-accepted:
		if err != nil {
			c.releaseGroupLock("g", entry)
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		c.releaseGroupLock("g", entry)
		t.Fatal("submission waited on group mutex")
	}
	done := make(chan error, 1)
	e := worker.New(worker.WithLogger(worker.NopLogger()))
	defer e.Shutdown()
	go func() { done <- e.ExecuteSync(context.Background(), disp.tasks[0]) }()
	select {
	case err := <-done:
		c.releaseGroupLock("g", entry)
		t.Fatalf("execution bypassed group lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	c.releaseGroupLock("g", entry)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("settlement did not finish")
	}
	c.groupMu.Lock()
	left := len(c.groupLocks)
	c.groupMu.Unlock()
	if left != 0 {
		t.Fatal("terminal admission left stale barrier")
	}
}

func TestAdmissionSettlementCannotClearOtherTopicHead(t *testing.T) {
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	c := NewConsumer(d, NewRouter(), disp, WithLogger(worker.NopLogger()))
	defer c.Stop()
	head := NewMessage("x", nil, WithGroup("g", 1))
	entry := c.acquireGroupLock("g")
	entry.mu.Lock()
	c.updateGroupSettlement(entry, head, true, time.Hour)
	c.releaseGroupLock("g", entry)
	copy := NewMessage("x:dlq", nil, WithGroup("g", 1))
	copy.ID = head.ID
	if err := c.settleAdmissionFailure(context.Background(), &Delivery{Message: copy}, ErrMemoryClaimTooLarge); err != nil {
		t.Fatal(err)
	}
	runBudgetTasks(t, disp)
	c.groupMu.Lock()
	intact := entry.retryHeadID == head.ID && entry.retryHeadTopic == head.Topic
	c.groupMu.Unlock()
	if !intact {
		t.Fatal("admission settlement of DLQ copy released original")
	}
}
