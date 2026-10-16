package queue

import (
	"context"
	"testing"

	"github.com/rachmanzz/gosuite/v3/worker"
)

func TestMixedMemoryQueuePreservesGroupOrder(t *testing.T) {
	d := NewMemoryDriver()
	disp := &budgetDispatcher{}
	router := NewRouter()
	var order []int
	router.Register("group", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		order = append(order, m.GroupOrder)
		return nil
	})
	router.Register("independent", func(context.Context, *Message, worker.SubWorkerPool) error { return nil })
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(4096), WithMemoryEstimate(512))
	defer c.Stop()
	// All three messages are present before consuming. GroupOrder must take
	// precedence within g even though order 2 was constructed before order 1.
	a2 := NewMessage("group", nil, WithGroup("g", 2))
	b := NewMessage("independent", nil)
	a1 := NewMessage("group", nil, WithGroup("g", 1))
	for _, m := range []*Message{a2, b, a1} {
		d.Enqueue(context.Background(), m)
	}
	for i := 0; i < 3; i++ {
		c.fetchAndProcess()
		runBudgetTasks(t, disp)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("reproduction order=%v", order)
	}

}

type auditMemoryPressureDriver struct {
	*MemoryDriver
	held []*Delivery
}

func (d *auditMemoryPressureDriver) Nack(_ context.Context, del *Delivery, _ error) error {
	d.held = append(d.held, del)
	return nil
}
func TestTemporaryMemoryRejectionPreservesGroupBarrier(t *testing.T) {
	b := NewMemoryBudget(4096)
	external, _ := b.TryClaim(2500)
	defer external.Close()
	d := &auditMemoryPressureDriver{MemoryDriver: NewMemoryDriver()}
	disp := &budgetDispatcher{}
	router := NewRouter()
	var order []int
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		order = append(order, m.GroupOrder)
		return nil
	})
	c := newBudgetConsumer(d, router, disp, b, WithMemoryEstimate(512))
	defer c.Stop()
	first := NewMessage("x", make([]byte, 2048), WithGroup("g", 1))
	second := NewMessage("x", nil, WithGroup("g", 2))
	if messageMemorySize(first) > b.Limit() {
		t.Fatal("head should fit complete budget")
	}
	d.Enqueue(context.Background(), first)
	d.Enqueue(context.Background(), second)
	if n := c.fetchAndProcess(); n != 0 || len(d.held) != 1 || d.held[0].Message != first {
		t.Fatal("temporary memory rejection not exercised")
	}
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 0 {
		t.Fatalf("member 2 overtook rejected head: %v", order)
	}
	c.groupMu.Lock()
	entry := c.groupLocks["g"]
	intact := entry != nil && entry.retryHeadID == first.ID
	c.groupMu.Unlock()
	if !intact {
		t.Fatal("rejected head lost barrier")
	}
	external.Close()
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("recovery order=%v", order)
	}
	if first.Attempt != 0 {
		t.Fatal("admission rejection consumed handler attempt")
	}
}

func TestDispatchBackpressurePreservesGroupBarrier(t *testing.T) {
	d := &auditMemoryPressureDriver{MemoryDriver: NewMemoryDriver()}
	disp := &budgetDispatcher{reject: true}
	router := NewRouter()
	var order []int
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		order = append(order, m.GroupOrder)
		return nil
	})
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(4096), WithMemoryEstimate(512))
	defer c.Stop()
	first := NewMessage("x", nil, WithGroup("g", 1))
	second := NewMessage("x", nil, WithGroup("g", 2))
	d.Enqueue(context.Background(), first)
	d.Enqueue(context.Background(), second)
	if n := c.fetchAndProcess(); n != 0 || len(d.held) != 1 {
		t.Fatal("dispatch backpressure not exercised")
	}
	disp.reject = false
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 0 {
		t.Fatalf("member 2 overtook rejected head: %v", order)
	}
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("recovery order=%v", order)
	}
	if first.Attempt != 0 {
		t.Fatal("dispatch rejection consumed handler attempt")
	}
}

func TestDefaultGroupOrderKeepsOriginalPositionsAcrossRedelivery(t *testing.T) {
	d := &auditMemoryPressureDriver{MemoryDriver: NewMemoryDriver()}
	disp := &budgetDispatcher{reject: true}
	router := NewRouter()
	var got []string
	router.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		got = append(got, string(m.Payload))
		return nil
	})
	c := newBudgetConsumer(d, router, disp, NewMemoryBudget(4096), WithMemoryEstimate(512))
	defer c.Stop()
	first := NewMessage("x", []byte("1"), WithGroup("g"))
	second := NewMessage("x", []byte("2"), WithGroup("g"))
	third := NewMessage("x", []byte("3"), WithGroup("g"))
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	disp.reject = false
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(got) != 0 {
		t.Fatalf("second member overtook rejected first: %v", got)
	}
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), first)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), third)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(got) != 1 || got[0] != "1" {
		t.Fatalf("third overtook second held by broker: %v", got)
	}
	d.Enqueue(context.Background(), second)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), third)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if len(got) != 3 || got[0] != "1" || got[1] != "2" || got[2] != "3" {
		t.Fatalf("redelivery changed logical positions: %v", got)
	}
}
