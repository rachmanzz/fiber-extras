package queue

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

type partialBatchDriver struct {
	*MemoryDriver
	first                  []*Delivery
	firstErr               error
	firstHook              func()
	fetched                bool
	acked, nacked, delayed []*Message
	nackContextErr         error
}

func (d *partialBatchDriver) Dequeue(ctx context.Context, req DequeueRequest) ([]*Delivery, error) {
	if !d.fetched {
		d.fetched = true
		if d.firstHook != nil {
			d.firstHook()
		}
		return d.first, d.firstErr
	}
	return d.MemoryDriver.Dequeue(ctx, req)
}
func (d *partialBatchDriver) Ack(_ context.Context, del *Delivery) error {
	d.acked = append(d.acked, del.Message)
	return nil
}
func (d *partialBatchDriver) Nack(ctx context.Context, del *Delivery, _ error) error {
	d.nackContextErr = ctx.Err()
	d.nacked = append(d.nacked, del.Message)
	return ctx.Err()
}
func (d *partialBatchDriver) Reenqueue(_ context.Context, _ *Delivery, m *Message, _ time.Time) error {
	d.delayed = append(d.delayed, m)
	return nil
}

func TestPartialBatchFetchErrorProcessesKnownHeadAndReleasesMemory(t *testing.T) {
	for _, budget := range []bool{false, true} {
		t.Run(map[bool]string{false: "without_budget", true: "memory_budget"}[budget], func(t *testing.T) {
			head := NewMessage("job", []byte("payload"), WithGroup("g", 1))
			later := NewMessage("job", nil, WithGroup("g", 2))
			d := &partialBatchDriver{MemoryDriver: NewMemoryDriver(), first: []*Delivery{{Message: head}}, firstErr: errors.New("later priority unavailable")}
			r := NewRouter()
			var ran []int
			r.Register("job", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
				ran = append(ran, m.GroupOrder)
				return nil
			})
			disp := &budgetDispatcher{}
			b := NewMemoryBudget(4096)
			opts := []ConsumerOption{WithBatchSize(1), WithLogger(worker.NopLogger())}
			if budget {
				opts = append(opts, WithMemoryBudget(b), WithMemoryEstimate(512))
			}
			c := NewConsumer(d, r, disp, opts...)
			defer c.Stop()
			if n := c.fetchAndProcess(); n != 0 {
				t.Fatal("partial fetch error must stop adaptive drain")
			}
			if len(disp.tasks) != 1 || (budget && b.Used() == 0) {
				t.Fatal("partial work was discarded or not reserved")
			}
			// Accept a later poll before running the first group drain.
			d.Enqueue(context.Background(), later)
			c.fetchAndProcess()
			runBudgetTasks(t, disp)
			if !reflect.DeepEqual(ran, []int{1, 2}) || len(d.acked) != 2 || len(d.nacked) != 0 {
				t.Fatalf("partial ordering/settlement: ran=%v Ack=%d Nack=%d", ran, len(d.acked), len(d.nacked))
			}
			if b.Used() != 0 || len(c.groupLocks) != 0 {
				t.Fatal("partial batch leaked claims/group state")
			}
		})
	}
}

func TestPartialBatchRetryBlocksLaterMemberUntilHeadSettles(t *testing.T) {
	head := NewMessage("job", nil, WithGroup("g", 1))
	later := NewMessage("job", nil, WithGroup("g", 2))
	d := &partialBatchDriver{MemoryDriver: NewMemoryDriver(), first: []*Delivery{{Message: head}}, firstErr: context.DeadlineExceeded}
	r := NewRouter()
	var ran []int
	calls := 0
	r.Register("job", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		ran = append(ran, m.GroupOrder)
		if m.GroupOrder == 1 {
			calls++
			if calls == 1 {
				return errors.New("handler retry")
			}
		}
		return nil
	})
	disp := &budgetDispatcher{}
	b := NewMemoryBudget(4096)
	c := newBudgetConsumer(d, r, disp, b, WithMemoryEstimate(512))
	defer c.Stop()
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if !reflect.DeepEqual(ran, []int{1}) {
		t.Fatalf("later member bypassed partial retry head: %v", ran)
	}
	expireGroupBackoff(c, "g")
	d.Enqueue(context.Background(), head)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	d.Enqueue(context.Background(), later)
	c.fetchAndProcess()
	runBudgetTasks(t, disp)
	if !reflect.DeepEqual(ran, []int{1, 1, 2}) || b.Used() != 0 || len(c.groupLocks) != 0 {
		t.Fatalf("partial retry failed to resume: %v used=%d", ran, b.Used())
	}
}

func TestPartialBatchCanceledConsumerNacksBeforeDispatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		budget bool
		err    error
	}{
		{"without_budget", false, context.Canceled},
		{"memory_budget", true, context.Canceled},
		{"successful_fetch_after_cancel", true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := tc.budget
			head := NewMessage("job", nil, WithGroup("g", 1))
			d := &partialBatchDriver{MemoryDriver: NewMemoryDriver(), first: []*Delivery{{Message: head}}, firstErr: tc.err}
			if !budget {
				d.first = append(d.first, &Delivery{Message: NewMessage("independent", nil)})
			}
			disp := &budgetDispatcher{}
			b := NewMemoryBudget(4096)
			opts := []ConsumerOption{WithLogger(worker.NopLogger())}
			if budget {
				opts = append(opts, WithMemoryBudget(b), WithMemoryEstimate(512))
			}
			c := NewConsumer(d, NewRouter(), disp, opts...)
			defer c.Stop()
			d.firstHook = c.cancel
			c.fetchAndProcess()
			if len(disp.tasks) != 0 || len(d.acked) != 0 || len(d.nacked) != len(d.first) || d.nackContextErr != nil {
				t.Fatal("canceled partial batch was dispatched, discarded, or settled with canceled context")
			}
			if b.Used() != 0 || head.Attempt != 0 {
				t.Fatal("cancel rollback leaked memory or consumed handler attempt")
			}
			entry := c.groupLocks["g"]
			if entry == nil || entry.retryHeadID != head.ID {
				t.Fatal("cancellation discarded known group position")
			}
		})
	}
}
