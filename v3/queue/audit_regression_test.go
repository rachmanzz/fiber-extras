package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

type auditDispatcher struct{ tasks []worker.Task }

func (a *auditDispatcher) Dispatch(_ context.Context, t worker.Task) error {
	a.tasks = append(a.tasks, t)
	return nil
}
func (a *auditDispatcher) ExecuteSync(context.Context, worker.Task) error { return nil }

type auditDriver struct {
	*MemoryDriver
	nacks int
}

func (a *auditDriver) Reenqueue(context.Context, *Delivery, *Message, time.Time) error {
	return errors.New("publish failed")
}
func (a *auditDriver) Nack(context.Context, *Delivery, error) error { a.nacks++; return nil }
func TestAuditRetryPublishFailure(t *testing.T) {
	d := &auditDriver{MemoryDriver: NewMemoryDriver()}
	c := NewConsumer(d, NewRouter(), &auditDispatcher{}, WithLogger(worker.NopLogger()))
	m := NewMessage("x", nil)
	_, _, err := c.handleFailure(context.Background(), &Delivery{Message: m}, errors.New("handler failed"))
	if err == nil || d.nacks != 1 {
		t.Fatalf("unexpected err=%v nacks=%d", err, d.nacks)
	}
}
func TestAuditGroupOvertake(t *testing.T) {
	d := NewMemoryDriver()
	r := NewRouter()
	var got []int
	r.Register("x", func(_ context.Context, m *Message, _ worker.SubWorkerPool) error {
		got = append(got, m.GroupOrder)
		return nil
	})
	a := &auditDispatcher{}
	c := NewConsumer(d, r, a, WithBatchSize(1), WithLogger(worker.NopLogger()))
	for i := 1; i <= 2; i++ {
		d.Enqueue(context.Background(), NewMessage("x", nil, WithGroup("g", i)))
	}
	c.fetchAndProcess()
	c.fetchAndProcess()
	e := worker.New(worker.WithLogger(worker.NopLogger()))
	for i := len(a.tasks) - 1; i >= 0; i-- {
		if err := e.ExecuteSync(context.Background(), a.tasks[i]); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v", got)
	}
}
func TestAuditUnknownDriverFallback(t *testing.T) {
	Reset()
	defer Reset()
	d := NewMemoryDriver()
	SetDriver(d)
	if err := Enqueue(context.Background(), NewMessage("x", nil, WithDriver("typo"))); err == nil {
		t.Fatal("unknown explicit driver must fail")
	}
	ds, _ := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
	if len(ds) != 0 {
		t.Fatal("unknown driver routed to default")
	}
}

type auditDelayedDriver struct {
	Driver
	failure error
	nacks   int
}

func (d *auditDelayedDriver) EnqueueAt(context.Context, *Message, time.Time) error { return d.failure }
func (d *auditDelayedDriver) Nack(context.Context, *Delivery, error) error         { d.nacks++; return nil }
func TestAuditRetryDelayedFailureAndLostLease(t *testing.T) {
	for _, failure := range []error{errors.New("publish unavailable"), ErrDeliveryLost} {
		d := &auditDelayedDriver{Driver: NewMemoryDriver(), failure: failure}
		c := NewConsumer(d, NewRouter(), &auditDispatcher{}, WithLogger(worker.NopLogger()))
		retried, _, err := c.handleFailure(context.Background(), &Delivery{Message: NewMessage("x", nil)}, errors.New("handler failure"))
		want := 1
		if errors.Is(failure, ErrDeliveryLost) {
			want = 0
		}
		if retried || !errors.Is(err, failure) || d.nacks != want {
			t.Fatalf("retried=%v err=%v nacks=%d want=%d", retried, err, d.nacks, want)
		}
	}
}
