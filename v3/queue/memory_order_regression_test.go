package queue

import (
	"context"
	"testing"
	"time"
)

func TestMemoryGroupOrderingAcrossEnqueuePermutations(t *testing.T) {
	base := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	specs := []struct {
		group string
		order int
	}{{"g", 3}, {"", 0}, {"g", 1}, {"g", 2}}
	var visit func([]int, []bool)
	visit = func(permutation []int, used []bool) {
		if len(permutation) < len(specs) {
			for i := range specs {
				if !used[i] {
					used[i] = true
					visit(append(permutation, i), used)
					used[i] = false
				}
			}
			return
		}
		d := NewMemoryDriver()
		defer d.Close()
		for _, i := range permutation {
			m := NewMessage("x", nil, WithGroup(specs[i].group, specs[i].order))
			m.CreatedAt = base.Add(time.Duration(i) * time.Second)
			d.Enqueue(context.Background(), m)
		}
		var got []int
		for range specs {
			ds, err := d.Dequeue(context.Background(), DequeueRequest{BatchSize: 1})
			if err != nil || len(ds) != 1 {
				t.Fatalf("dequeue err=%v count=%d", err, len(ds))
			}
			if ds[0].Message.GroupID == "g" {
				got = append(got, ds[0].Message.GroupOrder)
			}
		}
		if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
			t.Fatalf("permutation=%v order=%v", permutation, got)
		}
	}
	visit(nil, make([]bool, len(specs)))
}
func TestMemoryEligibleHeadsPreservePriorityAndFIFO(t *testing.T) {
	d := NewMemoryDriver()
	defer d.Close()
	ctx := context.Background()
	groupLow := NewMessage("x", nil, WithGroup("g", 1), WithPriority(PriorityLow))
	independent := NewMessage("x", nil)
	groupHigh := NewMessage("x", nil, WithGroup("g", 2), WithPriority(PriorityHigh))
	for _, m := range []*Message{groupLow, independent, groupHigh} {
		d.Enqueue(ctx, m)
	}
	ds, _ := d.Dequeue(ctx, DequeueRequest{BatchSize: 10})
	if len(ds) != 3 || ds[0].Message != groupHigh || ds[1].Message != independent || ds[2].Message != groupLow {
		t.Fatal("priority ordering changed")
	}
	// Equal priority and equal timestamp must remain stable even across buckets.
	for _, m := range []*Message{groupLow, independent, groupHigh} {
		m.Priority = PriorityDefault
		m.GroupOrder = 0
		m.CreatedAt = time.Time{}
		d.Enqueue(ctx, m)
	}
	ds, _ = d.Dequeue(ctx, DequeueRequest{BatchSize: 10})
	if len(ds) != 3 || ds[0].Message != groupLow || ds[1].Message != independent || ds[2].Message != groupHigh {
		t.Fatal("equal-timestamp insertion order changed")
	}
}
func TestMemoryLargeBatchDoesNotAllocateForAbsentMessages(t *testing.T) {
	d := NewMemoryDriver()
	defer d.Close()
	d.Enqueue(context.Background(), NewMessage("x", nil))
	ds, err := d.Dequeue(context.Background(), DequeueRequest{BatchSize: int(^uint(0) >> 1)})
	if err != nil || len(ds) != 1 {
		t.Fatalf("err=%v count=%d", err, len(ds))
	}
}
