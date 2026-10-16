package queue

import (
	"context"
	"errors"
	"testing"
)

// Model the storage invariant shared by Redis/PostgreSQL: ID identifies a
// stored record, so replacing it invalidates or changes the original settlement.
type idKeyedDLQDriver struct {
	*MemoryDriver
	records              map[string]*Message
	failAck, failPublish bool
	nacked               bool
}

func (d *idKeyedDLQDriver) Enqueue(_ context.Context, m *Message) error {
	if d.failPublish {
		return errors.New("publish failed")
	}
	d.records[m.ID] = m
	return nil
}
func (d *idKeyedDLQDriver) Ack(_ context.Context, del *Delivery) error {
	if d.records[del.Message.ID] != del.Message {
		return ErrDeliveryLost
	}
	if d.failAck {
		return errors.New("Ack failed")
	}
	delete(d.records, del.Message.ID)
	return nil
}
func (d *idKeyedDLQDriver) Nack(_ context.Context, _ *Delivery, _ error) error {
	d.nacked = true
	return nil
}

func TestDLQPublicationSurvivesOriginalAckWithIDKeyedStorage(t *testing.T) {
	for _, failAck := range []bool{false, true} {
		t.Run(map[bool]string{false: "ack", true: "ack_failure_and_redelivery"}[failAck], func(t *testing.T) {
			original := NewMessage("job", []byte("payload"), WithGroup("g", 1), WithHeaders(map[string]string{"content-type": ContentTypeJSON, "custom": "kept"}))
			d := &idKeyedDLQDriver{MemoryDriver: NewMemoryDriver(), records: map[string]*Message{original.ID: original}, failAck: failAck}
			c := NewConsumer(d, NewRouter(), &budgetDispatcher{})
			defer c.Stop()
			delivery := &Delivery{Message: original, Token: original.ID}
			err := c.routeToDLQ(context.Background(), delivery, errors.New("failure"))
			if (err != nil) != failAck {
				t.Fatalf("unexpected settlement: %v", err)
			}
			if failAck {
				if d.records[original.ID] != original {
					t.Fatal("failed Ack lost original")
				}
				d.failAck = false
				if err := c.routeToDLQ(context.Background(), delivery, errors.New("failure")); err != nil {
					t.Fatal(err)
				}
			}
			want := 1
			if failAck {
				want = 2
			}
			if len(d.records) != want {
				t.Fatalf("DLQ records=%d, want %d", len(d.records), want)
			}
			for id, m := range d.records {
				if id == original.ID || m.Topic != "job:dlq" || string(m.Payload) != "payload" || m.GroupID != "" || m.GroupOrder != 0 {
					t.Fatalf("invalid DLQ: %+v", m)
				}
				if m.Headers["original_message_id"] != original.ID || m.Headers["original_topic"] != "job" || m.Headers["custom"] != "kept" || m.Headers["content-type"] != ContentTypeJSON || m.Headers["original_group_id"] != "g" || m.Headers["original_group_order"] != "1" {
					t.Fatalf("metadata: %v", m.Headers)
				}
				m.Headers["custom"] = "changed"
			}
			if original.Headers["custom"] != "kept" || original.Headers["original_message_id"] != "" {
				t.Fatal("DLQ mutated original headers")
			}
		})
	}
}
func TestDLQPublishFailureKeepsIDKeyedOriginal(t *testing.T) {
	original := NewMessage("job", nil)
	d := &idKeyedDLQDriver{MemoryDriver: NewMemoryDriver(), records: map[string]*Message{original.ID: original}, failPublish: true}
	c := NewConsumer(d, NewRouter(), &budgetDispatcher{})
	defer c.Stop()
	if err := c.routeToDLQ(context.Background(), &Delivery{Message: original}, errors.New("failure")); err == nil {
		t.Fatal("publication failure ignored")
	}
	if !d.nacked || len(d.records) != 1 || d.records[original.ID] != original {
		t.Fatal("failed publication lost original")
	}
}

func TestTerminalDLQDeliveryIsAckedWithoutRecursivePublication(t *testing.T) {
	for _, reason := range []error{ErrNoHandlerFound, errors.New("DLQ storage failed")} {
		t.Run(reason.Error(), func(t *testing.T) {
			m := NewMessage("job:dlq", nil, WithMaxAttempts(1))
			d := &idKeyedDLQDriver{MemoryDriver: NewMemoryDriver(), records: map[string]*Message{m.ID: m}}
			c := NewConsumer(d, NewRouter(), &budgetDispatcher{})
			defer c.Stop()
			retry, _, err := c.handleFailure(context.Background(), &Delivery{Message: m}, reason)
			if err != nil || retry || len(d.records) != 0 || d.nacked {
				t.Fatalf("terminal DLQ not dropped: retry=%v err=%v records=%v", retry, err, d.records)
			}
		})
	}
}

func TestDLQPayloadSnapshotsAreIndependentAfterAckFailure(t *testing.T) {
	original := NewMessage("job", []byte("original"), WithGroup("g", 1))
	d := &idKeyedDLQDriver{MemoryDriver: NewMemoryDriver(), records: map[string]*Message{original.ID: original}, failAck: true}
	c := NewConsumer(d, NewRouter(), &budgetDispatcher{})
	defer c.Stop()
	for i := 0; i < 2; i++ {
		if err := c.routeToDLQ(context.Background(), &Delivery{Message: original}, errors.New("failure")); err == nil {
			t.Fatal("expected original Ack failure")
		}
	}
	var copies []*Message
	for id, m := range d.records {
		if id != original.ID {
			copies = append(copies, m)
		}
	}
	if len(copies) != 2 {
		t.Fatalf("DLQ copies=%d", len(copies))
	}
	copies[0].Payload[0] = 'X'
	if string(original.Payload) != "original" || string(copies[1].Payload) != "original" {
		t.Fatal("DLQ mutation changed original or another DLQ copy")
	}
	original.Payload[1] = 'Y'
	if string(copies[0].Payload) != "Xriginal" || string(copies[1].Payload) != "original" {
		t.Fatal("original mutation changed a published DLQ snapshot")
	}
}
