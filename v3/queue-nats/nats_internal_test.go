package queuenats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rachmanzz/gosuite/v3/queue"
)

type retryPublishStore struct {
	nats.JetStreamContext
	records   map[string]*nats.Msg
	fail      error
	uncertain bool
}

func (s *retryPublishStore) PublishMsg(m *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	id := m.Header.Get(nats.MsgIdHdr)
	if _, ok := s.records[id]; ok {
		return &nats.PubAck{Duplicate: true}, nil
	}
	s.records[id] = m
	if s.uncertain {
		s.uncertain = false
		return nil, errors.New("publish response lost")
	}
	return &nats.PubAck{}, nil
}
func TestNATSRetryPublicationDedupAndFailures(t *testing.T) {
	original := queue.NewMessage("job", []byte("payload"))
	raw := buildNATSMsg("S.default.general.job", original)
	store := &retryPublishStore{records: map[string]*nats.Msg{raw.Header.Get(nats.MsgIdHdr): raw}}
	a := &NATSAdapter{js: store, stream: "S"}
	d := &queue.Delivery{Message: original, Token: raw}
	retry := *original
	retry.Attempt = 1
	// No bound subscription: Ack must fail, exercising publication replay after
	// settlement failure without needing a broker for this storage-level test.
	for i := 0; i < 2; i++ {
		if err := a.Reenqueue(context.Background(), d, &retry, time.Now()); !errors.Is(err, nats.ErrMsgNotBound) {
			t.Fatalf("Ack failure: %v", err)
		}
	}
	if len(store.records) != 2 {
		t.Fatalf("original plus retry expected, got %d", len(store.records))
	}
	var published *nats.Msg
	for id, m := range store.records {
		if id != raw.Header.Get(nats.MsgIdHdr) {
			published = m
		}
	}
	if published == nil || published.Header.Get(hdrID) != original.ID || published.Header.Get(hdrAttempt) != "1" {
		t.Fatalf("invalid retry: %+v", published)
	}
	store.fail = errors.New("publish unavailable")
	if err := a.Reenqueue(context.Background(), d, &retry, time.Now()); !errors.Is(err, store.fail) {
		t.Fatalf("publication failure replaced by Ack: %v", err)
	}
	store.fail = nil
	retry.Attempt = 2
	store.uncertain = true
	if err := a.Reenqueue(context.Background(), d, &retry, time.Now()); err == nil {
		t.Fatal("lost response ignored")
	}
	if err := a.Reenqueue(context.Background(), d, &retry, time.Now()); !errors.Is(err, nats.ErrMsgNotBound) {
		t.Fatalf("recovery: %v", err)
	}
	if len(store.records) != 3 {
		t.Fatalf("uncertain publication duplicated retry: %d", len(store.records))
	}
}
func TestNATSReenqueueRejectsInvalidInputsAndClosedAdapter(t *testing.T) {
	a := &NATSAdapter{}
	msg := queue.NewMessage("job", nil)
	if err := a.Reenqueue(context.Background(), &queue.Delivery{Message: msg, Token: "invalid"}, msg, time.Now()); err == nil {
		t.Fatal("invalid token accepted")
	}
	raw := buildNATSMsg("S.default.general.job", msg)
	d := &queue.Delivery{Message: msg, Token: raw}
	if err := a.Reenqueue(context.Background(), d, nil, time.Now()); !errors.Is(err, ErrNilMessage) {
		t.Fatalf("nil retry: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Reenqueue(context.Background(), d, msg, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("closed adapter accepted retry")
	}
}
