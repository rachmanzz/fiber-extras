package queuenats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rachmanzz/gosuite/v3/queue"
)

type poisonPublishFailure struct {
	nats.JetStreamContext
	failure error
}

func (js poisonPublishFailure) PublishMsg(m *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.Header.Get(hdrTopic) == queue.PoisonTopic {
		return nil, js.failure
	}
	return js.JetStreamContext.PublishMsg(m, opts...)
}

func TestNATSPoisonFailurePreservesPrefixAndReleasesRemainingClaims(t *testing.T) {
	for _, invalidDueTime := range []bool{false, true} {
		for corruptIndex := 0; corruptIndex < 3; corruptIndex++ {
			t.Run(fmt.Sprintf("invalidDueTime=%t/index=%d", invalidDueTime, corruptIndex), func(t *testing.T) {
				srv, err := server.NewServer(&server.Options{Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true})
				if err != nil {
					t.Fatal(err)
				}
				go srv.Start()
				defer srv.Shutdown()
				if !srv.ReadyForConnections(5 * time.Second) {
					t.Fatal("server not ready")
				}
				nc, err := nats.Connect(srv.ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				cfg := DefaultConfig()
				cfg.Stream = "PARTIAL_POISON"
				cfg.Storage = "memory"
				// Recovery below must happen through NAK, not expiration of AckWait.
				cfg.AckWait = time.Minute
				a, err := NewWithConfig(nc, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Close()
				ids := make([]string, 3)
				for i := range ids {
					m := queue.NewMessage("job", nil, queue.WithGroup("g", i+1), queue.WithPriority(queue.PriorityCritical))
					raw := buildNATSMsg(a.subjectFor(m), m)
					ids[i] = m.ID
					if i == corruptIndex {
						if invalidDueTime {
							raw.Header.Set(hdrNotBefore, "invalid")
						} else {
							raw.Header.Del(hdrTopic)
						}
					}
					if _, err := a.js.PublishMsg(raw); err != nil {
						t.Fatal(err)
					}
				}
				failure := errors.New("injected poison publication failure")
				a.js = poisonPublishFailure{JetStreamContext: a.js, failure: failure}
				ds, err := a.Dequeue(context.Background(), queue.DequeueRequest{BatchSize: 3})
				if !errors.Is(err, failure) {
					t.Fatalf("publication error lost: %v", err)
				}
				if len(ds) != corruptIndex {
					t.Fatalf("valid prefix lost: got=%d want=%d", len(ds), corruptIndex)
				}
				for i, d := range ds {
					if d.Message.ID != ids[i] {
						t.Fatalf("prefix identity: got=%s want=%s", d.Message.ID, ids[i])
					}
					// AckSync proves the prefix was not NAKed by cleanup.
					if err := d.Token.(*nats.Msg).AckSync(); err != nil {
						t.Fatal(err)
					}
				}
				if err := nc.Flush(); err != nil {
					t.Fatal(err)
				}
				remaining, err := a.critSub.Fetch(3-corruptIndex, nats.MaxWait(2*time.Second))
				if err != nil {
					t.Fatalf("claims not released promptly: %v", err)
				}
				if len(remaining) != 3-corruptIndex {
					t.Fatalf("missing released claims: %d", len(remaining))
				}
				want := make(map[string]bool)
				for _, id := range ids[corruptIndex:] {
					want[id] = true
				}
				for _, raw := range remaining {
					id := raw.Header.Get(hdrID)
					if !want[id] {
						t.Fatalf("unexpected or duplicate redelivery: %s", id)
					}
					delete(want, id)
					meta, err := raw.Metadata()
					if err != nil {
						t.Fatal(err)
					}
					if meta.NumDelivered != 2 {
						t.Fatalf("claim was not redelivered: %+v", meta)
					}
					if err := raw.Term(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestNATSReleaseFetchedMessagesPreservesCauseAndCleanupErrors(t *testing.T) {
	cause := errors.New("original failure")
	err := releaseFetchedMessages([]*nats.Msg{{}, {}}, cause)
	if !errors.Is(err, cause) || !errors.Is(err, nats.ErrMsgNotBound) {
		t.Fatalf("original/cleanup errors not preserved: %v", err)
	}
}
