package queuemsgpack_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuemsgpack "github.com/rachmanzz/fiber-extras/v3/queue-msgpack"
)

type sample struct {
	Name  string
	Count int
	Tags  []string
}

func TestCodec_ContentType(t *testing.T) {
	if got := queuemsgpack.Codec().ContentType(); got != queuemsgpack.ContentType {
		t.Fatalf("ContentType() = %q, want %q", got, queuemsgpack.ContentType)
	}
}

func TestCodec_RoundTrip(t *testing.T) {
	in := sample{Name: "alice", Count: 3, Tags: []string{"a", "b"}}

	raw, err := queuemsgpack.Codec().Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out sample
	if err := queuemsgpack.Codec().Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch: %+v != %+v", in, out)
	}
}

func TestCodec_RegisteredForAutoDecode(t *testing.T) {
	c, ok := queue.CodecFor(queuemsgpack.ContentType)
	if !ok {
		t.Fatalf("codec for %q not registered", queuemsgpack.ContentType)
	}
	if c.ContentType() != queuemsgpack.ContentType {
		t.Fatalf("registered codec content-type = %q", c.ContentType())
	}
}

func TestDispatch_MsgpackThroughMemoryDriver(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	want := sample{Name: "zoe", Count: 2, Tags: []string{"x"}}
	if err := queue.Dispatch(ctx, "topic", want, queue.WithCodec(queuemsgpack.Codec())); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}
	msg := deliveries[0].Message

	if got := msg.Headers["content-type"]; got != queuemsgpack.ContentType {
		t.Fatalf("content-type header = %q, want %q", got, queuemsgpack.ContentType)
	}
	if len(msg.Payload) > 0 && msg.Payload[0] == '{' {
		t.Fatalf("payload looks like JSON, expected binary MessagePack: %q", msg.Payload)
	}

	var got sample
	if err := queue.Decode(msg, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("decoded = %+v, want %+v", got, want)
	}
}

// TestDispatch_MsgpackSurvivesJSONEnvelopeRoundTrip mirrors what the
// redis/nats/rabbitmq adapters do on the wire: the whole Message (including the
// binary payload and the content-type header) is carried inside a JSON envelope.
func TestDispatch_MsgpackSurvivesJSONEnvelopeRoundTrip(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	want := sample{Name: "envelope", Count: 5, Tags: []string{"p", "q"}}
	if err := queue.Dispatch(ctx, "topic", want, queue.WithCodec(queuemsgpack.Codec())); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(deliveries))
	}

	raw, err := json.Marshal(deliveries[0].Message)
	if err != nil {
		t.Fatalf("envelope marshal: %v", err)
	}
	var round queue.Message
	if err := json.Unmarshal(raw, &round); err != nil {
		t.Fatalf("envelope unmarshal: %v", err)
	}

	if got := round.Headers["content-type"]; got != queuemsgpack.ContentType {
		t.Fatalf("content-type lost through JSON envelope: %q", got)
	}

	var got sample
	if err := queue.Decode(&round, &got); err != nil {
		t.Fatalf("decode after envelope round trip: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("decoded = %+v, want %+v", got, want)
	}
}
