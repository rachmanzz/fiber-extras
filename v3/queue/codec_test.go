package queue_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
)

// prefixCodec is a test-only codec that prefixes JSON with a sentinel so we can
// prove Dispatch routed the payload through the selected codec.
type prefixCodec struct{ prefix string }

func (prefixCodec) ContentType() string { return "application/x-prefix" }

func (c prefixCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append([]byte(c.prefix), b...), nil
}

func (c prefixCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(bytes.TrimPrefix(data, []byte(c.prefix)), v)
}

func TestCodec_JSONRegisteredByDefault(t *testing.T) {
	if _, ok := queue.CodecFor(queue.ContentTypeJSON); !ok {
		t.Fatalf("expected %q to be registered by default", queue.ContentTypeJSON)
	}
	if got := (queue.JSONCodec{}).ContentType(); got != queue.ContentTypeJSON {
		t.Fatalf("JSONCodec.ContentType() = %q, want %q", got, queue.ContentTypeJSON)
	}
}

func TestCodec_DecodeDefaultsToJSON(t *testing.T) {
	msg := queue.NewMessage("t", []byte(`{"name":"alice"}`))

	var out struct{ Name string }
	if err := queue.Decode(msg, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Name != "alice" {
		t.Fatalf("decoded name = %q, want alice", out.Name)
	}
}

func TestCodec_DecodeResolvesFromHeader(t *testing.T) {
	c := prefixCodec{prefix: "PX:"}
	queue.RegisterCodec(c)

	raw, err := c.Marshal(map[string]int{"count": 7})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := queue.NewMessage("t", raw, queue.WithHeaders(map[string]string{
		"content-type": c.ContentType(),
	}))

	var out map[string]int
	if err := queue.Decode(msg, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["count"] != 7 {
		t.Fatalf("decoded count = %d, want 7", out["count"])
	}
}

func TestCodec_DecodeUnknownContentTypeErrors(t *testing.T) {
	msg := queue.NewMessage("t", []byte("x"), queue.WithHeaders(map[string]string{
		"content-type": "application/x-does-not-exist",
	}))

	var out any
	if err := queue.Decode(msg, &out); err == nil {
		t.Fatal("expected error for unregistered content-type, got nil")
	}
}

func TestCodec_DecodeNilMessage(t *testing.T) {
	var out any
	if err := queue.Decode(nil, &out); err == nil {
		t.Fatal("expected error decoding nil message, got nil")
	}
}

func TestCodec_DispatchUsesSelectedCodec(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	c := prefixCodec{prefix: "PX:"}
	ctx := context.Background()
	if err := queue.Dispatch(ctx, "topic", map[string]string{"k": "v"}, queue.WithCodec(c)); err != nil {
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

	if got := msg.Headers["content-type"]; got != c.ContentType() {
		t.Fatalf("content-type header = %q, want %q", got, c.ContentType())
	}
	if !bytes.HasPrefix(msg.Payload, []byte("PX:")) {
		t.Fatalf("payload = %q, expected codec prefix", msg.Payload)
	}

	var out map[string]string
	if err := queue.Decode(msg, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["k"] != "v" {
		t.Fatalf("decoded k = %q, want v", out["k"])
	}
}

func TestCodec_DispatchDefaultStaysJSON(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	if err := queue.Dispatch(ctx, "topic", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	msg := deliveries[0].Message

	if _, ok := msg.Headers["content-type"]; ok {
		t.Fatal("default dispatch must not set a content-type header")
	}
	if !json.Valid(msg.Payload) {
		t.Fatalf("default payload is not valid JSON: %q", msg.Payload)
	}
}

func TestCodec_DispatchRawBytesWithCodecIsUntagged(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	raw := []byte("already-encoded")
	ctx := context.Background()
	if err := queue.Dispatch(ctx, "topic", raw, queue.WithCodec(prefixCodec{prefix: "PX:"})); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	msg := deliveries[0].Message

	if got, ok := msg.Headers["content-type"]; ok {
		t.Fatalf("raw byte payload must not be codec-tagged, got content-type=%q", got)
	}
	if !bytes.Equal(msg.Payload, raw) {
		t.Fatalf("raw byte payload was mutated: %q", msg.Payload)
	}
}

// jsonPrefixCodec is a reversible codec registered under the JSON content type,
// used to prove registration overrides apply symmetrically to encode and decode.
type jsonPrefixCodec struct{ prefix string }

func (jsonPrefixCodec) ContentType() string { return queue.ContentTypeJSON }

func (c jsonPrefixCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append([]byte(c.prefix), b...), nil
}

func (c jsonPrefixCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(bytes.TrimPrefix(data, []byte(c.prefix)), v)
}

func TestCodec_OverrideJSONAppliesToDispatchAndDecode(t *testing.T) {
	orig, ok := queue.CodecFor(queue.ContentTypeJSON)
	if !ok {
		t.Fatal("expected a default JSON codec to be registered")
	}
	queue.RegisterCodec(jsonPrefixCodec{prefix: "J2:"})
	defer queue.RegisterCodec(orig)

	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	if err := queue.Dispatch(ctx, "topic", map[string]int{"n": 1}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	msg := deliveries[0].Message

	if !bytes.HasPrefix(msg.Payload, []byte("J2:")) {
		t.Fatalf("default Dispatch ignored JSON codec override: %q", msg.Payload)
	}

	var out map[string]int
	if err := queue.Decode(msg, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["n"] != 1 {
		t.Fatalf("decoded n = %d, want 1", out["n"])
	}
}

func TestCodec_UserContentTypePreserved(t *testing.T) {
	queue.Reset()
	defer queue.Reset()

	driver := queue.NewMemoryDriver()
	queue.SetDriver(driver)

	ctx := context.Background()
	if err := queue.Dispatch(ctx, "topic", map[string]int{"n": 1},
		queue.WithHeaders(map[string]string{"content-type": "application/custom"}),
		queue.WithCodec(prefixCodec{prefix: "PX:"}),
	); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	deliveries, err := driver.Dequeue(ctx, queue.DequeueRequest{BatchSize: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got := deliveries[0].Message.Headers["content-type"]; got != "application/custom" {
		t.Fatalf("user content-type was overwritten: %q", got)
	}
}
