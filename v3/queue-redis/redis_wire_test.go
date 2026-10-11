package queueredis

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/queue"
)

// benchMessage builds a realistic message with a binary payload -- the shape
// that pays the base64 tax under the v1 JSON envelope.
func benchMessage() *queue.Message {
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	return queue.NewMessage("email.render",
		payload,
		queue.WithPriority(queue.PriorityHigh),
		queue.WithHeaders(map[string]string{
			"user_id": "usr_12345",
			"trace":   "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		}),
		queue.WithMaxAttempts(3),
	)
}

// BenchmarkWire_V1_JSON_Marshal measures the legacy wire format: the whole
// Message (payload included) goes through encoding/json, which base64-encodes
// the []byte payload.
func BenchmarkWire_V1_JSON_Marshal(b *testing.B) {
	msg := benchMessage()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := json.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(raw)), "v1-bytes")
	}
}

func BenchmarkWire_V1_JSON_Unmarshal(b *testing.B) {
	msg := benchMessage()
	raw, err := json.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var m queue.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWire_V2_MetaEncode measures the v2 split transport's
// serialization cost: only the metadata is encoded (msgpack); the payload is
// stored raw in its own hash field with zero copying/encoding.
func BenchmarkWire_V2_MetaEncode(b *testing.B) {
	msg := benchMessage()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta, err := encodeMeta(msg)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(meta)), "meta-bytes")
	}
}

func BenchmarkWire_V2_MetaDecode(b *testing.B) {
	msg := benchMessage()
	meta, err := encodeMeta(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(meta)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeMeta(meta); err != nil {
			b.Fatal(err)
		}
	}
}

// TestWire_SplitSmallerThanJSONEnvelope guards the reason the split wire
// format exists: for binary payloads the v2 record (raw payload + msgpack
// metadata) must be smaller than the v1 JSON envelope (payload base64'd).
func TestWire_SplitSmallerThanJSONEnvelope(t *testing.T) {
	msg := benchMessage()

	v1, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := encodeMeta(msg)
	if err != nil {
		t.Fatal(err)
	}
	v2Total := len(meta) + len(msg.Payload)

	if v2Total >= len(v1) {
		t.Fatalf("v2 wire (%d B) is not smaller than v1 JSON envelope (%d B)", v2Total, len(v1))
	}
	t.Logf("v1 JSON envelope: %d B | v2 (meta %d B + raw payload %d B): %d B (%.1f%% smaller)",
		len(v1), len(meta), len(msg.Payload), v2Total, 100*(1-float64(v2Total)/float64(len(v1))))
}

// TestWire_MetaRoundTrip checks the v2 metadata round-trips every field,
// including binary-unsafe header values that JSON would corrupt.
func TestWire_MetaRoundTrip(t *testing.T) {
	msg := queue.NewMessage("email.render", []byte("x"),
		queue.WithPriority(queue.PriorityCritical),
		queue.WithGroup("grp-1", 7),
		queue.WithHeaders(map[string]string{
			"raw_payload": "\xff\xfe\x00binary", // invalid UTF-8: JSON would replace it
		}),
		queue.WithMaxAttempts(5),
	)
	msg.Attempt = 2
	msg.CreatedAt = time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC)
	msg.Driver = "redis"

	raw, err := encodeMeta(msg)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := decodeMeta(raw)
	if err != nil {
		t.Fatal(err)
	}
	got := meta.toMessage(msg.Payload)

	if got.ID != msg.ID || got.Topic != msg.Topic || got.Priority != msg.Priority ||
		got.GroupID != msg.GroupID || got.GroupOrder != msg.GroupOrder ||
		got.Attempt != msg.Attempt || got.MaxAttempts != msg.MaxAttempts ||
		got.Driver != msg.Driver || !got.CreatedAt.Equal(msg.CreatedAt) {
		t.Fatalf("metadata mismatch: got %+v", got)
	}
	if got.Headers["raw_payload"] != msg.Headers["raw_payload"] {
		t.Fatalf("binary-unsafe header value was corrupted: %q != %q", got.Headers["raw_payload"], msg.Headers["raw_payload"])
	}
}
