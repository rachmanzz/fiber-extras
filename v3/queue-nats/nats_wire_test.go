package queuenats

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
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

// headerBytes approximates the protocol cost of the v2 metadata headers
// (each header line adds the key, value and framing overhead).
func headerBytes(h map[string][]string) int {
	n := 0
	for k, vs := range h {
		for _, v := range vs {
			n += len(k) + len(v) + 4 // "Key: value\r\n"
		}
	}
	return n
}

// BenchmarkWire_V1_JSON_Marshal measures the legacy wire format: the whole
// Message goes through encoding/json, which base64-encodes the []byte payload.
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

// BenchmarkWire_V2_BuildMsg measures the v2 split transport's render cost:
// the payload is passed through zero-copy and metadata becomes header lines.
func BenchmarkWire_V2_BuildMsg(b *testing.B) {
	msg := benchMessage()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := buildNATSMsg("FIBER_QUEUE.default.general.email.render", msg)
		b.ReportMetric(float64(len(m.Data)+headerBytes(m.Header)), "v2-bytes")
	}
}

func BenchmarkWire_V2_DecodeMsg(b *testing.B) {
	msg := benchMessage()
	wire := buildNATSMsg("FIBER_QUEUE.default.general.email.render", msg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeNATSMsg(wire); err != nil {
			b.Fatal(err)
		}
	}
}

// TestWire_V2RoundTrip checks the v2 headers round-trip every metadata field,
// and that the payload arrives as the exact raw bytes (no base64).
func TestWire_V2RoundTrip(t *testing.T) {
	msg := queue.NewMessage("email.render", []byte{0x00, 0xff, 0xfe, 0x01},
		queue.WithPriority(queue.PriorityCritical),
		queue.WithGroup("grp-1", 7),
		queue.WithHeaders(map[string]string{"k": "v"}),
		queue.WithMaxAttempts(5),
	)
	msg.Attempt = 2
	msg.CreatedAt = time.Date(2026, 10, 11, 9, 30, 0, 0, time.UTC)
	msg.Driver = "nats"

	wire := buildNATSMsg("S.default.general.email.render", msg)
	got, err := decodeNATSMsg(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != msg.ID || got.Topic != msg.Topic || got.Priority != msg.Priority ||
		got.GroupID != msg.GroupID || got.GroupOrder != msg.GroupOrder ||
		got.Attempt != msg.Attempt || got.MaxAttempts != msg.MaxAttempts ||
		got.Driver != msg.Driver || !got.CreatedAt.Equal(msg.CreatedAt) {
		t.Fatalf("metadata mismatch: got %+v", got)
	}
	if string(got.Payload) != string(msg.Payload) {
		t.Fatalf("payload corrupted: %x != %x", got.Payload, msg.Payload)
	}
	if got.Headers["k"] != "v" {
		t.Fatalf("user header lost: %+v", got.Headers)
	}
}
