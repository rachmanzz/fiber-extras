package queuemsgpack_test

import (
	"context"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	queuemsgpack "github.com/rachmanzz/fiber-extras/v3/queue-msgpack"
)

// benchPayload mirrors a realistic small background-job payload.
type benchPayload struct {
	Topic     string            `json:"topic" msgpack:"topic"`
	Data      map[string]string `json:"data" msgpack:"data"`
	Attempts  int               `json:"attempts" msgpack:"attempts"`
	Priority  int               `json:"priority" msgpack:"priority"`
	CreatedAt string            `json:"created_at" msgpack:"created_at"`
}

func newBenchPayload() benchPayload {
	return benchPayload{
		Topic:     "user:welcome_email",
		Data:      map[string]string{"user_id": "usr_12345", "email": "alice@example.com", "locale": "id-ID", "campaign": "welcome-v2"},
		Attempts:  1,
		Priority:  100,
		CreatedAt: "2026-10-10T09:00:00Z",
	}
}

var benchSink []byte

func BenchmarkCodec_JSON_Marshal(b *testing.B) {
	c := queue.JSONCodec{}
	p := newBenchPayload()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchSink, err = c.Marshal(p)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_Msgpack_Marshal(b *testing.B) {
	c := queuemsgpack.Codec()
	p := newBenchPayload()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchSink, err = c.Marshal(p)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_JSON_Unmarshal(b *testing.B) {
	c := queue.JSONCodec{}
	raw, err := c.Marshal(newBenchPayload())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out benchPayload
		if err := c.Unmarshal(raw, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodec_Msgpack_Unmarshal(b *testing.B) {
	c := queuemsgpack.Codec()
	raw, err := c.Marshal(newBenchPayload())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out benchPayload
		if err := c.Unmarshal(raw, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkQueueDispatch(codec queue.Codec) func(*testing.B) {
	return func(b *testing.B) {
		queue.Reset()
		defer queue.Reset()

		driver := queue.NewMemoryDriver()
		queue.SetDriver(driver)

		ctx := context.Background()
		p := newBenchPayload()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var err error
			if codec != nil {
				err = queue.Dispatch(ctx, "user:welcome_email", p, queue.WithCodec(codec))
			} else {
				err = queue.Dispatch(ctx, "user:welcome_email", p)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkQueue_Dispatch_JSON(b *testing.B) {
	benchmarkQueueDispatch(nil)(b)
}

func BenchmarkQueue_Dispatch_Msgpack(b *testing.B) {
	benchmarkQueueDispatch(queuemsgpack.Codec())(b)
}
