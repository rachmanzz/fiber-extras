# MessagePack Codec for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-msgpack.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-msgpack)

Pluggable [`queue.Codec`](../queue) implementation that encodes message payloads with **MessagePack** instead of JSON. Because codecs sit at the payload boundary, this works with **every** `v3/queue` adapter (memory, Redis, NATS, RabbitMQ, PostgreSQL) without a single driver change.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/queue
go get github.com/rachmanzz/fiber-extras/v3/queue-msgpack
```

---

## 📖 Quickstart

### Producer (dispatch)

```go
import queuemsgpack "github.com/rachmanzz/fiber-extras/v3/queue-msgpack"

// Opt into MessagePack on a per-message basis:
queue.Dispatch(context.Background(), "user:welcome_email", WelcomeEmail{
    UserID: "usr_12345",
    Email:  "alice@example.com",
    Tags:   []string{"beta", "eu"},
}, queue.WithCodec(queuemsgpack.Codec())) // payload -> MessagePack bytes + content-type header
```

### Consumer (decode — wiring is explicit)

`queue.Decode` has **no idea** about MessagePack until a codec is registered for the `application/msgpack` content type. Register it explicitly so the wiring is visible (the import also self-registers it via `init()`; the call below is idempotent and documents the contract):

```go
import (
    "context"
    "fmt"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    queuemsgpack "github.com/rachmanzz/fiber-extras/v3/queue-msgpack"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

func RegisterQueueTopics() {
    // 1. Wiring: register the codec so Decode can resolve it from the header.
    queue.RegisterCodec(queuemsgpack.Codec())

    // 2. Handler: Decode reads msg.Headers["content-type"], finds
    //    "application/msgpack" in the registry, and unmarshals with it.
    queue.RegisterHandler("user:welcome_email", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        var payload WelcomeEmail
        if err := queue.Decode(msg, &payload); err != nil {
            return queue.MarkNonRetryable(err) // malformed payload -> DLQ
        }
        fmt.Printf("welcome %s <%s> tags=%v\n", payload.UserID, payload.Email, payload.Tags)
        return nil
    })

    _, _ = queue.StartConsumer()
}
```

### Separate producer / consumer services

The producer and consumer are different processes. The **consumer** must still wire the codec; a bare import is the minimal wiring because `init()` performs the registration:

```go
import _ "github.com/rachmanzz/fiber-extras/v3/queue-msgpack" // registers application/msgpack via init()
```

Without any registration the consumer fails with `no codec registered for content-type "application/msgpack"`. To skip the registry entirely, decode explicitly:

```go
var dst WelcomeEmail
err := queuemsgpack.Codec().Unmarshal(msg.Payload, &dst) // no registry wiring required
```

---

## ⚡ How It Works (wiring flow)

1. **Producer wires the codec into the message.** `queue.WithCodec(queuemsgpack.Codec())` sets the codec on the message; `Dispatch` marshals the payload with it and writes a `content-type: application/msgpack` header (unless the caller already set one).
2. **Adapters are bystanders.** They only move opaque bytes — no codec knowledge.
3. **Consumer wires the codec into the registry.** `queue.RegisterCodec(queuemsgpack.Codec())` — or the package's `init()` on import — maps `application/msgpack` → the codec.
4. **`queue.Decode` joins the two wires.** It reads the header → looks up the registry (`queue.CodecFor`) → calls `Unmarshal`.

Without `WithCodec`, the queue keeps using JSON exactly as before (fully backward compatible).

---

## 🔌 Adapter Compatibility

| Adapter | Payload storage | MessagePack |
| :--- | :--- | :--- |
| `queue` (in-memory) | pointer, no serialization | ✅ |
| `queue-redis` | inside the JSON envelope (base64) | ✅ |
| `queue-nats` | inside the JSON envelope (base64) | ✅ |
| `queue-rabbitmq` | inside the JSON envelope (base64) | ✅ |
| `queue-postgres` | `payload BYTEA` (raw binary) + `headers JSONB` | ✅ |

> Redis, NATS and RabbitMQ wrap the whole `Message` (including the payload) in a JSON envelope, so the binary payload is base64-encoded on the wire (~33% overhead). PostgreSQL stores the payload as a raw `BYTEA`, giving the full size benefit end-to-end.

---

## ⚠️ Caveats

- **Binary, not human-readable.** MessagePack payloads are opaque in `redis-cli`, the RabbitMQ management UI, or any non-Go consumer. JSON stays friendlier for debugging.
- **Shared schema required.** Cross-language consumers must know the fields/types (and any `msgpack` struct tags you define).
- **Codec wins on conflict.** If you do not set a `content-type` header yourself, `WithCodec` sets it. If you set one explicitly, it is preserved.
- **Dispatch-only.** `WithCodec` only takes effect through `queue.Dispatch`; it is a no-op when you build a `Message` manually and call `queue.Enqueue`.

---

## 📊 Benchmarks (before vs after)

Measured on one realistic probe payload (topic key + 4-key `map[string]string` + 3 scalar fields, encoded as `benchPayload`) with `go test -bench=. -benchmem` on Linux `amd64`. The JSON runs are the "before" baseline, MessagePack the "after":

```text
goarch: amd64
BenchmarkCodec_JSON_Marshal-8           	 1000000	      1008 ns/op	     560 B/op	      11 allocs/op
BenchmarkCodec_Msgpack_Marshal-8        	 1939525	       611.0 ns/op	     560 B/op	       5 allocs/op
BenchmarkCodec_JSON_Unmarshal-8         	  424887	      2828 ns/op	     840 B/op	      23 allocs/op
BenchmarkCodec_Msgpack_Unmarshal-8      	 1196113	       979.9 ns/op	     584 B/op	      14 allocs/op
BenchmarkQueue_Dispatch_JSON-8          	   27219	    193410 ns/op	    1057 B/op	      22 allocs/op
BenchmarkQueue_Dispatch_Msgpack-8       	   18412	    125670 ns/op	    1372 B/op	      18 allocs/op
```

| Operation | JSON | MessagePack | Δ |
| :--- | ---: | ---: | :--- |
| Marshal speed | 1008 ns/op | 611 ns/op | **~39% faster** |
| Unmarshal speed | 2828 ns/op | 980 ns/op | **~65% faster** |
| Encoded size | 194 B | 159 B | **−18%** |
| Dispatch (end-to-end) | 193 µs/op | 126 µs/op | **~35% faster** |

- **Marshal** cuts allocations by more than half (11 → 5); **Unmarshal** from 23 → 14.
- The end-to-end `Dispatch` runs use the in-memory driver, whose per-enqueue `sort.SliceStable` dominates raw cost — the relative gap still favours MessagePack, and both add meaningful binary size savings on the wire.
- Numbers are informational (local hardware); the CI [`bench`](../.github/workflows/bench.yml) workflow replays these benchmarks on every `main` commit.

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
