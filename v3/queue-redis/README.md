# Redis Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-redis.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-redis)

High-performance Redis adapter for [`v3/queue`](../queue) implementing priority queues and scheduled delayed enqueue via Redis Sorted Sets (`ZSet`) and Hashes.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/queue
go get github.com/rachmanzz/fiber-extras/v3/queue-redis
```

---

## 📖 Quickstart

```go
package main

import (
    "context"
    "fmt"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    queueredis "github.com/rachmanzz/fiber-extras/v3/queue-redis"
    "github.com/rachmanzz/fiber-extras/v3/worker"
    "github.com/redis/go-redis/v9"
)

func main() {
    // 1. Connect to Redis
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // 2. Register Redis adapter into Queue Core
    redisAdapter := queueredis.New(rdb, queueredis.WithPrefix("myapp:queue:"))
    queue.RegisterDriver("redis", redisAdapter)
    queue.SetDefaultDriver("redis")

    // 3. Register Topic Handler
    queue.RegisterHandler("notifications:push", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        fmt.Printf("Push notification received: %s\n", string(msg.Payload))
        return nil
    })

    // 4. Start background consumer
    _, _ = queue.StartConsumer()
    defer queue.StopConsumer()

    // 5. Dispatch message with priority
    _ = queue.Dispatch(context.Background(), "notifications:push", map[string]string{
        "title": "New Alert",
    }, queue.WithPriority(queue.PriorityHigh))
}
```

---

## 🔒 Reliability (at-least-once)

Redis has no native acknowledgement, so the adapter supplies one: a message popped by `Dequeue` is moved into an **in-flight ZSet** with a **visibility timeout (lease)**. If the consumer crashes without calling `Ack`, the lease expires and the next `Dequeue` reclaims and redelivers the message. `Ack` removes both the payload and the in-flight entry; `Nack` requeues it atomically.

```go
adapter := queueredis.New(rdb, queueredis.WithVisibilityTimeout(1*time.Minute))
```

The default lease is `DefaultVisibilityTimeout` (30 seconds). Keep it comfortably above your expected processing time to avoid duplicate deliveries; delivery is at-least-once, so handlers should be idempotent.

---

## 📄 License

This module is part of the [GoSuite](https://github.com/rachmanzz/gosuite) repository and is licensed under the [MIT License](../../LICENSE).

---

## 📦 Wire format (split transport)

The Redis record is split so the payload never passes through a text
encoding (no base64 tax) — and there is exactly **one** wire format:

```
hash <prefix>messages_hash:
  <id>:p   ->  payload bytes (raw, zero-copy)
  <id>:m   ->  metadata (msgpack: id, topic, priority, group, headers, ...)
```

Metadata is msgpack-encoded, so header values stay binary-safe (e.g. a poison
message's `raw_payload` round-trips losslessly). There is no legacy format and
no version negotiation: a record that is missing or cannot be decoded is
surfaced as a **poison delivery** (routed to the DLQ) instead of being guessed.

> Deploy note: because old-format records are not decoded, drain the queue (or
> stop producers) while upgrading from a release that wrote the old envelope.
