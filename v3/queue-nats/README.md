# NATS JetStream Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/gosuite/v3/queue-nats.svg)](https://pkg.go.dev/github.com/rachmanzz/gosuite/v3/queue-nats)

High-throughput, cloud-native NATS JetStream adapter for [`v3/queue`](../queue) with WorkQueue retention, automatic deduplication, and prioritized pull consumers.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/gosuite/v3/queue
go get github.com/rachmanzz/gosuite/v3/queue-nats
```

---

## 📖 Quickstart

```go
package main

import (
    "context"

    "github.com/nats-io/nats.go"
    "github.com/rachmanzz/gosuite/v3/queue"
    queuenats "github.com/rachmanzz/gosuite/v3/queue-nats"
    "github.com/rachmanzz/gosuite/v3/worker"
)

func main() {
    nc, _ := nats.Connect(nats.DefaultURL)
    defer nc.Close()

    adapter, _ := queuenats.New(nc, queuenats.WithStream("MY_STREAM"))
    queue.RegisterDriver("nats", adapter)
    queue.SetDefaultDriver("nats")

    queue.RegisterHandler("logs:ingest", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        // Process message
        return nil
    })

    _, _ = queue.StartConsumer()
    defer queue.StopConsumer()
}
```

---

## Retry, Backpressure, and Deduplication

`Message.Attempt` counts handler failures recorded by the core. Broker delivery
counts do not change it: memory admission rejection, worker queue rejection,
group deferral, and lease recovery leave the published attempt unchanged.

After a handler failure, the adapter publishes an envelope containing the updated
attempt before acknowledging its original delivery. Logical `Message.ID` stays
stable. `Nats-Msg-Id` uses a separate deterministic identity for each nonzero
attempt, so the retry is distinct from the original publication while replaying
the same retry attempt still deduplicates within the stream's configured window.
Publish or Ack failures can still lead to duplicate handler execution; handlers
should be idempotent. If retry publication fails, the original is not acknowledged.
Undecodable records also use a separate poison-publication ID; the source IDs
are retained in `source_message_id` and `source_publication_id` headers.

Delayed handler retries are published immediately with a reserved
`fiber-not-before` timestamp. `Dequeue` keeps future retries on JetStream using
`NakWithDelay`, and exposes them to handlers only once due. Their payload, due
time, and attempt state survive closing/recreating the adapter; no local timer
holds the only retry copy. Deferral without a new handler failure uses native
NAK on the existing broker message.

Upgrade all consumers of a stream before relying on the new delayed-retry
header: older consumers do not enforce `fiber-not-before`. Retries already
scheduled by an older version's native NAK retain their old published attempt
state, so drain existing retries before changing the attempt-accounting policy.

---

## 🧪 Dependencies

`github.com/nats-io/nats-server/v2` is required **only by the test suite**, which
spins up an in-process embedded JetStream server for broker-less tests
(`TestNATSAdapter_Live_*` fall back to it when `TEST_NATS_URL` is unset). It is
not imported by any package of this module, so downstream consumers never
compile or download it. Live tests can instead target an external broker:

```bash
TEST_NATS_URL=nats://127.0.0.1:4222 go test ./...
```

---

## ⚡ Context-Aware I/O & Subject Sanitization

- **Context-Aware I/O**: Each priority fetch uses a cancellable child context, capped at 20 ms. Short caller deadlines are shared among the remaining priorities, so an empty critical/high consumer cannot consume the entire polling budget. A local fetch timeout advances to the next priority; caller cancellation or expiry remains an error. Partially filled batches can return without waiting for the full caller deadline.
- **Subject Sanitization**: Queue topics are automatically sanitized into valid NATS JetStream subject tokens by replacing invalid whitespace with dots (`.`), while preserving wildcard tokens (`*` and `>`).

---

## 📄 License

This module is part of the [GoSuite](https://github.com/rachmanzz/gosuite) repository and is licensed under the [MIT License](../../LICENSE).

---

## 📦 Wire format (split transport)

The JetStream message body is the **raw payload** and all metadata travels in
NATS headers under reserved `fiber-*` keys (`fiber-id`, `fiber-topic`,
`fiber-priority`, `fiber-created-at`, `fiber-attempt`, `fiber-max-attempts`,
`fiber-group-id`, `fiber-group-order`, `fiber-driver`, and the optional
`fiber-not-before` due time); user headers are
prefixed `fiber-h-` so they can never collide with the reserved keys. There is
exactly **one** wire format and no version negotiation: a record that is
missing its envelope marker or cannot be decoded is surfaced as a **poison
delivery** (routed to the DLQ) instead of being guessed.

If poison publication or settlement fails during a fetched batch, `Dequeue`
returns any valid prefix alongside the error. The caller must settle those
returned deliveries. The failed record and unvisited tail are NAKed for
redelivery; cleanup failures are included in the error and fall back to
JetStream lease recovery.

> Deploy note: because old-envelope records are not decoded, drain the stream
> (or stop producers) while upgrading from a release that wrote the old
> envelope.
