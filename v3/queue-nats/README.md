# NATS JetStream Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-nats.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-nats)

High-throughput, cloud-native NATS JetStream adapter for [`v3/queue`](../queue) with WorkQueue retention, automatic deduplication, and prioritized pull consumers.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/queue
go get github.com/rachmanzz/fiber-extras/v3/queue-nats
```

---

## 📖 Quickstart

```go
package main

import (
    "context"

    "github.com/nats-io/nats.go"
    "github.com/rachmanzz/fiber-extras/v3/queue"
    queuenats "github.com/rachmanzz/fiber-extras/v3/queue-nats"
    "github.com/rachmanzz/fiber-extras/v3/worker"
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

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).

---

## 📦 Wire format (split transport)

The JetStream message body is the **raw payload** and all metadata travels in
NATS headers under reserved `fiber-*` keys (`fiber-id`, `fiber-topic`,
`fiber-priority`, `fiber-created-at`, `fiber-attempt`, `fiber-max-attempts`,
`fiber-group-id`, `fiber-group-order`, `fiber-driver`); user headers are
prefixed `fiber-h-` so they can never collide with the reserved keys. There is
exactly **one** wire format and no version negotiation: a record that is
missing its envelope marker or cannot be decoded is surfaced as a **poison
delivery** (routed to the DLQ) instead of being guessed.

> Deploy note: because old-envelope records are not decoded, drain the stream
> (or stop producers) while upgrading from a release that wrote the old
> envelope.
