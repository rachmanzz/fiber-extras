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

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
