# RabbitMQ Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq)

AMQP 0.9.1 RabbitMQ adapter for [`v3/queue`](../queue) supporting hardware-enforced priority queues (`x-max-priority=10`) and manual message acknowledgments.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/queue
go get github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq
```

---

## 📖 Quickstart

```go
package main

import (
    "context"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    queuerabbitmq "github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

func main() {
    adapter, _ := queuerabbitmq.New("amqp://guest:guest@localhost:5672/")
    defer adapter.Close()

    queue.RegisterDriver("rabbitmq", adapter)
    queue.SetDefaultDriver("rabbitmq")

    queue.RegisterHandler("billing:invoice", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        // Process billing invoice
        return nil
    })

    _, _ = queue.StartConsumer()
    defer queue.StopConsumer()
}
```

---

## 🔁 Automatic reconnection

The adapter keeps its AMQP connection alive automatically: a supervisor
watches the connection and channel and re-dials with exponential backoff
(100ms → 10s, configurable via `WithReconnectBackoff`) whenever the broker or
the network closes either.

Semantics after a connection loss:

- Deliveries that were unacked are **requeued by the broker** and redelivered
  (at-least-once). Nothing is lost.
- `Ack`/`Nack` on deliveries from a dropped connection are treated as settled;
  the redelivery is the retry.
- Operations issued while the supervisor is reconnecting return the sentinel
  `queuerabbitmq.ErrReconnecting` — safe to retry.

```go
adapter, err := queuerabbitmq.New("amqp://guest:guest@localhost:5672/",
    queuerabbitmq.WithReconnectBackoff(100*time.Millisecond, 10*time.Second))
```

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).

---

## 📦 Wire format (split transport)

The AMQP Body is the **raw payload** and all metadata travels in AMQP headers
under reserved `fiber-*` keys (`fiber-id`, `fiber-topic`, `fiber-priority`,
`fiber-created-at`, `fiber-attempt`, `fiber-max-attempts`, `fiber-group-id`,
`fiber-group-order`, `fiber-driver`); user headers are packed as JSON into the
single `fiber-headers` entry so arbitrary keys can never collide with the
reserved ones (`x-msg-id` is still set for tooling). There is exactly **one**
wire format and no version negotiation: a record that is missing its envelope
marker or cannot be decoded is surfaced as a **poison delivery** (routed to
the DLQ) instead of being guessed.

> Deploy note: because old-envelope records are not decoded, drain the queue
> (or stop producers) while upgrading from a release that wrote the old
> envelope.
