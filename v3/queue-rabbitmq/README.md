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

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
