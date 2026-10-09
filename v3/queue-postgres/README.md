# PostgreSQL Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-postgres.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-postgres)

Transactional PostgreSQL adapter for [`v3/queue`](../queue) utilizing the `SELECT ... FOR UPDATE SKIP LOCKED` pattern, ideal for transactional outbox queueing without requiring extra broker infrastructure.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/queue
go get github.com/rachmanzz/fiber-extras/v3/queue-postgres
```

---

## 📖 Quickstart

```go
package main

import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/rachmanzz/fiber-extras/v3/queue"
    queuepostgres "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

func main() {
    ctx := context.Background()
    pool, _ := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/mydb")
    defer pool.Close()

    adapter := queuepostgres.New(pool)
    _ = adapter.EnsureTable(ctx)

    queue.RegisterDriver("postgres", adapter)
    queue.SetDefaultDriver("postgres")

    queue.RegisterHandler("billing:audit", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        // Process message inside postgres queue
        return nil
    })

    _, _ = queue.StartConsumer()
    defer queue.StopConsumer()
}
```

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
