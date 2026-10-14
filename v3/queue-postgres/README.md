# PostgreSQL Adapter for Queue Core

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue-postgres.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue-postgres)

Transactional PostgreSQL adapter for [`v3/queue`](../queue) utilizing the `SELECT ... FOR UPDATE SKIP LOCKED` pattern, ideal for transactional outbox queueing without requiring extra broker infrastructure.

## Reliability

In-flight messages are guarded by a **visibility timeout (lease)**. A message claimed by a worker is invisible to other consumers while its lease is active; if the worker crashes without acknowledging, the lease expires and the message is **redelivered automatically** — no message is ever stuck in-flight. Configure the lease when constructing the driver:

```go
adapter := queuepostgres.New(pool, queuepostgres.WithVisibilityTimeout(5*time.Minute))
```

The default lease is `DefaultVisibilityTimeout` (3 minutes). On `Ack` the row is **soft-deleted** (`status = 'acked'`) so `Replay` can still surface processed messages for auditing/recovery; the row is only physically removed by `PurgeAcked`. On `Nack` the delivery is released back to `pending` immediately.

### Replay & retention

`Replay(ctx, from)` streams every message created since `from` — including completed (acked) rows — ordered by `created_at`. Because acked rows are retained, run `PurgeAcked` periodically to reclaim storage:

```go
// Drop completed rows acked more than 24h ago.
_, _ = adapter.PurgeAcked(ctx, time.Now().Add(-24*time.Hour))
```

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

This module is part of the [GoSuite](https://github.com/rachmanzz/gosuite) repository and is licensed under the [MIT License](../../LICENSE).
