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

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
