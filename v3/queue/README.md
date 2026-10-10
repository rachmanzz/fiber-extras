# Queue Core for Go & Fiber v3

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/queue.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/queue)

Pluggable, multi-driver asynchronous message queuing orchestrator with strict priority ordering, group-based batching, centralized exponential backoff retries, and dead-letter queue (DLQ) management.

Designed around the **Ports & Adapters (Hexagonal)** architecture:
- **Queue Core (`v3/queue`)**: Defines message models, routing, priority ordering, adaptive drain consumer loops, retry policies, and worker pool execution via [`v3/worker`](../worker). Includes a zero-dependency in-memory driver out of the box.
- **Pluggable Broker Adapters**: Transport drivers (Redis, NATS, RabbitMQ, PostgreSQL) are decoupled into isolated modules, eliminating dependency bloat.

---

## 📦 Installation

```bash
# Core queue engine (includes In-Memory driver)
go get github.com/rachmanzz/fiber-extras/v3/queue

# Optional transport adapters (install only what you need):
go get github.com/rachmanzz/fiber-extras/v3/queue-redis     # Redis Priority ZSet & delayed
go get github.com/rachmanzz/fiber-extras/v3/queue-postgres  # PostgreSQL SKIP LOCKED transactional
go get github.com/rachmanzz/fiber-extras/v3/queue-nats      # NATS JetStream WorkQueue
go get github.com/rachmanzz/fiber-extras/v3/queue-rabbitmq  # RabbitMQ AMQP 0.9.1
```

---

## ⚡ Key Highlights

- **Multi-Driver Hybrid Architecture**: Run multiple brokers concurrently (e.g. PostgreSQL for transactional outbox, Redis for low-latency background jobs).
- **Zero-Dependency Built-in Driver**: Ships with an in-memory priority queue (`NewMemoryDriver()`) for local development and unit tests without external brokers.
- **4-Tier Priority Ordering**: Strict prioritization (`PriorityCritical`, `PriorityHigh`, `PriorityDefault`, `PriorityLow`).
- **Coordinated Group Batching**: FIFO execution per group (`WithGroup(groupID, order)`).
- **Parallel Sub-Worker Fan-Out**: Topic handlers receive `worker.SubWorkerPool` to spawn concurrent sub-tasks with bounded semaphore throttling and fail-fast cancellation.
- **Centralized Retry & DLQ**: Exponential backoff with random jitter; non-retryable errors or exhausted attempts are automatically routed to `<topic>:dlq`.
- **Clean Lifecycle Integration**: Start and stop consumers directly inside `cores.AppContracts` and Fiber v3 shutdown hooks without wrappers.

---

## 📖 1. Quickstart (Pure Go / In-Memory)

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

func main() {
    // 1. Setup in-memory driver & register handler
    queue.SetDriver(queue.NewMemoryDriver())

    queue.RegisterHandler("user:welcome_email", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        fmt.Printf("Sending welcome email to payload: %s\n", string(msg.Payload))
        return nil
    })

    // 2. Start background consumer
    _, _ = queue.StartConsumer()
    defer queue.StopConsumer()

    // 3. Dispatch message
    _ = queue.Dispatch(context.Background(), "user:welcome_email", map[string]string{
        "email": "alice@example.com",
    }, queue.WithPriority(queue.PriorityHigh))

    time.Sleep(100 * time.Millisecond)
}
```

---

## 🔀 2. Multi-Driver Architecture (Hybrid Brokers)

Real-world applications often need different broker guarantees for different tasks:
- **PostgreSQL**: Transactional Outbox where messages must commit atomically with database records (e.g. orders, billing, payments).
- **Redis**: High-throughput, sub-millisecond tasks where raw speed matters (e.g. notifications, webhook delivery, cache warming).
- **NATS / RabbitMQ**: Heavy distributed event streaming across multiple external microservices.

`v3/queue` manages these brokers under a single unified registry without conflicting configurations.

### 2.1 Registering Multiple Drivers

```go
import (
    "github.com/rachmanzz/fiber-extras/v3/queue"
    redisadapter "github.com/rachmanzz/fiber-extras/v3/queue-redis"
    pgadapter "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
    natsadapter "github.com/rachmanzz/fiber-extras/v3/queue-nats"
)

func SetupQueues(pgPool *pgxpool.Pool, rdb *redis.Client, js nats.JetStreamContext) {
    // Initialize adapters
    redisDriver := redisadapter.New(rdb)
    pgDriver    := pgadapter.New(pgPool)
    natsDriver  := natsadapter.New(js)

    // Register into the global manager with identifiers
    queue.RegisterDriver("redis", redisDriver)
    queue.RegisterDriver("postgres", pgDriver)
    queue.RegisterDriver("nats", natsDriver)

    // Set fallback default driver
    queue.SetDefaultDriver("redis")
}
```

### 2.2 Directing Messages to Specific Drivers

There are two routing approaches:

#### Approach A: Automatic Topic-Level Binding (`WithHandlerDriver`)
Bind a topic to a specific driver during registration. Producers only need to specify the topic name; the engine routes it automatically:

```go
// 1. Bind handlers to target drivers
queue.RegisterHandler("billing.payment_processed", handlePayment, 
    queue.WithHandlerDriver("postgres"), // Always routed to PostgreSQL
)

queue.RegisterHandler("notifications.push", handlePushNotification, 
    queue.WithHandlerDriver("redis"),    // Always routed to Redis
)

// 2. Dispatches automatically route to the assigned driver!
queue.Dispatch(ctx, "billing.payment_processed", invoiceData) // -> Enqueued to PostgreSQL
queue.Dispatch(ctx, "notifications.push", pushPayload)        // -> Enqueued to Redis
```

#### Approach B: Explicit Per-Dispatch Override (`WithDriver`)
Override the target driver dynamically on individual dispatch calls:

```go
// Force dispatch to PostgreSQL regardless of topic defaults
queue.Dispatch(ctx, "audit.log", auditEntry, queue.WithDriver("postgres"))

// Force dispatch to NATS JetStream
queue.Dispatch(ctx, "events.exported", exportData, queue.WithDriver("nats"))
```

---

## 🏃 3. Standalone Daemon Execution (`queue.Runner`)

For production architectures where queue workers run in dedicated processes, containers, or Kubernetes pods (e.g. `cmd/queue/main.go`) **without** running the Fiber HTTP server, use **`queue.Runner`**.

### 3.1 Features of `queue.Runner`
- **Signal Trapping**: Intercepts `os.Interrupt`, `syscall.SIGTERM`, and context cancellation.
- **Lifecycle Hooks**: Runs `RegisterBeforeStart` (connect DB/cache) and `RegisterPostShutdown` (close connections) in order.
- **Graceful Draining**: Waits for active in-flight tasks to finish within a configurable timeout.
- **Multi-Driver Supervision**: Automatically manages all registered drivers or accepts explicit custom consumer instances.

### 3.2 Production Standalone Example (`cmd/queue/main.go`)

```go
package main

import (
    "os"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/redis/go-redis/v9"
    "github.com/rachmanzz/fiber-extras/v3/queue"
    redisadapter "github.com/rachmanzz/fiber-extras/v3/queue-redis"
    pgadapter "github.com/rachmanzz/fiber-extras/v3/queue-postgres"
)

func main() {
    // 1. Connect infrastructure
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    pgPool, _ := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/app")

    // 2. Register multi-driver adapters
    queue.RegisterDriver("redis", redisadapter.New(rdb))
    queue.RegisterDriver("postgres", pgadapter.New(pgPool))
    queue.SetDefaultDriver("redis")

    // 3. Register application topic handlers
    registerApplicationHandlers()

    // 4. Create standalone runner
    // Without arguments, Runner supervises all drivers registered above
    runner := queue.NewRunner().Configure(
        queue.WithShutdownTimeout(30 * time.Second),
    )

    // 5. Register lifecycle hooks
    runner.RegisterBeforeStart(func() error {
        // e.g. Ping database or ensure table migrations
        return pgPool.Ping(context.Background())
    })

    runner.RegisterPostShutdown(func() error {
        // Cleanup resources in reverse order
        pgPool.Close()
        return rdb.Close()
    })

    // 6. Start blocking runner loop (stops on SIGINT / SIGTERM)
    if err := runner.Run(); err != nil {
        os.Exit(1)
    }
}
```

### 3.3 Explicit Multi-Consumer Composition

If different drivers require distinct polling intervals, batch sizes, or worker limits, instantiate consumers explicitly:

```go
// Redis: fast polling, large batches
redisConsumer := queue.NewConsumer(redisDriver, router, workerEngine,
    queue.WithPollInterval(10*time.Millisecond),
    queue.WithBatchSize(50),
)

// PostgreSQL: longer polling, smaller batches
pgConsumer := queue.NewConsumer(pgDriver, router, workerEngine,
    queue.WithPollInterval(250*time.Millisecond),
    queue.WithBatchSize(10),
)

// Supervise both consumers concurrently
runner := queue.NewRunner(redisConsumer)
runner.AddConsumer(pgConsumer)

runner.Run()
```

---

## 🚀 4. Full Integration Guide with `fiber-starter`

In projects based on [fiber-starter](https://github.com/rachmanzz/fiber-starter), the queue engine can run in **two modes**:
- **Web Server Mode (In-Process)**: Queue consumers run concurrently within the main Fiber HTTP server process.
- **Standalone Worker Daemon Mode**: Queue consumers run in a dedicated background process (e.g. separate Kubernetes worker pods via `cmd/queue/main.go`).

---

### 4.1 Recommended File Structure in `fiber-starter`

```text
fiber-starter/
├── app/
│   ├── events/
│   │   ├── queue.go              # Central registration for all queue topics
│   │   └── queues/
│   │       ├── email_welcome.go  # Welcome email task handler
│   │       └── report_export.go  # Report export task handler
│   └── services/
│       └── user_service.go       # Dispatches queue messages from business services
├── bootstrap/
│   └── hook.go                   # Driver initialization & lifecycle hooks
└── cmd/
    ├── web/main.go               # HTTP server entrypoint
    └── queue/main.go             # Standalone worker daemon entrypoint (optional)
```

---

### 4.2 Initialization in `bootstrap/hook.go` (Web Server Mode)

Attach the broker adapter and register topics inside the application's lifecycle hooks:

```go
package bootstrap

import (
    "github.com/rachmanzz/fiber-extras/v3/queue"
    redisadapter "github.com/rachmanzz/fiber-extras/v3/queue-redis"
    "github.com/rachmanzz/fiber-starter/app/events"
    "github.com/rachmanzz/fiber-starter/cores"
    redisprovider "github.com/rachmanzz/fiber-starter/providers/redis"
)

func RegisterHook(core *cores.AppContracts) {
    if core.App == nil {
        return
    }

    core.RegisterBeforeStart(func() error {
        // 1. Initialize broker connection (e.g. Redis)
        if cores.Config().Redis.Enable {
            redisprovider.Connect()
            rdb := redisprovider.Client()
            driver := redisadapter.New(rdb, cores.Config().Redis.Prefix)
            queue.SetDriver(driver)
        } else {
            // Fallback to in-memory driver when Redis is disabled
            queue.SetDriver(queue.NewMemoryDriver())
        }

        // 2. Register all application topic handlers
        events.RegisterQueueTopics()

        // 3. Start queue consumer(s)
        _, err := queue.StartConsumer()
        return err
    })

    // 4. Drain and stop queue consumers on Fiber shutdown
    core.App.Hooks().OnPostShutdown(func(err error) error {
        queue.StopConsumer()
        return nil
    })
}
```

---

### 4.3 Writing Topic Handlers in `app/events/queues/`

Create modular task handlers under `app/events/queues/`:

```go
package queues

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

const TopicEmailWelcome = "user.welcome_email"

type WelcomeEmailPayload struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

func WelcomeEmailQueueHandler() queue.Handler {
    return func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        var payload WelcomeEmailPayload
        if err := json.Unmarshal(msg.Payload, &payload); err != nil {
            // Malformed payloads should not retry, route directly to DLQ
            return queue.MarkNonRetryable(err)
        }

        fmt.Printf("Sending welcome email to %s (UserID: %s)\n", payload.Email, payload.UserID)
        return nil
    }
}
```

Register handlers in `app/events/queue.go`:

```go
package events

import (
    "github.com/rachmanzz/fiber-extras/v3/queue"
    "github.com/rachmanzz/fiber-starter/app/events/queues"
)

func RegisterQueueTopics() {
    queue.RegisterHandler(queues.TopicEmailWelcome, queues.WelcomeEmailQueueHandler())
    // register additional topics...
}
```

---

### 4.4 Dispatching Messages from Services (`app/services/`)

In controllers or domain services, call `queue.Dispatch`:

```go
package services

import (
    "context"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    "github.com/rachmanzz/fiber-starter/app/events/queues"
)

type UserService struct{}

func (s *UserService) Register(ctx context.Context, email, name string) error {
    // 1. Persist user into database...
    userID := "usr_12345"

    // 2. Dispatch asynchronous message to the queue
    err := queue.Dispatch(ctx, queues.TopicEmailWelcome, queues.WelcomeEmailPayload{
        UserID: userID,
        Email:  email,
    }, queue.WithPriority(queue.PriorityHigh))
    if err != nil {
        return err
    }

    return nil
}
```

---

### 4.5 Standalone Worker Daemon Mode (`cmd/queue/main.go`)

If you want to run worker consumers in separate containers or pods without serving HTTP requests:

```go
package main

import (
    "context"
    "os"
    "time"

    "github.com/rachmanzz/fiber-extras/v3/queue"
    redisadapter "github.com/rachmanzz/fiber-extras/v3/queue-redis"
    "github.com/rachmanzz/fiber-starter/app/events"
    "github.com/rachmanzz/fiber-starter/cores"
    redisprovider "github.com/rachmanzz/fiber-starter/providers/redis"
)

func main() {
    // 1. Initialize core configuration and logger
    cores.NewLogger()
    redisprovider.Connect()
    rdb := redisprovider.Client()

    // 2. Attach driver and register topic handlers
    queue.SetDriver(redisadapter.New(rdb, cores.Config().Redis.Prefix))
    events.RegisterQueueTopics()

    // 3. Launch standalone Runner with graceful shutdown
    runner := queue.NewRunner().Configure(
        queue.WithShutdownTimeout(30 * time.Second),
    )

    runner.RegisterPostShutdown(func() error {
        return redisprovider.Close()
    })

    if err := runner.Run(); err != nil {
        os.Exit(1)
    }
}
```

---

## 🔁 5. Retry Policy & Dead Letter Queue (DLQ)

Failed messages automatically undergo exponential backoff retries with jitter:

```go
customRetry := queue.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     10 * time.Second,
    Multiplier:   2.0,
    Jitter:       true,
}

consumer := queue.NewConsumer(driver, router, dispatcher,
    queue.WithRetryPolicy(customRetry),
)
```

### Non-Retryable Errors
If a handler returns an error marked as non-retryable via `queue.MarkNonRetryable(err)` (or if no handler exists for the topic), the message **bypasses retry attempts** and is routed immediately to `<topic>:dlq`.

---

## 🎯 6. Priority & Group Ordering

### Priorities
```go
queue.Dispatch(ctx, "tasks.urgent", data, queue.WithPriority(queue.PriorityCritical))
queue.Dispatch(ctx, "tasks.standard", data, queue.WithPriority(queue.PriorityDefault))
queue.Dispatch(ctx, "tasks.cleanup", data, queue.WithPriority(queue.PriorityLow))
```

### Group Ordering (FIFO per Key)
Messages with identical `GroupID` are processed sequentially in `GroupOrder` sequence:

```go
// Sequential processing for tenant "tenant-101"
queue.Dispatch(ctx, "sync.job", part1, queue.WithGroup("tenant-101", 1))
queue.Dispatch(ctx, "sync.job", part2, queue.WithGroup("tenant-101", 2))
```

---

## 📊 Benchmarks

Measured using Go's standard benchmarking framework (`go test -bench=. -benchmem`) on Linux `amd64` (11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz):

```text
goos: linux
goarch: amd64
pkg: github.com/rachmanzz/fiber-extras/v3/queue
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkQueue_Memory_EnqueueDequeue-8   	 5186100	       211.9 ns/op	      80 B/op	       4 allocs/op
BenchmarkQueue_Message_Creation-8        	 1528195	       780.2 ns/op	     392 B/op	       9 allocs/op
BenchmarkQueue_Dispatch_JSON-8           	   20395	    117774 ns/op	     528 B/op	      12 allocs/op
```

- **In-Memory Priority Throughput (`EnqueueDequeue`)**: Over **5.1 million ops/sec** at **211.9 ns/op** and 80 B/op, confirming zero lock contention on memory driver operations.
- **Message Model Instantiation**: Over **1.5 million msgs/sec** with functional options (`WithPriority`, `WithMaxAttempts`, `WithGroup`).
- **End-to-End JSON Dispatch**: ~20,000 dispatches/sec incorporating runtime JSON serialization, topic routing validation, and adapter storage.

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).

