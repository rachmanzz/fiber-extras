# Queue Core for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/gosuite/v3/queue.svg)](https://pkg.go.dev/github.com/rachmanzz/gosuite/v3/queue)

Pluggable, multi-driver asynchronous message queuing orchestrator with strict priority ordering, group-based batching, centralized exponential backoff retries, and dead-letter queue (DLQ) management.

Designed around the **Ports & Adapters (Hexagonal)** architecture:
- **Queue Core (`v3/queue`)**: Defines message models, routing, priority ordering, adaptive drain consumer loops, retry policies, and worker pool execution via [`v3/worker`](../worker). Includes a zero-dependency in-memory driver out of the box.
- **Pluggable Broker Adapters**: Transport drivers (Redis, NATS, RabbitMQ, PostgreSQL) are decoupled into isolated modules, eliminating dependency bloat.

---

## 📦 Installation

```bash
# Core queue engine (includes In-Memory driver)
go get github.com/rachmanzz/gosuite/v3/queue

# Optional transport adapters (install only what you need):
go get github.com/rachmanzz/gosuite/v3/queue-redis     # Redis Priority ZSet & delayed
go get github.com/rachmanzz/gosuite/v3/queue-postgres  # PostgreSQL SKIP LOCKED transactional
go get github.com/rachmanzz/gosuite/v3/queue-nats      # NATS JetStream WorkQueue
go get github.com/rachmanzz/gosuite/v3/queue-rabbitmq  # RabbitMQ AMQP 0.9.1
go get github.com/rachmanzz/gosuite/v3/queue-msgpack   # Optional MessagePack payload codec
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

    "github.com/rachmanzz/gosuite/v3/queue"
    "github.com/rachmanzz/gosuite/v3/worker"
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

### 1.1 Important: Registration Behavior & Concurrency Patterns

> [!IMPORTANT]
> **Handler Registration & Overwrite Protection:**
> - `queue.RegisterHandler(topic, handler)` binds a handler to a topic on the global router.
> - **Collision Protection**: If the same topic is registered more than once, `RegisterHandler` / `router.Register` immediately panics to prevent accidental silent route collisions across modular services.
> - **Intentional Overwrites**: To intentionally override an existing handler (such as in unit tests or mocks), provide `queue.WithAllowOverwrite()`:
>   ```go
>   queue.RegisterHandler("orders.created", mockHandler, queue.WithAllowOverwrite())
>   ```
> - **Best Practice**: All handlers should be registered **once during application bootstrap / startup** (e.g. in `bootstrap/hook.go` or `init()`). Avoid calling `RegisterHandler` inside dynamic request handlers or concurrent worker loops.

#### Global Convenience API vs. Instance-Based API

`v3/queue` provides two operational patterns to support both simple application services and complex multi-tenant workloads:

| Pattern | API Methods | Best For | Concurrency Behavior |
| :--- | :--- | :--- | :--- |
| **Global Convenience API** | `queue.RegisterHandler`, `queue.SetDriver`, `queue.StartConsumer`, `queue.StopConsumer` | Typical web applications, HTTP APIs, and single-tenant microservices | Process-wide singleton. Handlers and drivers are registered once during startup. Calling `StopConsumer()` stops all global consumers. |
| **Instance-Based API** | `queue.NewRouter()`, `queue.NewConsumer()`, `queue.NewMemoryDriver()` | Multi-tenant services, concurrent episodic scans, isolated sub-systems, or tests | Completely decoupled. Each consumer maintains its own isolated router, drivers, and lifecycle with zero cross-job interference. |

**Instance-Based Example (Safe for Concurrent Dynamic Jobs / Scans):**
```go
// Create completely isolated queue instances per job/scan without touching global state
driver := queue.NewMemoryDriver()
router := queue.NewRouter()
router.Register("scan:file", myFileHandler)

consumer := queue.NewConsumer(driver, router, myWorkerEngine)
consumer.Start()
defer consumer.Stop() // Safely drains and stops only this specific consumer instance
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
    "github.com/rachmanzz/gosuite/v3/queue"
    redisadapter "github.com/rachmanzz/gosuite/v3/queue-redis"
    pgadapter "github.com/rachmanzz/gosuite/v3/queue-postgres"
    natsadapter "github.com/rachmanzz/gosuite/v3/queue-nats"
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
    "github.com/rachmanzz/gosuite/v3/queue"
    redisadapter "github.com/rachmanzz/gosuite/v3/queue-redis"
    pgadapter "github.com/rachmanzz/gosuite/v3/queue-postgres"
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
    "github.com/rachmanzz/gosuite/v3/queue"
    redisadapter "github.com/rachmanzz/gosuite/v3/queue-redis"
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
            driver := redisadapter.New(rdb, redisadapter.WithPrefix(cores.Config().Redis.Prefix))
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

    "github.com/rachmanzz/gosuite/v3/queue"
    "github.com/rachmanzz/gosuite/v3/worker"
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
    "github.com/rachmanzz/gosuite/v3/queue"
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

    "github.com/rachmanzz/gosuite/v3/queue"
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

    "github.com/rachmanzz/gosuite/v3/queue"
    redisadapter "github.com/rachmanzz/gosuite/v3/queue-redis"
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
    queue.SetDriver(redisadapter.New(rdb, redisadapter.WithPrefix(cores.Config().Redis.Prefix)))
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
    MaxAttempts: 5,
    Initial:     100 * time.Millisecond,
    Max:         10 * time.Second,
    Multiplier:  2.0,
    Jitter:      0.2,
}

consumer := queue.NewConsumer(driver, router, dispatcher,
    queue.WithRetryPolicy(customRetry),
)
```

### Crash-Safe Retry
Retries are rescheduled **before** the current delivery is acknowledged whenever the adapter implements `queue.Reenqueuer` (all in-repo adapters do). This folds the reschedule into the delivery itself, so a crash between acking the old delivery and re-enqueuing the retry copy cannot lose the message — the worst case is a duplicate, never a loss.

### Non-Retryable Errors
If a handler returns an error marked as non-retryable via `queue.MarkNonRetryable(err)` (or if no handler exists for the topic), the message **bypasses retry attempts** and is routed immediately to `<topic>:dlq`.

### Lease & Delivery Sentinel (`queue.ErrDeliveryLost`)
Broker operations (`Ack`, `Nack`, `Reenqueue`) return the canonical `queue.ErrDeliveryLost` sentinel error if the message's visibility timeout (lease) expired while processing and was reclaimed/redelivered by another worker, or if connection was dropped. Callers can safely check this uniformly across all adapters:
```go
if errors.Is(err, queue.ErrDeliveryLost) {
    // Delivery token is no longer owned (lease expired or connection lost)
}
```

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

#### Cross-Poll Group Quarantine
If an upstream group member fails and is scheduled for retry with exponential backoff:
1. Any remaining downstream members within the **same batch** are delayed by the matching backoff window.
2. If new messages for the same `GroupID` arrive in **subsequent polling ticks** while the upstream member is still waiting for its retry backoff, the consumer automatically quarantines the incoming batch by delaying it for the remainder of the quarantine window. This prevents cross-poll FIFO inversion and guarantees strict sequential execution.

---

## 🧩 7. Payload Codecs (JSON / MessagePack)

Payload serialization is pluggable through the `queue.Codec` interface. JSON is the built-in default; any other format can be registered under its own `content-type` and selected per dispatch.

```go
// Optional MessagePack codec (separate module, zero impact unless imported)
import queuemsgpack "github.com/rachmanzz/gosuite/v3/queue-msgpack"

queue.Dispatch(ctx, "user:welcome_email", payload,
    queue.WithCodec(queuemsgpack.Codec()), // serialized as MessagePack
)

// Consumers decode without knowing which codec was used:
var out WelcomeEmail
err := queue.Decode(msg, &out) // resolves the codec from the content-type header
```

- **Codec-agnostic adapters**: the payload boundary is per-message, so every driver (memory, Redis, NATS, RabbitMQ, PostgreSQL) works unchanged.
- **Header-driven decode**: `queue.Decode` resolves the codec from the `content-type` header, so the consumer process must import the codec package (blank import is fine) or decode explicitly with the codec.
- **Custom codecs**: implement `queue.Codec` and `queue.RegisterCodec(myCodec)`.
- **Backward compatible**: messages without a codec default to JSON.

See [`v3/queue-msgpack`](../queue-msgpack) for a complete example.

---

## 📊 Benchmarks

Historical figures from the earlier storage implementation (not the current heap scheduler), measured using Go's standard benchmarking framework (`go test -bench=. -benchmem`) on Linux `amd64` (11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz):

```text
goos: linux
goarch: amd64
pkg: github.com/rachmanzz/gosuite/v3/queue
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkQueue_Memory_EnqueueDequeue-8   	17391493	       137.6 ns/op	      56 B/op	       3 allocs/op
BenchmarkQueue_Message_Creation-8        	 6782758	       345.2 ns/op	     288 B/op	       4 allocs/op
BenchmarkQueue_Dispatch_JSON-8           	 2691786	       985.3 ns/op	     378 B/op	       5 allocs/op
```

- **Memory driver**: rerun `go test -run=^$ -bench=BenchmarkQueue_Memory_EnqueueDequeue -benchmem` for current heap scheduler figures. The historical numbers above do not establish concurrent lock contention or current throughput.
- **Message Model Instantiation**: Over **6.8 million msgs/sec** with functional options (`WithPriority`, `WithMaxAttempts`, `WithGroup`).
- **End-to-End JSON Dispatch**: ~2.7 million dispatches/sec incorporating runtime JSON serialization, topic routing validation, and adapter storage.

---

## 📄 License

This module is part of the [GoSuite](https://github.com/rachmanzz/gosuite) repository and is licensed under the [MIT License](../../LICENSE).


## Optional memory budget

A memory budget bounds **reserved bytes**, rather than process RSS or measured Go
heap usage. It is disabled unless `WithMemoryBudget` is supplied and adds no new
module dependencies. Multiple consumers can share the same budget.

```go
budget := queue.NewMemoryBudget(500 * 1024 * 1024) // 500 MiB
consumer := queue.NewConsumer(driver, router, dispatcher,
    queue.WithMemoryBudget(budget),
    queue.WithMemoryEstimate(8 * 1024 * 1024), // anticipated total bytes per delivery
)
consumer.Start()
```

Admission reserves the estimate **before dequeue**, then accounts for at least the
payload's backing capacity, message strings, headers, and a fixed envelope
allowance. The default estimate is 64 KiB, capped at the configured budget limit. An explicit
`WithMemoryEstimate` is never reduced: if it exceeds the limit, admission logs an
error and pauses polling before dequeue. Increase the limit or correct the
estimate; polling resumes automatically once the configured estimate fits. This
also applies after shrinking the budget with `SetLimit`.
Reserve enough for normal handler decoding and processing buffers; the consumer
cannot infer arbitrary allocations made by application code.

While admission waits for capacity, polling pauses without dequeuing messages.
With a budget enabled, each dequeue requests **one delivery**, regardless of
`WithBatchSize`, so a full batch is not materialized before admission. If the
returned message needs more bytes than are currently available, it is Nacked and
the adaptive drain stops until the next poll interval. This does not increment
its handler attempt count. A message larger than the entire budget is routed to
DLQ instead of waiting forever. The adapter may prefetch internally; its
prefetch buffers are outside this accounting.

Claims cover queued tasks, pending group batches, execution, joined sub-workers,
and payloads retained by core fallback retry timers. Claims are reclaimed on
success, failure, panic recovery, dispatch rejection, and shutdown after work
finishes. Driver-managed delayed messages and the MemoryDriver backlog are
outside the consumer budget, as are adapter tokens and other process memory.
Set broker/prefetch limits separately when necessary.

Handlers and sub-workers receive the budget through their context:

```go
router.Register("resize", func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
    // Reject immediately if the additional buffer cannot be reserved.
    extra, err := queue.TryClaimMemory(ctx, 32 * 1024 * 1024)
    if err != nil {
        return err
    }
    defer extra.Close() // also automatically closed at the end of this delivery

    buffer := make([]byte, 32 * 1024 * 1024)
    _ = buffer // process data, then stop retaining it before releasing its claim
    return nil
})
```

`ClaimMemory(ctx, bytes)` waits for capacity and respects context cancellation.
Prefer `TryClaimMemory` or reserve the full expected working set at admission:
waiting for extra capacity while holding existing claims can deadlock handlers
that are all waiting for one another to release memory. A blocking claim inside
an ordered group must also not depend on releasing a later member's claim.

For partial reclaim, call `reservation.Release(bytes)` once those bytes are no
longer retained. `MemoryReservationFromContext(ctx)` exposes the admission
reservation, whose `TryGrow(bytes)` reserves more without waiting. Automatic
cleanup is a safety net; callers must not continue using allocated buffers after
releasing the bytes that account for them.

Standalone reservations are also available:

```go
reservation, err := budget.Claim(ctx, 16 * 1024 * 1024) // blocking, cancellable
if err != nil {
    return err
}
defer reservation.Close() // standalone claims require explicit cleanup

// budget.TryClaim(bytes) rejects immediately when there is no capacity.
// reservation.Release(bytes) reclaims part of a claim.
// reservation.Close() releases the remainder and is idempotent.
// budget.Used(), budget.Available(), budget.Limit() expose accounting.
// budget.SetLimit(bytes) adjusts the limit; shrinking below Used is rejected.
```

Invalid sizes and claims larger than the complete budget fail immediately.
Reclaim changes the budget counter; it does not force garbage collection or
immediate release of RAM to the operating system. The application remains
responsible for its overall runtime/container memory configuration.


### Group retries and ordering

Within one consumer, a failed or unsettled group member remains the retry head.
Later members wait until that head is successfully acknowledged or transferred
to DLQ; expiration of the backoff alone does not release the group. Failed Ack
or failed DLQ publication also retains the barrier. Idle group state is removed
after settlement, and pending barriers are cleared on consumer shutdown.

If the broker never redelivers an unsettled head, the group remains blocked until
that delivery is recovered or the consumer is restarted. Ordering coordination
is local to the consumer; it does not coordinate multiple processes.


A retry head is identified by its original topic and message ID together. A DLQ
copy that preserves the same ID cannot acknowledge or release that original
head. Oversized retry messages rejected by memory admission participate in the
same serialized settlement transition: successful original Ack after DLQ
publication releases the barrier, while settlement failure retains it.


### Bounded oversized settlement and multiple pending group members

Oversized admission submits a settlement operation to the same group drain as
normal execution. The poller never waits on an active handler's group mutex.
There is one oversized settlement slot per consumer, shared across groups and
independent deliveries. The slot is released on completion or dispatch rejection;
additional oversized deliveries are Nacked without consuming handler attempts.
At saturation, progress for other broker messages still depends on the broker's
redelivery ordering and backpressure behavior.

This slot is a bounded exception to reservation accounting: Dequeue has already
materialized an oversized message that cannot fit the configured budget. At most
one such payload per consumer is retained for queued/active settlement, outside
the normal byte claim (plus any transient message currently being dequeued).
Adapter buffers and the MemoryDriver backlog remain outside consumer accounting.
The byte budget is therefore still not a hard RSS ceiling.

A group retains every known unsettled topic/ID, rather than only its current
retry head. The earliest GroupOrder is the active barrier; equal orders follow
local admission sequence. Once that member settles, the next unresolved member
is promoted, including members rejected due to memory admission or a busy
settlement slot. Earlier queued batches may still execute before later barriers.

During Stop, polling is cancelled and joined before pending retries are flushed,
so flushed jobs cannot be immediately dequeued again during shutdown. Accepted
settlement operations are joined before the driver is closed.


### MemoryDriver ordering and pre-execution backpressure

MemoryDriver uses one message heap per group (with one shared bucket for
independent messages), plus a heap that selects among eligible bucket heads.
The existing priority-first policy is retained: higher priority precedes lower
priority; within the same priority a group uses ascending GroupOrder. FIFO by
CreatedAt and enqueue sequence provides stable ties. Each enqueue/dequeue uses
heap operations instead of a comparator that changes rules across message pairs.

Consumer records each received group message's original topic/ID and admission
position before checking memory or handing it to a worker. Temporary memory
pressure and worker queue rejection preserve this state while Nacking the
payload back to the broker, without consuming a handler attempt. Redelivery
keeps the original position, including when GroupOrder is the default zero.
Already-known later members remain deferred until earlier originals settle.

Admission metadata stays until settlement, recovery, or Stop; it does not hold
the rejected payload. This remains local ordering of known deliveries, not
coordination across processes or a guarantee about messages the broker has not
yet returned to the consumer.
