# Queue Core for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/gosuite/v3/queue.svg)](https://pkg.go.dev/github.com/rachmanzz/gosuite/v3/queue)

Pluggable, multi-driver asynchronous message queuing orchestrator with adapter-dependent priority ordering, group-based batching, centralized exponential backoff retries, and dead-letter queue (DLQ) management.

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

- **Multi-Driver Hybrid Architecture**: Run multiple brokers concurrently (e.g. PostgreSQL for durable jobs, Redis for low-latency background jobs).
- **Zero-Dependency Built-in Driver**: Ships with an in-memory priority queue (`NewMemoryDriver()`) for local development and unit tests without external brokers.
- **4-Tier Priority Ordering**: Priority levels (`PriorityCritical`, `PriorityHigh`, `PriorityDefault`, `PriorityLow`) with guarantees determined by the adapter (see section 6).
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
| **Instance-Based API** | `queue.NewRouter()`, `queue.NewConsumer()`, `queue.NewMemoryDriver()` | Multi-tenant services, concurrent episodic scans, isolated sub-systems, or tests | Each consumer has independent internal state. Router, driver, dispatcher, and memory budget references may be shared; resource isolation depends on what the caller supplies. |

**Instance-Based Example (Safe for Concurrent Dynamic Jobs / Scans):**
```go
// Supply separate resources for each isolated job/scan
driver := queue.NewMemoryDriver()
router := queue.NewRouter()
router.Register("scan:file", myFileHandler)

engine := worker.New().Start()
defer engine.Shutdown()

consumer := queue.NewConsumer(driver, router, engine)
consumer.Start()
defer consumer.Stop() // Runs before engine.Shutdown and closes this driver
```

The snippet uses the `queue` and `worker` imports from the quickstart and an application-defined `myFileHandler`. A nil dispatcher selects the global worker dispatcher. `Consumer.Stop` closes its driver but does not shut down the supplied dispatcher; sharing a driver therefore requires coordinated lifecycle ownership. Shared dispatchers and budgets also share worker capacity and memory admission limits.

---

## 🔀 2. Multi-Driver Architecture (Hybrid Brokers)

Real-world applications often need different broker guarantees for different tasks:
- **PostgreSQL**: Durable PostgreSQL-backed queue for jobs stored in the database. The adapter writes through `*pgxpool.Pool`; `queue.Dispatch` does not join the caller's transaction. Atomic transactional outbox writes require explicit application transaction integration, which this adapter API does not provide.
- **Redis**: High-throughput, sub-millisecond tasks where raw speed matters (e.g. notifications, webhook delivery, cache warming).
- **NATS / RabbitMQ**: Heavy distributed event streaming across multiple external microservices.

`v3/queue` manages these brokers under a single unified registry without conflicting configurations.

### 2.1 Registering Multiple Drivers

```go
import (
    "context"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/nats-io/nats.go"
    "github.com/redis/go-redis/v9"

    "github.com/rachmanzz/gosuite/v3/queue"
    redisadapter "github.com/rachmanzz/gosuite/v3/queue-redis"
    pgadapter "github.com/rachmanzz/gosuite/v3/queue-postgres"
    natsadapter "github.com/rachmanzz/gosuite/v3/queue-nats"
)

func SetupQueues(ctx context.Context, pgPool *pgxpool.Pool, rdb *redis.Client, nc *nats.Conn) error {
    // Initialize adapters
    redisDriver := redisadapter.New(rdb)
    pgDriver    := pgadapter.New(pgPool)
    if err := pgDriver.EnsureTable(ctx); err != nil {
        return err
    }
    natsDriver, err := natsadapter.New(nc)
    if err != nil {
        return err
    }

    // Register into the global manager with identifiers
    queue.RegisterDriver("redis", redisDriver)
    queue.RegisterDriver("postgres", pgDriver)
    queue.RegisterDriver("nats", natsDriver)

    // Set fallback default driver
    return queue.SetDefaultDriver("redis")
}
```

The PostgreSQL schema must exist before dispatch or consumption. This example uses `EnsureTable` to create/update it and returns any DDL error. Deployments that manage schema through migrations can provision the equivalent schema separately and omit this call; creating the driver or pinging its pool does not create tables.

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
- **Lifecycle Hooks**: Runs `RegisterBeforeStart` and `RegisterPostShutdown` hooks in registration order. Post-shutdown hooks also run after startup failure or a shutdown timeout; they are not proof that all handlers stopped.
- **Graceful Draining**: Waits for consumers to stop up to a configurable timeout, then returns `ErrShutdownTimeout` if they are still stopping. Timeout does not forcibly terminate handlers.
- **Multi-Driver Supervision**: Automatically manages all registered drivers or accepts explicit custom consumer instances.

### 3.2 Production Standalone Example (`cmd/queue/main.go`)

```go
package main

import (
    "context"
    "errors"
    "log"
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
    pgPool, err := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/app")
    if err != nil {
        _ = rdb.Close()
        log.Print(err)
        os.Exit(1)
    }

    // 2. Register multi-driver adapters
    queue.RegisterDriver("redis", redisadapter.New(rdb))
    pgDriver := pgadapter.New(pgPool)
    queue.RegisterDriver("postgres", pgDriver)
    queue.SetDefaultDriver("redis")

    // 3. Register application topic handlers (application-defined function)
    registerApplicationHandlers()

    // 4. Create standalone runner
    // Without arguments, Runner supervises all drivers registered above
    runner := queue.NewRunner().Configure(
        queue.WithShutdownTimeout(30 * time.Second),
    )

    // 5. Register lifecycle hooks
    runner.RegisterBeforeStart(func() error {
        // Prepare the schema before starting consumers; fail startup on error.
        return pgDriver.EnsureTable(context.Background())
    })

    // 6. Start blocking runner loop (stops on SIGINT / SIGTERM)
    err = runner.Run()
    if errors.Is(err, queue.ErrShutdownTimeout) {
        // Handlers may still use DB/cache. End this dedicated process without
        // closing their clients underneath them; the OS releases resources.
        log.Print(err)
        os.Exit(1)
    }

    // Startup failed before consumers started, or all consumers stopped.
    pgPool.Close()
    if err := errors.Join(err, rdb.Close()); err != nil {
        log.Print(err)
        os.Exit(1)
    }
}
```

### 3.3 Explicit Multi-Consumer Composition

Instantiate consumers explicitly for distinct polling intervals or batch sizes. This example shares `workerEngine`; for separate worker limits, supply a separate started engine to each consumer and shut each engine down after its consumer stops:

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

type payloadError struct { error }

func (payloadError) Retryable() bool { return false }
func (e payloadError) Unwrap() error { return e.error }

func WelcomeEmailQueueHandler() queue.Handler {
    return func(ctx context.Context, msg *queue.Message, pool worker.SubWorkerPool) error {
        var payload WelcomeEmailPayload
        if err := json.Unmarshal(msg.Payload, &payload); err != nil {
            // Malformed payloads should not retry, route directly to DLQ
            return payloadError{err}
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
    "errors"
    "log"
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

    err := runner.Run()
    if errors.Is(err, queue.ErrShutdownTimeout) {
        // Dedicated daemon policy: exit while clients remain available to
        // handlers still draining. Do not close the provider in a hook/defer.
        log.Print(err)
        os.Exit(1)
    }
    if err := errors.Join(err, redisprovider.Close()); err != nil {
        log.Print(err)
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

The message's positive `MaxAttempts` takes precedence over the consumer policy.
`Dispatch` and `NewMessage` default this field to 3, so configuring the consumer's
fallback to 5 alone does not change those messages. The limit counts total handler
attempts, including the first execution: 5 attempts permit at most 4 retries.

```go
// Explicit per-message limit: at most five handler attempts.
err := queue.Dispatch(ctx, "tasks.retry", payload, queue.WithMaxAttempts(5))

// Alternatively, use the consuming consumer's RetryPolicy.MaxAttempts fallback.
err = queue.Dispatch(ctx, "tasks.retry", payload, queue.WithMaxAttempts(0))
```

### Retry Rescheduling and Durability

All in-repo adapters implement `queue.Reenqueuer`, but retry durability depends on the adapter and broker configuration:

- **PostgreSQL / Redis**: Reschedule the claimed delivery atomically, guarded by its ownership token.
- **NATS**: Publishes handler retries with the updated attempt count before acknowledging the original. Delayed retries carry a broker-stored due-time header and are deferred with native NAK until due. Admission rejection and group deferral use native NAK without consuming handler attempts.
- **RabbitMQ**: Delayed retries wait in a local timer while the original delivery remains unacknowledged, then publish the retry copy before acknowledging the original. A crash between publish and Ack can cause duplicate delivery; recovery of an interrupted timer depends on broker redelivery of the original.
- **MemoryDriver**: Keeps delayed retries in local timers. Shutdown cancels these timers, and process failure loses all in-memory state. It provides no persistent retry guarantee.

Durable adapters avoid the acknowledge-before-reschedule crash window. This does not guarantee survival of every broker failure or configuration; consult the adapter's `Capabilities()` and persistence settings, and make handlers safe for duplicate delivery.

### Non-Retryable Errors
A handler can return an error implementing `queue.Retryable` with `Retryable() bool` returning `false`, as `payloadError` does in section 4.3. `queue.IsRetryable` finds this interface through wrapped errors using `errors.As`. Such errors (and missing handlers) **bypass retry attempts** and are routed immediately to `<topic>:dlq`. Other handler errors retry by default.

### DLQ Handling and Retention

A DLQ publication has its own message ID. The headers `original_message_id` and
`original_topic` identify the failed original; payload, codec headers, and
attempt state are preserved. Each publication snapshots the payload into its own
byte buffer so a DLQ handler cannot mutate the unsettled original or another
DLQ copy. DLQ copies are independent deliveries with empty
`GroupID` and zero `GroupOrder`. The original ordering information is preserved
in headers `original_group_id` and `original_group_order` (decimal string). If publishing succeeds but acknowledging the
original fails, redelivery can publish another DLQ copy with another ID. Use the
original topic/ID pair when deduplicating failure records. DLQ handlers can run
concurrently and may run before the original Ack succeeds. They do not release
the original group barrier; only settlement of the original does.

The consumer also polls DLQ messages. **Register a DLQ handler before starting
consumers** if failures must be retained. Without a handler, the DLQ delivery is
acknowledged and dropped to prevent recursive `:dlq:dlq` routing. A DLQ handler
that returns a non-retryable error, or exhausts its retry limit, is also dropped.
DLQ copies inherit the original attempt count and limit, so an already-exhausted
copy can be dropped on its first handler failure. This is a processing route,
not automatic archival storage or guaranteed retention.

```go
import (
    "context"

    "github.com/rachmanzz/gosuite/v3/queue"
    "github.com/rachmanzz/gosuite/v3/worker"
)

// persistFailure is application-provided durable storage. It should deduplicate
// by original_topic/original_message_id and return nil only after persistence.
func registerFailureHandler(persistFailure func(context.Context, *queue.Message) error) {
    queue.RegisterHandler(queue.DLQTopic("tasks.retry"),
        func(ctx context.Context, msg *queue.Message, _ worker.SubWorkerPool) error {
            return persistFailure(ctx, msg)
        },
    )
}
```

Choose a failure-storage and retry policy that accounts for the terminal-drop
behavior above; returning an error alone does not guarantee indefinite retention.

### Lease & Delivery Sentinel (`queue.ErrDeliveryLost`)
Adapters that detect loss of delivery ownership can return `queue.ErrDeliveryLost` from settlement operations (`Ack`, `Nack`, `Reenqueue`). For example, PostgreSQL and Redis guard mutations with claim tokens and report the sentinel when the original claim no longer owns the delivery.

The sentinel is not a universal connection or lease error classifier. Transport, context, and library errors may be returned directly; NATS forwards errors from its message Ack/NAK operations without mapping them to this sentinel. A successful operation also does not prove lease ownership was checked by every adapter.

```go
if errors.Is(err, queue.ErrDeliveryLost) {
    // This adapter detected that the delivery is no longer owned
}
```

### Shutdown and Pending Retries

During Stop, polling is cancelled and joined before pending retries are flushed,
so flushed jobs cannot be immediately dequeued again during shutdown. Accepted
settlement operations are joined before the driver is closed.

`Runner` only waits for this Stop sequence until its shutdown timeout. On timeout,
`Run` returns `ErrShutdownTimeout` while Stop may still be running, and
`RegisterPostShutdown` hooks still execute. Hooks that close DB/cache clients can
therefore interrupt active handlers or settlement. The dedicated-daemon examples
above close application clients after `Run` returns only when it did not time out;
on timeout they log the error and exit the process without closing those clients
first. This policy can interrupt unfinished jobs and depends on broker redelivery
for recovery. An embedded application needs its own policy to wait for users of
shared resources to stop before closing them. Avoid unconditional client cleanup
in post-shutdown hooks or defers when those clients may still be in use.

---

## 🎯 6. Priority & Group Ordering

### Priorities

Adapters advertise their priority policy through `Capabilities().NativePriority`:

| Adapter | Priority policy |
| :--- | :--- |
| Memory, PostgreSQL, RabbitMQ | `PriorityHard` |
| Redis, NATS | `PrioritySoft` |

Hard priority applies to selecting eligible messages for delivery within the adapter's ordering constraints; soft priority is best effort. Priority does not preempt a running handler. Independent messages may execute concurrently, so even hard dequeue priority does not guarantee handler start or completion order.

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

### Group Retries and Settlement

Within one consumer, a failed or unsettled group member remains the retry head.
Later members wait until that head is successfully acknowledged or transferred
to DLQ; expiration of the backoff alone does not release the group. Failed Ack
or failed DLQ publication also retains the barrier. Idle group state is removed
after settlement, and pending barriers are cleared on consumer shutdown.

If the broker never redelivers an unsettled head, the group remains blocked until
that delivery is recovered or the consumer is restarted. Ordering coordination
is local to the consumer; it does not coordinate multiple processes.

A retry head is identified by its original topic and message ID together. Generated
DLQ copies have separate IDs and do not join the original work group, so a
low-priority DLQ delivery cannot become that group's head. Even manually supplied
legacy DLQ copies with the same ID cannot acknowledge or release the original
head. Oversized retry messages rejected by memory admission participate in the
same serialized settlement transition: successful original Ack after DLQ
publication releases the barrier, while settlement failure retains it.

A group retains every known unsettled topic/ID, rather than only its current
retry head. The earliest GroupOrder is the active barrier; equal orders follow
local admission sequence. Once that member settles, the next unresolved member
is promoted, including members rejected due to memory admission or a busy
settlement slot. Earlier queued batches may still execute before later barriers.

### Partial Fetch Errors

A driver can return claimed deliveries together with an error when a later fetch
fails. The core logs the error and processes the returned deliveries through the
normal memory admission, group ordering, retry, and settlement paths. It waits
until the next polling interval before fetching again after such an error.

If the consumer context was canceled while the batch was being fetched, the core
records group positions and Nacks the deliveries with a settlement context that
is not canceled, rather than dispatching new work. Admission reservations are
released; unacknowledged group heads remain barriers until recovered/settled.

### MemoryDriver Ordering and Pre-Execution Backpressure

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

---

## 🧩 7. Payload Codecs (JSON / MessagePack)

Payload serialization is pluggable through the `queue.Codec` interface. JSON is the built-in default; any other format can be registered under its own `content-type` and selected per dispatch.

```go
import (
    "context"

    "github.com/rachmanzz/gosuite/v3/queue"
    queuemsgpack "github.com/rachmanzz/gosuite/v3/queue-msgpack"
)

type WelcomeEmail struct {
    Email string `json:"email" msgpack:"email"`
}

func dispatchWelcome(ctx context.Context, email string) error {
    // A structured payload is marshaled with the selected codec.
    return queue.Dispatch(ctx, "user:welcome_email", WelcomeEmail{Email: email},
        queue.WithCodec(queuemsgpack.Codec()),
    )
}

func decodeWelcome(msg *queue.Message) (WelcomeEmail, error) {
    var out WelcomeEmail
    err := queue.Decode(msg, &out) // resolves the content-type header
    return out, err
}
```

`Dispatch` treats `string` and `[]byte` payloads as **already encoded bytes**:
it does not call the codec's `Marshal` for those types. `WithCodec` still labels
them with the selected content type, so the bytes must already match that format.
For example, a literal string `"hello"` is not encoded as MessagePack merely by
adding `WithCodec`; marshal it with `queuemsgpack.Codec().Marshal("hello")` first,
check the error, then dispatch the returned bytes with the same codec. Structured
values such as `WelcomeEmail` above are marshaled automatically.

`WithCodec` applies through `Dispatch`. Building a message with `NewMessage` and
calling `Enqueue` directly does not serialize its payload or automatically set a
codec header. On that path, encode the bytes yourself and set `WithHeaders` with
`"content-type"` equal to the codec's `ContentType()`.

- **Codec-agnostic adapters**: the payload boundary is per-message, so every driver (memory, Redis, NATS, RabbitMQ, PostgreSQL) works unchanged.
- **Header-driven decode**: `queue.Decode` resolves the codec from the `content-type` header, so the consumer process must import the codec package (blank import is fine) or decode explicitly with the codec.
- **Custom codecs**: implement `queue.Codec` and `queue.RegisterCodec(myCodec)`.
- **Backward compatible**: messages without a codec default to JSON.

See [`v3/queue-msgpack`](../queue-msgpack) for a complete example.

---

## 🧠 8. Optional Memory Budget

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
Set broker/prefetch limits separately when necessary. DLQ publication also
allocates a separate payload snapshot; that publication buffer and its storage
in the driver are outside consumer reservation accounting.

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

### Bounded Oversized Settlement

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
- **Message Model Instantiation**: Historical **345.2 ns/op**, equivalent to approximately **2.90 million ops/sec**, with functional options (`WithPriority`, `WithMaxAttempts`, `WithGroup`).
- **End-to-End JSON Dispatch**: Historical **985.3 ns/op**, equivalent to approximately **1.015 million ops/sec**, incorporating runtime JSON serialization, topic routing validation, and adapter storage.

These equivalent rates use `1e9 / ns_per_op`; the iteration counts in Go benchmark output are total measured iterations, not operations per second. They describe the historical benchmark workloads, not current production throughput.

---

## 📄 License

This module is part of the [GoSuite](https://github.com/rachmanzz/gosuite) repository and is licensed under the [MIT License](../../LICENSE).
