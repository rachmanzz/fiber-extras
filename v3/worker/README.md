# Worker Engine for Go & Fiber v3

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/worker.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/worker)

High-performance background worker engine and task concurrency orchestrator for Go and [Fiber v3](https://github.com/gofiber/fiber), extracted and refined from production workloads.

Engineered around three core principles:
1. **Pure Go Core (Zero External Dependencies)** — The core engine is built purely on Go's standard library (`sync`, `chan`, `context`, `log/slog`), making it suitable for CLI, HTTP servers, microservices, and background daemons alike.
2. **First-Class [fiber-starter](https://github.com/rachmanzz/fiber-starter) Integration** — Features out-of-the-box lifecycle hooks (`RegisterStarterHook`, `RegisterAppHooks`) for effortless integration into Fiber v3 graceful shutdown without boilerplate.
3. **Resilient Parallelism (Sub-Worker Fan-Out & Panic Safety)** — Native support for parent-to-child task splitting with bounded semaphore throttling, isolated panic recovery per goroutine, and fail-fast context propagation.

---

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/worker
```

---

## ⚡ Key Highlights

- **Goroutine Pool & Bounded Queue**: Fixed worker goroutines process tasks from a buffered channel with non-blocking dispatch and queue saturation safety (`ErrQueueFull`).
- **Parallel Sub-Worker Fan-Out**: Single tasks can spawn parallel sub-tasks throttled by a semaphore (`SubWorkerPool`).
- **Fail-Fast Error Propagation**: If any sub-worker fails, sibling sub-workers are promptly signaled via `context.WithCancelCause`, and `pool.Wait()` returns the first error.
- **Isolated Panic Recovery**: Panics within main tasks or individual sub-workers are safely captured with full stack traces without crashing the host application.
- **Dual Dispatch Modes**: Asynchronous non-blocking (`Dispatch`) or synchronous blocking (`ExecuteSync`).
- **Pluggable Structured Logging**: Backed by standard `log/slog` by default, with easy adapters for Uber Zap, Zerolog, or custom loggers.
- **Graceful Shutdown**: Automatically drains active tasks up to a configurable timeout (`ShutdownTimeout`).

---

## 📖 Part 1: Usage in Pure Go

You can use the worker engine in any Go application without Fiber:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/rachmanzz/fiber-extras/v3/worker"
)

func main() {
    // 1. Initialize engine
    engine := worker.New(
        worker.WithMaxWorkers(5),
        worker.WithSubWorkerLimit(3),
        worker.WithShutdownTimeout(10 * time.Second),
    ).Start()
    defer engine.Shutdown()

    // 2. Define a task with parallel sub-workers
    task := worker.NewTaskFunc("email:batch_send", func(ctx context.Context, pool worker.SubWorkerPool) error {
        recipients := []string{"alice@example.com", "bob@example.com", "charlie@example.com"}

        for _, recipient := range recipients {
            r := recipient
            pool.Go(func(subCtx context.Context) error {
                fmt.Printf("Sending email to %s...\n", r)
                time.Sleep(100 * time.Millisecond)
                return nil
            })
        }

        // Wait blocks until all sub-workers finish
        return pool.Wait()
    })

    // 3. Dispatch asynchronously
    if err := engine.Dispatch(context.Background(), task); err != nil {
        fmt.Printf("Failed to dispatch: %v\n", err)
    }

    // Wait for demonstration
    time.Sleep(500 * time.Millisecond)
}
```

---

## 📖 Part 2: Integration with `fiber-starter`

In applications generated from or adhering to `fiber-starter`:

### 2.1 Registering Lifecycle in `bootstrap/hook.go`

Hook the worker engine directly into `core.RegisterBeforeStart` and Fiber's `OnPostShutdown`:

```go
package bootstrap

import (
    "github.com/rachmanzz/fiber-extras/v3/worker"
    "github.com/rachmanzz/fiber-starter/cores"
    "go.uber.org/zap"
)

func RegisterHook(core *cores.AppContracts) {
    if core.App == nil {
        return
    }

    // Adapt Uber Zap to worker Logger
    logger := worker.NewCustomLogger(
        func(msg string, args ...any) { zap.S().Debugw(msg, args...) },
        func(msg string, args ...any) { zap.S().Infow(msg, args...) },
        func(msg string, args ...any) { zap.S().Warnw(msg, args...) },
        func(msg string, args ...any) { zap.S().Errorw(msg, args...) },
    )

    // Register before start & on shutdown hooks with one line
    worker.RegisterStarterHook(core, core.App, worker.Config{
        MaxWorkers:      cores.Config().Worker.MaxWorkers,     // e.g. from .env
        SubWorkerLimit:  cores.Config().Worker.SubWorkerLimit,
        TaskQueueSize:   cores.Config().Worker.QueueCapacity,
        Logger:          logger,
    })
}
```

### 2.2 Writing Modular Tasks in `app/events/workers/`

Define your business tasks implementing the `worker.Task` interface:

```go
package workers

import (
    "context"
    "github.com/rachmanzz/fiber-extras/v3/worker"
)

type CleanupTask struct {
    // Inject repositories or clients here
}

func NewCleanupTask() *CleanupTask {
    return &CleanupTask{}
}

func (t *CleanupTask) Name() string {
    return "maintenance:cleanup"
}

func (t *CleanupTask) Execute(ctx context.Context, pool worker.SubWorkerPool) error {
    // Fan-out sub-workers if needed:
    pool.Go(func(subCtx context.Context) error {
        // e.g. delete expired sessions
        return nil
    })

    pool.Go(func(subCtx context.Context) error {
        // e.g. delete temporary uploads
        return nil
    })

    return pool.Wait()
}
```

### 2.3 Dispatching from Services or HTTP Handlers

Inject `worker.Dispatcher` or call package-level dispatchers:

```go
package services

import (
    "context"
    "github.com/rachmanzz/fiber-extras/v3/worker"
    "your-project/app/events/workers"
)

type OrderService struct {
    dispatcher worker.Dispatcher
}

func NewOrderService(d ...worker.Dispatcher) *OrderService {
    var disp worker.Dispatcher
    if len(d) > 0 && d[0] != nil {
        disp = d[0]
    } else {
        disp = worker.GetDispatcher()
    }
    return &OrderService{dispatcher: disp}
}

func (s *OrderService) ProcessOrder(ctx context.Context) error {
    // Non-blocking fire-and-forget
    return s.dispatcher.Dispatch(ctx, workers.NewCleanupTask())
}
```

---

## ⚙️ Configuration Reference

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `MaxWorkers` | `10` | Total worker goroutines processing the task queue. |
| `SubWorkerLimit` | `5` | Maximum parallel sub-workers spawned per task fan-out. |
| `TaskQueueSize` | `1000` | Buffer capacity of the task queue channel. |
| `ShutdownTimeout` | `15s` | Maximum duration to wait for running tasks during shutdown. |
| `Logger` | `slog.Default()` | Pluggable structured logger instance. |

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and is licensed under the [MIT License](../../LICENSE).
