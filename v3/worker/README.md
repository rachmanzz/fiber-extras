# Worker Engine for Go & Fiber v3

[![Go Reference](https://pkg.go.dev/badge/github.com/rachmanzz/fiber-extras/v3/worker.svg)](https://pkg.go.dev/github.com/rachmanzz/fiber-extras/v3/worker)

High-performance background worker engine and task concurrency orchestrator for Go and [Fiber v3](https://github.com/gofiber/fiber), extracted and refined from production workloads.

Engineered around three core principles:
1. **Pure Go Core (Zero External Dependencies)** — The core engine is built purely on Go's standard library (`sync`, `chan`, `context`, `log/slog`), making it suitable for CLI, HTTP servers, microservices, and background daemons alike.
2. **First-Class [fiber-starter](https://github.com/rachmanzz/fiber-starter) Integration** — Features clean, direct lifecycle methods (`InitGlobal`, `Shutdown`) for effortless integration into Fiber v3 graceful shutdown without wrappers or boilerplate.
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

    // 1. Initialize worker engine during BeforeStart
    core.RegisterBeforeStart(func() error {
        logger := worker.NewCustomLogger(
            func(msg string, args ...any) { zap.S().Debugw(msg, args...) },
            func(msg string, args ...any) { zap.S().Infow(msg, args...) },
            func(msg string, args ...any) { zap.S().Warnw(msg, args...) },
            func(msg string, args ...any) { zap.S().Errorw(msg, args...) },
        )

        worker.InitGlobal(worker.Config{
            MaxWorkers:     cores.Config().Worker.MaxWorkers,     // e.g. from .env
            SubWorkerLimit: cores.Config().Worker.SubWorkerLimit,
            TaskQueueSize:  cores.Config().Worker.QueueCapacity,
            Logger:         logger,
        })
        return nil
    })

    // 2. Drain and stop worker engine gracefully on Fiber shutdown
    core.App.Hooks().OnPostShutdown(func(err error) error {
        return worker.Shutdown()
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

## 📊 Benchmarks

Measured using Go's standard benchmarking framework (`go test -bench=. -benchmem`) on Linux `amd64` (11th Gen Intel® Core™ i7-1165G7 @ 2.80GHz):

```text
goos: linux
goarch: amd64
pkg: github.com/rachmanzz/fiber-extras/v3/worker
cpu: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz
BenchmarkWorker_Dispatch_Async-8     	 6498375	       162.2 ns/op	      88 B/op	       1 allocs/op
BenchmarkWorker_ExecuteSync-8        	 4913364	       311.3 ns/op	     320 B/op	       4 allocs/op
BenchmarkWorker_SubWorker_FanOut-8   	  318261	      3970 ns/op	     680 B/op	      18 allocs/op
```

- **Async Dispatch**: ~6.5 million ops/sec at only **1 heap allocation** per dispatch (channel schedule).
- **Synchronous Execution**: ~4.9 million ops/sec with direct panic safety barrier and sync waiter channel.
- **Parallel Sub-Worker Fan-Out**: ~318,000 sub-pools/sec coordinating concurrent sub-routines with dynamic semaphore acquisition, error grouping, and wait synchronization.

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

