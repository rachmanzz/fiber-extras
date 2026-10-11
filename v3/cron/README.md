# fiber-extras v3/cron

Production-grade scheduler for Go, built on [robfig/cron/v3](https://github.com/robfig/cron/v3) with a
Task contract, fluent schedule builders, panic recovery, skip-if-still-running protection, graceful
shutdown, and a standalone process runner.

The module is **dependency-free apart from the cron engine** — no queue, no worker, no logging
framework. Hosts wire their own Execute logic into tasks.

## 📦 Installation

```bash
go get github.com/rachmanzz/fiber-extras/v3/cron@latest
```

## 📖 Quickstart

```go
package main

import (
    "context"
    "os"

    "github.com/rachmanzz/fiber-extras/v3/cron"
)

type cleanupTask struct{}

func (cleanupTask) Schedule() string { return cron.EveryDay("02:00") }
// Jakarta 02:00 daily:
func (cleanupTask) Location() *time.Location { return cron.TZ_ASIA_JAKARTA }
func (cleanupTask) Execute(ctx context.Context) error {
    // ... retention cleanup ...
    return nil
}
func (cleanupTask) OnError(ctx context.Context, err error)  { /* log / retry */ }
func (cleanupTask) OnFail(ctx context.Context, err error)   { /* page on-call */ }

func main() {
    s := cron.NewScheduler()
    if err := s.Register(cleanupTask{}); err != nil {
        os.Exit(1)
    }

    // Standalone runner: hooks + graceful shutdown on SIGINT/SIGTERM.
    r := cron.NewRunner(s)
    r.RegisterBeforeStart(func() error { /* open resources */ return nil })
    r.RegisterPostShutdown(func() error { /* release resources */ return nil })
    _ = r.Run()
}
```

## ✨ Features

- **Task contract** — `Execute` + `OnError` (recoverable) + `OnFail` (fatal/panic).
- **Declarative schedules** — `Schedule()`, optional `WriteSchedule()` (fluent, wins when non-empty),
  optional timezone via `Timezone()` (string) or `Location()` (`*time.Location`, wins when both).
- **Zones** — pre-declared DST-free pointers (`cron.TZ_ASIA_JAKARTA`, `cron.TZ_ASIA_TOKYO`, ...) with
  a fixed-offset fallback so they are correct even without system tzdata; anything else via
  `cron.ResolveZone("Europe/London")` (cached, shared pointer) or `cron.MustResolveZone(...)` (panics
  on unknown names — for app startup).
- **Fluent builders** — `EveryMinutes(n)`, `EveryHours(n)`, `EveryDay("02:00")`,
  `EveryMonth(1, "00:00")`, `EveryWeekly("mon", "07:45")` with safe fallbacks.
- **Production defaults** — panic recovery, skip-if-still-running (`WithoutSkipOverlap()` to allow
  overlap), 5-field specs (`WithSeconds(true)` for 6-field), base location `WithLocation(...)`.
- **Graceful stop** — `Stop(ctx)` waits for in-flight jobs up to the deadline; they are never
  cancelled mid-run.
- **Standalone runner** — before-start/post-shutdown hooks, OS signal handling
  (`WithSignals`, default SIGINT + SIGTERM), shutdown timeout (`WithShutdownTimeout`).
  A failing before-start hook aborts startup but still runs post-shutdown hooks.
  `Run` returns `nil` on a clean shutdown and the stop error
  (`context.DeadlineExceeded` when the graceful-stop deadline expires while jobs
  are still running) — post-shutdown hooks always run, so hosts can report the
  incomplete shutdown instead of it being masked as success.
- **Observability** — `Registered()` returns resolved specs (incl. `CRON_TZ` prefix), `Entries()`
  the live schedule.
- **Pluggable logging** — a minimal `Logger` interface (`NewSlogLogger`, `NopLogger`) — no zap, no
  direct slog requirement.

`Register` never exits the process: invalid specs are returned as errors (the task's `OnFail` is
still invoked) so hosts decide how to fail at startup.

## 🚀 Integration with `fiber-starter`

The recommended pattern (mirrored from real deployments) is a **standalone cron daemon** — never
in-process inside the web server. The daemon is thin: it only triggers tasks on schedule; each task
decides what to do (dispatch a queue message, run a worker, call a service).

### 1. Recommended file structure

```text
fiber-starter/
├── app/
│   └── events/
│       ├── cron.go                # Central registration: binds every task to the scheduler
│       └── crons/
│           ├── partition.go       # Monthly partition maintenance task
│           └── retention.go       # Daily retention cleanup task
└── cmd/
    ├── web/main.go                # HTTP server entrypoint (no cron here)
    └── cron/main.go               # Cron daemon entrypoint
```

### 2. Writing tasks in `app/events/crons/`

A task is a plain struct implementing the **Task contract** (`Execute` / `OnError` / `OnFail`) plus
one schedule interface — `Schedule()` (string spec), optionally `Location()` for a typed timezone,
or `WriteSchedule()` for a fluent builder (wins when non-empty).

```go
// app/events/crons/partition.go
package crons

import (
    "context"
    "fmt"
    "time"

    "github.com/rachmanzz/fiber-extras/v3/cron"
)

const TopicCronPartitionMaintain = "events:partition:maintain"

// PartitionCronTask pre-provisions database partitions for the upcoming months.
type PartitionCronTask struct {
    schedule string
    location *time.Location
}

func NewPartitionCronTask() *PartitionCronTask {
    return &PartitionCronTask{
        schedule: "0 0 1 * *", // every 1st of the month at 00:00
        location: cron.TZ_ASIA_JAKARTA,
    }
}

// --- schedule interfaces ----------------------------------------------------

// Schedule returns the standard 5-field cron spec.
func (t *PartitionCronTask) Schedule() string { return t.schedule }

// Location scopes the schedule (optional; return nil or TZ_UTC for UTC).
func (t *PartitionCronTask) Location() *time.Location { return t.location }

// --- Task contract ----------------------------------------------------------

// Execute performs the scheduled work.
func (t *PartitionCronTask) Execute(ctx context.Context) error {
    // Thin task: delegate to the queue so the cron daemon stays lightweight.
    return myqueue.Dispatch(ctx, TopicCronPartitionMaintain,
        map[string]string{"trigger": "cron", "type": "monthly_partition"})
}

// OnError handles recoverable failures (e.g. a transient dispatch error).
func (t *PartitionCronTask) OnError(ctx context.Context, err error) {
    logger.Error("partition cron failed", "error", err)
}

// OnFail handles fatal failures and panics (recovered by the scheduler).
func (t *PartitionCronTask) OnFail(ctx context.Context, err error) {
    logger.Error("partition cron CRITICAL failure", "error", err)
    // e.g. dispatch an alert message to your on-call channel
}
```

```go
// app/events/crons/retention.go
package crons

type RetentionCronTask struct{ /* config, deps */ }

func (t *RetentionCronTask) Schedule() string                     { return cron.EveryDay("02:00") }
func (t *RetentionCronTask) Execute(ctx context.Context) error    { /* ... */ return nil }
func (t *RetentionCronTask) OnError(ctx context.Context, e error) { /* ... */ }
func (t *RetentionCronTask) OnFail(ctx context.Context, e error)  { /* ... */ }
```

### 3. Central registration in `app/events/cron.go`

```go
package events

import (
    "github.com/rachmanzz/fiber-extras/v3/cron"
    "github.com/rachmanzz/fiber-starter/app/events/crons"
)

// RegisterCronTasks binds every scheduled task to the scheduler.
func RegisterCronTasks(s *cron.Scheduler) error {
    return s.RegisterAll(
        crons.NewPartitionCronTask(),
        crons.NewRetentionCronTask(),
    )
}
```

### 4. Daemon entrypoint in `cmd/cron/main.go`

```go
package main

import (
    "os"
    "time"

    "github.com/rachmanzz/fiber-extras/v3/cron"
    "github.com/rachmanzz/fiber-starter/app/events"
)

func main() {
    scheduler := cron.NewScheduler()
    if err := events.RegisterCronTasks(scheduler); err != nil {
        os.Exit(1)
    }

    runner := cron.NewRunner(scheduler,
        cron.WithShutdownTimeout(10*time.Second), // waits for in-flight jobs
    )
    runner.RegisterBeforeStart(func() error { /* open connections */ return nil })
    runner.RegisterPostShutdown(func() error { /* close connections */ return nil })

    // Blocks until SIGINT / SIGTERM, then drains gracefully.
    if err := runner.Run(); err != nil {
        os.Exit(1)
    }
}
```

> **Thin vs direct tasks**: keep the daemon thin — `Execute` dispatches a queue message and the
> workers do the heavy lifting (restart-safe, horizontally scalable). A task may also execute its
> work directly (database + worker engine inside the cron process); the contract is identical,
> only the body of `Execute` differs.

---

## 📄 License

This module is part of the [fiber-extras](https://github.com/rachmanzz/fiber-extras) repository and
is licensed under the [MIT License](../../LICENSE).
