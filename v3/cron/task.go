package cron

import (
	"context"
	"fmt"
	"sync"
)

// Task is the contract for an executable scheduled job with lifecycle hooks.
// Execute carries the work; OnError receives recoverable failures (e.g. a
// transient downstream error); OnFail receives unrecoverable failures --
// including panics recovered on the task's behalf.
type Task interface {
	Execute(ctx context.Context) error
	OnError(ctx context.Context, err error)
	OnFail(ctx context.Context, err error)
}

// ScheduledTask is a Task that supplies its own cron spec (e.g. "0 2 * * *").
type ScheduledTask interface {
	Task
	Schedule() string
}

// TimezoneAwareTask optionally scopes its schedule to a timezone. Returning
// "" (or "UTC") keeps the scheduler's base location; anything else is applied
// as a CRON_TZ prefix so the schedule is evaluated in that zone.
type TimezoneAwareTask interface {
	Task
	Timezone() string
}

// FluentScheduledTask optionally supplies its spec through the fluent
// builders (EveryDay, EveryMonth, ...). A non-empty WriteSchedule() takes
// precedence over Schedule().
type FluentScheduledTask interface {
	Task
	WriteSchedule() string
}

// TaskRunner adapts a Task to the cron engine: it runs Execute with the scheduler's
// lifecycle context, routes errors to OnError, and converts panics into
// OnFail so a crashing task can never silently kill the schedule.
type TaskRunner struct {
	task Task
	mu   sync.RWMutex
	ctx  context.Context
}

// NewTaskRunner wraps task for execution by the scheduler. An optional parent context
// can be provided to link execution to the scheduler's shutdown cancellation.
func NewTaskRunner(task Task, parentCtx ...context.Context) *TaskRunner {
	c := context.Background()
	if len(parentCtx) > 0 && parentCtx[0] != nil {
		c = parentCtx[0]
	}
	return &TaskRunner{task: task, ctx: c}
}

// SetContext updates the base context used for running tasks.
func (r *TaskRunner) SetContext(ctx context.Context) {
	if ctx != nil {
		r.mu.Lock()
		r.ctx = ctx
		r.mu.Unlock()
	}
}

// Run executes the task, converting panics into OnFail and errors into
// OnError. It never returns an error: all outcomes are handled through the
// task's own lifecycle hooks.
func (r *TaskRunner) Run() {
	r.mu.RLock()
	baseCtx := r.ctx
	r.mu.RUnlock()

	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	defer func() {
		if rec := recover(); rec != nil {
			err := fmt.Errorf("cron task panicked: %v", rec)
			r.task.OnFail(ctx, err)
		}
	}()

	if err := r.task.Execute(ctx); err != nil {
		r.task.OnError(ctx, err)
	}
}
