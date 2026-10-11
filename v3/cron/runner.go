package cron

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Hook is a lifecycle callback executed by the Runner.
type Hook func() error

// Runner runs a Scheduler as a standalone process: it executes before-start
// hooks, starts the scheduler, waits for an OS signal (default SIGINT and
// SIGTERM), performs a graceful stop with a deadline, and then executes
// post-shutdown hooks. It carries no dependency on any other module, so hosts
// wire their own Execute logic into Tasks.
type Runner struct {
	scheduler       *Scheduler
	beforeStart     []Hook
	postShutdown    []Hook
	shutdownTimeout time.Duration
	signals         []os.Signal
	logger          Logger
}

// RunnerOption configures the Runner.
type RunnerOption func(*Runner)

// WithShutdownTimeout caps how long a graceful stop may wait for in-flight
// jobs (default 10s).
func WithShutdownTimeout(d time.Duration) RunnerOption {
	return func(r *Runner) {
		if d > 0 {
			r.shutdownTimeout = d
		}
	}
}

// WithSignals overrides the OS signals that terminate the runner
// (default SIGINT and SIGTERM).
func WithSignals(sig ...os.Signal) RunnerOption {
	return func(r *Runner) {
		if len(sig) > 0 {
			r.signals = sig
		}
	}
}

// NewRunner builds a standalone Runner for the given scheduler.
func NewRunner(s *Scheduler, opts ...RunnerOption) *Runner {
	r := &Runner{
		scheduler:       s,
		shutdownTimeout: 10 * time.Second,
		signals:         DefaultSignals(),
		logger:          DefaultLogger(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// DefaultSignals returns the signals a standalone runner terminates on:
// SIGINT (Ctrl-C) and SIGTERM (docker stop / k8s delete).
func DefaultSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

// RegisterBeforeStart appends a hook executed before the scheduler starts.
// A failing hook aborts startup (and runs post-shutdown hooks for cleanup).
func (r *Runner) RegisterBeforeStart(h Hook) {
	r.beforeStart = append(r.beforeStart, h)
}

// RegisterPostShutdown appends a hook executed after the scheduler stopped.
// It always runs -- including on startup failure -- so resources opened by
// before-start hooks can be released.
func (r *Runner) RegisterPostShutdown(h Hook) {
	r.postShutdown = append(r.postShutdown, h)
}

// Run executes the full lifecycle and blocks until a termination signal
// arrives (or the process context ends). It returns nil on a clean shutdown.
func (r *Runner) Run() error {
	for _, h := range r.beforeStart {
		if err := h(); err != nil {
			r.logger.Error("cron runner before-start hook failed", "error", err)
			r.runPostShutdownHooks()
			return err
		}
	}

	sigCh := make(chan os.Signal, 1)
	if len(r.signals) > 0 {
		signal.Notify(sigCh, r.signals...)
		defer signal.Stop(sigCh)
	}

	r.scheduler.Start()

	sig := <-sigCh
	r.logger.Info("cron runner received signal, shutting down", "signal", fmt.Sprint(sig))

	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()
	stopErr := r.scheduler.Stop(ctx)
	r.runPostShutdownHooks()

	if stopErr != nil {
		// The graceful stop did not finish within the deadline (or Stop
		// failed): surface it instead of masking an incomplete shutdown as
		// success. Post-shutdown hooks have already run.
		r.logger.Warn("cron runner shutdown did not complete cleanly", "error", stopErr)
		return stopErr
	}
	return nil
}

func (r *Runner) runPostShutdownHooks() {
	for _, h := range r.postShutdown {
		if err := h(); err != nil {
			r.logger.Error("cron runner post-shutdown hook failed", "error", err)
		}
	}
}
