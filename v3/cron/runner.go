package cron

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ErrRunnerAlreadyRunning is returned when Run() is called on an already active Runner.
var ErrRunnerAlreadyRunning = errors.New("cron runner: already running")

// Hook is a lifecycle callback executed by the Runner.
type Hook func() error

// Runner runs a Scheduler as a standalone process: it executes before-start
// hooks, starts the scheduler, waits for an OS signal (default SIGINT and
// SIGTERM), context cancellation, or programmatic Stop(), performs a graceful
// stop with a deadline, and then executes post-shutdown hooks. It carries no
// dependency on any other module, so hosts wire their own Execute logic into Tasks.
type Runner struct {
	scheduler       *Scheduler
	beforeStart     []Hook
	postShutdown    []Hook
	shutdownTimeout time.Duration
	signals         []os.Signal
	logger          Logger

	mu      sync.Mutex
	running bool
	stopped bool
	stopCh  chan struct{}
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
		r.signals = sig
	}
}

// NewRunner builds a standalone Runner for the given scheduler.
func NewRunner(s *Scheduler, opts ...RunnerOption) *Runner {
	r := &Runner{
		scheduler:       s,
		shutdownTimeout: 10 * time.Second,
		signals:         DefaultSignals(),
		logger:          DefaultLogger(),
		stopCh:          make(chan struct{}),
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
// arrives, the provided context ends, or Stop() is called. It returns nil on a clean shutdown.
func (r *Runner) Run(ctx ...context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunnerAlreadyRunning
	}
	r.running = true
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.running = false
		r.stopped = false
		r.stopCh = make(chan struct{})
		r.mu.Unlock()
	}()

	var parentCtx context.Context
	if len(ctx) > 0 && ctx[0] != nil {
		parentCtx = ctx[0]
	} else {
		parentCtx = context.Background()
	}

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

	select {
	case <-parentCtx.Done():
		r.logger.Info("cron runner context cancelled, shutting down")
	case sig := <-sigCh:
		r.logger.Info("cron runner received signal, shutting down", "signal", fmt.Sprint(sig))
	case <-r.stopCh:
		r.logger.Info("cron runner stop requested programmatically")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()
	stopErr := r.scheduler.Stop(stopCtx)
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

// Stop programmatically signals the runner to shut down gracefully.
func (r *Runner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.stopped = true
		close(r.stopCh)
	}
}

func (r *Runner) runPostShutdownHooks() {
	for _, h := range r.postShutdown {
		if err := h(); err != nil {
			r.logger.Error("cron runner post-shutdown hook failed", "error", err)
		}
	}
}
