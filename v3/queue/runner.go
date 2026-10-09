package queue

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

// Common runner errors.
var (
	ErrRunnerAlreadyRunning = errors.New("queue: runner is already running")
	ErrRunnerNoConsumers    = errors.New("queue: runner has no consumers configured")
)

// Hook represents a lifecycle hook function for the standalone runner.
type Hook func() error

// RunnerOption configures parameters of a standalone queue Runner.
type RunnerOption func(*Runner)

// WithShutdownTimeout configures the maximum time to wait for consumers to stop during shutdown.
func WithShutdownTimeout(timeout time.Duration) RunnerOption {
	return func(r *Runner) {
		if timeout > 0 {
			r.shutdownTimeout = timeout
		}
	}
}

// WithSignals configures the OS signals that trigger a graceful shutdown.
func WithSignals(signals ...os.Signal) RunnerOption {
	return func(r *Runner) {
		if len(signals) > 0 {
			r.signals = signals
		}
	}
}

// WithRunnerLogger sets a custom logger for the Runner.
func WithRunnerLogger(logger Logger) RunnerOption {
	return func(r *Runner) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// Runner manages standalone queue consumer execution as a background daemon process.
// It handles OS signals (SIGINT, SIGTERM), runs BeforeStart and PostShutdown hooks,
// and ensures graceful shutdown of consumers without requiring an HTTP server.
type Runner struct {
	consumers         []*Consumer
	beforeStartHooks  []Hook
	postShutdownHooks []Hook
	shutdownTimeout   time.Duration
	signals           []os.Signal
	logger            Logger
	stopCh            chan struct{}
	running           bool
	stopped           bool
	mu                sync.Mutex
}

// NewRunner creates a new standalone Runner.
// It can supervise one or more consumers, or default to the global consumer manager if none are provided.
func NewRunner(consumers ...*Consumer) *Runner {
	var valid []*Consumer
	for _, c := range consumers {
		if c != nil {
			valid = append(valid, c)
		}
	}

	return &Runner{
		consumers:       valid,
		shutdownTimeout: 15 * time.Second,
		signals:         []os.Signal{os.Interrupt, syscall.SIGTERM},
		logger:          DefaultLogger(),
		stopCh:          make(chan struct{}),
	}
}

// Configure applies options to the Runner.
func (r *Runner) Configure(opts ...RunnerOption) *Runner {
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// AddConsumer attaches an additional consumer to be supervised by the runner.
func (r *Runner) AddConsumer(c *Consumer) {
	if c == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consumers = append(r.consumers, c)
}

// RegisterBeforeStart registers a hook to execute before starting queue consumers.
// If any BeforeStart hook returns an error, Run() aborts immediately.
func (r *Runner) RegisterBeforeStart(hook Hook) {
	if hook == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.beforeStartHooks = append(r.beforeStartHooks, hook)
}

// RegisterPostShutdown registers a hook to execute after consumers have stopped.
func (r *Runner) RegisterPostShutdown(hook Hook) {
	if hook == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.postShutdownHooks = append(r.postShutdownHooks, hook)
}

// Run starts the runner loop. It blocks until an OS signal (SIGINT, SIGTERM) is received,
// the provided context is cancelled, or Stop() is called.
func (r *Runner) Run(ctx ...context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunnerAlreadyRunning
	}
	r.running = true
	r.mu.Unlock()

	var parentCtx context.Context
	if len(ctx) > 0 && ctx[0] != nil {
		parentCtx = ctx[0]
	} else {
		parentCtx = context.Background()
	}

	// 1. Execute BeforeStart Hooks in FIFO order
	r.logger.Info("queue runner: executing before-start hooks", "count", len(r.beforeStartHooks))
	for i, hook := range r.beforeStartHooks {
		if err := hook(); err != nil {
			r.logger.Error("queue runner: before-start hook failed", "hook_index", i, "error", err)
			return fmt.Errorf("queue runner: before-start hook %d failed: %w", i, err)
		}
	}

	// 2. Resolve consumers: if none passed explicitly, start global consumer
	r.mu.Lock()
	consumers := make([]*Consumer, len(r.consumers))
	copy(consumers, r.consumers)
	r.mu.Unlock()

	usingGlobalConsumer := false
	if len(consumers) == 0 {
		globalConsumer, err := StartConsumer()
		if err != nil {
			return fmt.Errorf("queue runner: failed to start default consumer: %w", err)
		}
		consumers = append(consumers, globalConsumer)
		usingGlobalConsumer = true
	} else {
		for _, c := range consumers {
			c.Start()
		}
	}

	r.logger.Info("queue runner: consumer(s) active, listening for shutdown signals", "count", len(consumers))

	// 3. Setup OS signal notification
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, r.signals...)
	defer signal.Stop(sigCh)

	// 4. Block waiting for shutdown trigger
	select {
	case <-parentCtx.Done():
		r.logger.Info("queue runner: context cancelled, initiating shutdown")
	case sig := <-sigCh:
		r.logger.Info("queue runner: received shutdown signal", "signal", sig.String())
	case <-r.stopCh:
		r.logger.Info("queue runner: stop requested programmatically")
	}

	// 5. Gracefully shutdown consumers
	return r.shutdown(consumers, usingGlobalConsumer)
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

func (r *Runner) shutdown(consumers []*Consumer, usingGlobal bool) error {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()

	r.logger.Info("queue runner: stopping consumers gracefully", "timeout", r.shutdownTimeout)

	stoppedDone := make(chan struct{})
	go func() {
		if usingGlobal {
			StopConsumer()
		} else {
			var wg sync.WaitGroup
			for _, c := range consumers {
				wg.Add(1)
				go func(cons *Consumer) {
					defer wg.Done()
					cons.Stop()
				}(c)
			}
			wg.Wait()
		}
		close(stoppedDone)
	}()

	select {
	case <-stoppedDone:
		r.logger.Info("queue runner: all consumers stopped cleanly")
	case <-time.After(r.shutdownTimeout):
		r.logger.Warn("queue runner: shutdown timed out waiting for consumers")
	}

	// 6. Execute PostShutdown Hooks in FIFO order
	r.logger.Info("queue runner: executing post-shutdown hooks", "count", len(r.postShutdownHooks))
	var hookErr error
	for i, hook := range r.postShutdownHooks {
		if err := hook(); err != nil {
			r.logger.Error("queue runner: post-shutdown hook failed", "hook_index", i, "error", err)
			if hookErr == nil {
				hookErr = fmt.Errorf("queue runner: post-shutdown hook %d failed: %w", i, err)
			}
		}
	}

	r.logger.Info("queue runner: shutdown sequence finished")
	return hookErr
}
