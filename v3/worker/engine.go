package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// Common engine errors.
var (
	ErrEngineClosed = errors.New("worker: engine is closed")
	ErrQueueFull    = errors.New("worker: task queue is full")
	ErrNilTask      = errors.New("worker: cannot dispatch nil task")
)

// Dispatcher defines the interface for submitting background tasks.
type Dispatcher interface {
	// Dispatch submits a task for background execution (non-blocking, fire-and-forget).
	Dispatch(ctx context.Context, task Task) error

	// ExecuteSync executes a task synchronously, allowing the caller to wait for completion and sub-workers.
	ExecuteSync(ctx context.Context, task Task) error
}

// Engine represents the background worker engine orchestrating task execution and sub-worker pools.
type Engine struct {
	cfg       Config
	taskQueue chan Task
	subLimit  int
	wg        sync.WaitGroup
	quit      chan struct{}
	started   bool
	closed    bool
	closedMu  sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	logger    Logger
}

var (
	globalEngine   *Engine
	globalEngineMu sync.RWMutex
)

// NewEngine creates a new unstarted Worker Engine with the provided configuration and options.
func NewEngine(cfg Config, opts ...Option) *Engine {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = DefaultMaxWorkers
	}
	if cfg.SubWorkerLimit <= 0 {
		cfg.SubWorkerLimit = DefaultSubWorkerLimit
	}
	if cfg.TaskQueueSize <= 0 {
		cfg.TaskQueueSize = DefaultTaskQueueSize
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = DefaultLogger()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		cfg:       cfg,
		taskQueue: make(chan Task, cfg.TaskQueueSize),
		subLimit:  cfg.SubWorkerLimit,
		quit:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
		logger:    cfg.Logger,
	}
}

// New creates a new Worker Engine with default configuration modified by options.
func New(opts ...Option) *Engine {
	return NewEngine(DefaultConfig(), opts...)
}

// Start launches worker goroutines to process incoming tasks from the queue.
// It is idempotent: calling Start more than once (concurrently or otherwise)
// launches the worker set only once, so a second call cannot double the number
// of consumers draining the task queue.
func (e *Engine) Start() *Engine {
	e.closedMu.Lock()
	if e.closed || e.started {
		e.closedMu.Unlock()
		return e
	}
	e.started = true
	e.closedMu.Unlock()

	for i := 0; i < e.cfg.MaxWorkers; i++ {
		e.wg.Add(1)
		go e.workerLoop(i + 1)
	}

	e.logger.Info("worker engine started",
		"max_workers", e.cfg.MaxWorkers,
		"sub_worker_limit", e.cfg.SubWorkerLimit,
		"queue_capacity", e.cfg.TaskQueueSize,
	)

	return e
}

func (e *Engine) workerLoop(workerID int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.quit:
			return
		case task, ok := <-e.taskQueue:
			if !ok {
				return
			}
			e.runTask(e.ctx, task, workerID)
		}
	}
}

func (e *Engine) runTask(ctx context.Context, task Task, workerID int) {
	start := time.Now()
	taskName := task.Name()

	subPool := newSubWorkerPool(ctx, e.subLimit, taskName, e.logger)

	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			e.logger.Error("worker task recovered from panic",
				"task", taskName,
				"worker_id", workerID,
				"panic", r,
				"stack", stack,
			)
		}
	}()

	err := task.Execute(ctx, subPool)

	// Join any sub-workers the task spawned. A task that forgets to call
	// pool.Wait() would otherwise let the engine mark the task complete (and
	// free its worker) while sub-worker work is still in flight. Joining here
	// guarantees the engine never sees a task as done with live sub-workers and
	// surfaces an error the task failed to propagate.
	if waitErr := subPool.Wait(); waitErr != nil && err == nil {
		err = waitErr
	}

	elapsed := time.Since(start)

	if err != nil {
		e.logger.Error("worker task completed with error",
			"task", taskName,
			"worker_id", workerID,
			"elapsed", elapsed,
			"error", err,
		)
	} else {
		e.logger.Debug("worker task completed successfully",
			"task", taskName,
			"worker_id", workerID,
			"elapsed", elapsed,
		)
	}
}

// Dispatch queues a task to be processed asynchronously in the background (non-blocking).
// Returns ErrQueueFull if the task channel buffer is full.
func (e *Engine) Dispatch(ctx context.Context, task Task) error {
	if task == nil {
		return ErrNilTask
	}

	e.closedMu.RLock()
	defer e.closedMu.RUnlock()

	if e.closed {
		return ErrEngineClosed
	}

	select {
	case e.taskQueue <- task:
		return nil
	default:
		e.logger.Warn("worker task queue full, task dropped", "task", task.Name())
		return ErrQueueFull
	}
}

// ExecuteSync executes the task immediately in the current goroutine, with parallel sub-workers enabled.
func (e *Engine) ExecuteSync(ctx context.Context, task Task) error {
	if task == nil {
		return ErrNilTask
	}

	subPool := newSubWorkerPool(ctx, e.subLimit, task.Name(), e.logger)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("sync task recovered from panic",
					"task", task.Name(),
					"panic", r,
					"stack", string(debug.Stack()),
				)
				err = fmt.Errorf("worker: task %q panicked: %v", task.Name(), r)
			}
		}()
		err = task.Execute(ctx, subPool)
	}()

	// Mirror runTask: join sub-workers even if the task did not call Wait().
	if waitErr := subPool.Wait(); waitErr != nil && err == nil {
		err = waitErr
	}

	return err
}

// Shutdown gracefully stops the worker engine, allowing all queued and active workers
// up to ShutdownTimeout to finish. It closes the task queue to drain buffered tasks,
// and force terminates with context cancellation if ShutdownTimeout expires.
func (e *Engine) Shutdown() error {
	e.closedMu.Lock()
	if e.closed {
		e.closedMu.Unlock()
		return nil
	}
	e.closed = true
	e.closedMu.Unlock()

	// 1. Close task queue so no new tasks are accepted and workers drain remaining items
	close(e.taskQueue)

	c := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		if e.cancel != nil {
			e.cancel()
		}
		e.logger.Info("worker engine stopped gracefully")
		return nil
	case <-time.After(e.cfg.ShutdownTimeout):
		// Force termination on timeout: cancel task contexts and signal quit
		if e.cancel != nil {
			e.cancel()
		}
		close(e.quit)
		e.logger.Warn("worker engine shutdown timed out, force terminated")
		return fmt.Errorf("worker: engine shutdown timed out after %v", e.cfg.ShutdownTimeout)
	}
}

// InitGlobal initializes, starts, and sets the global worker engine.
func InitGlobal(cfg ...Config) *Engine {
	globalEngineMu.Lock()
	defer globalEngineMu.Unlock()

	c := DefaultConfig()
	if len(cfg) > 0 {
		c = cfg[0]
	}

	engine := NewEngine(c)
	engine.Start()
	globalEngine = engine
	return engine
}

// GetDispatcher returns the global dispatcher singleton.
// If not yet initialized, it lazily initializes an engine with DefaultConfig.
func GetDispatcher() Dispatcher {
	globalEngineMu.RLock()
	if globalEngine != nil {
		defer globalEngineMu.RUnlock()
		return globalEngine
	}
	globalEngineMu.RUnlock()

	// Upgrade to a write lock and re-check: two concurrent callers may both
	// observe nil, but only the first must create the engine, otherwise the
	// engine of the loser leaks (its worker goroutines keep running).
	globalEngineMu.Lock()
	defer globalEngineMu.Unlock()

	if globalEngine == nil {
		engine := NewEngine(DefaultConfig())
		engine.Start()
		globalEngine = engine
	}
	return globalEngine
}

// SetGlobalDispatcher sets or overrides the global dispatcher (useful for mocking in tests).
func SetGlobalDispatcher(d Dispatcher) {
	globalEngineMu.Lock()
	defer globalEngineMu.Unlock()

	if eng, ok := d.(*Engine); ok {
		globalEngine = eng
	}
}

// Dispatch submits a task to the global worker dispatcher.
func Dispatch(ctx context.Context, task Task) error {
	return GetDispatcher().Dispatch(ctx, task)
}

// ExecuteSync executes a task synchronously using the global dispatcher.
func ExecuteSync(ctx context.Context, task Task) error {
	return GetDispatcher().ExecuteSync(ctx, task)
}

// Shutdown gracefully stops the global worker engine if initialized.
func Shutdown() error {
	globalEngineMu.Lock()
	defer globalEngineMu.Unlock()

	if globalEngine != nil {
		err := globalEngine.Shutdown()
		globalEngine = nil
		return err
	}
	return nil
}
