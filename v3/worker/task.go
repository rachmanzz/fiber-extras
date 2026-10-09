package worker

import (
	"context"
)

// Task defines the executable contract for any background unit of work.
type Task interface {
	// Name returns the unique identifier or category for the task.
	Name() string

	// Execute runs the core logic of the task.
	// It receives the parent context and SubWorkerPool to spawn parallel sub-workers if needed.
	Execute(ctx context.Context, pool SubWorkerPool) error
}

// TaskFunc is an adapter to allow the use of ordinary functions as Tasks.
type TaskFunc struct {
	taskName string
	run      func(ctx context.Context, pool SubWorkerPool) error
}

// NewTaskFunc creates a new Task from a function closure.
func NewTaskFunc(name string, run func(ctx context.Context, pool SubWorkerPool) error) Task {
	return &TaskFunc{taskName: name, run: run}
}

// Name returns the identifier of the task.
func (t *TaskFunc) Name() string {
	return t.taskName
}

// Execute invokes the underlying function closure.
func (t *TaskFunc) Execute(ctx context.Context, pool SubWorkerPool) error {
	return t.run(ctx, pool)
}

// SubTaskFunc represents a single sub-task executed concurrently by a sub-worker.
type SubTaskFunc func(ctx context.Context) error

// SubWorkerPool defines the contract for splitting a parent task into parallel sub-workers.
type SubWorkerPool interface {
	// Go enqueues a sub-task to be executed by a sub-worker in parallel.
	Go(fn SubTaskFunc)

	// Wait blocks until all spawned sub-workers have finished and returns the first non-nil error if any.
	Wait() error
}
