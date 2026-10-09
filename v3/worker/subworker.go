package worker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
)

// subWorkerPool implements the SubWorkerPool interface using goroutines, a concurrency semaphore, and panic safety.
type subWorkerPool struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	sem      chan struct{}
	wg       sync.WaitGroup
	errOnce  sync.Once
	firstErr error
	taskName string
	logger   Logger
}

func newSubWorkerPool(ctx context.Context, maxConcurrency int, taskName string, logger Logger) *subWorkerPool {
	if maxConcurrency <= 0 {
		maxConcurrency = 5
	}
	if logger == nil {
		logger = NopLogger()
	}

	cCtx, cancel := context.WithCancelCause(ctx)
	return &subWorkerPool{
		ctx:      cCtx,
		cancel:   cancel,
		sem:      make(chan struct{}, maxConcurrency),
		taskName: taskName,
		logger:   logger,
	}
}

// Go submits a function to be executed concurrently by a sub-worker.
func (p *subWorkerPool) Go(fn SubTaskFunc) {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		// Acquire semaphore slot or abort if context canceled
		select {
		case p.sem <- struct{}{}:
			defer func() { <-p.sem }()
		case <-p.ctx.Done():
			return
		}

		// Panic recovery guard
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				err := fmt.Errorf("sub-worker panic in task %s: %v", p.taskName, r)
				p.logger.Error("sub-worker recovered from panic",
					"task", p.taskName,
					"panic", r,
					"stack", stack,
				)
				p.setError(err)
			}
		}()

		if err := fn(p.ctx); err != nil {
			p.setError(err)
		}
	}()
}

func (p *subWorkerPool) setError(err error) {
	if err == nil {
		return
	}
	p.errOnce.Do(func() {
		p.firstErr = err
		p.cancel(err)
	})
}

// Wait blocks until all sub-workers have completed and returns the first error encountered.
func (p *subWorkerPool) Wait() error {
	p.wg.Wait()
	return p.firstErr
}
