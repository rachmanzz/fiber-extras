package worker_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rachmanzz/fiber-extras/v3/worker"
)

func BenchmarkWorker_Dispatch_Async(b *testing.B) {
	engine := worker.NewEngine(worker.Config{
		MaxWorkers:    8,
		TaskQueueSize: b.N + 1000,
		Logger:        worker.NopLogger(),
	})
	engine.Start()
	defer engine.Shutdown()

	ctx := context.Background()
	task := worker.NewTaskFunc("bench.task", func(tCtx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.Dispatch(ctx, task)
	}
}

func BenchmarkWorker_ExecuteSync(b *testing.B) {
	engine := worker.NewEngine(worker.Config{
		MaxWorkers:     4,
		SubWorkerLimit: 4,
		Logger:         worker.NopLogger(),
	})
	engine.Start()
	defer engine.Shutdown()

	ctx := context.Background()
	task := worker.NewTaskFunc("bench.sync", func(tCtx context.Context, pool worker.SubWorkerPool) error {
		return nil
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.ExecuteSync(ctx, task)
	}
}

func BenchmarkWorker_SubWorker_FanOut(b *testing.B) {
	engine := worker.NewEngine(worker.Config{
		MaxWorkers:     4,
		SubWorkerLimit: 8,
		Logger:         worker.NopLogger(),
	})
	engine.Start()
	defer engine.Shutdown()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var counter int64
		task := worker.NewTaskFunc("bench.fanout", func(tCtx context.Context, pool worker.SubWorkerPool) error {
			for j := 0; j < 5; j++ {
				pool.Go(func(subCtx context.Context) error {
					atomic.AddInt64(&counter, 1)
					return nil
				})
			}
			return pool.Wait()
		})
		_ = engine.ExecuteSync(ctx, task)
	}
}
