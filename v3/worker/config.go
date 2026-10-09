package worker

import (
	"time"
)

// Default configuration constants.
const (
	DefaultMaxWorkers      = 10
	DefaultSubWorkerLimit  = 5
	DefaultTaskQueueSize   = 1000
	DefaultShutdownTimeout = 15 * time.Second
)

// Config holds configuration parameters for the Worker Engine.
type Config struct {
	// MaxWorkers is the total number of background worker goroutines (default: 10).
	MaxWorkers int

	// SubWorkerLimit is the maximum parallel sub-workers spawned per task fan-out (default: 5).
	SubWorkerLimit int

	// TaskQueueSize is the buffer size of the task queue channel (default: 1000).
	TaskQueueSize int

	// ShutdownTimeout is the maximum time to wait for active tasks during graceful shutdown (default: 15s).
	ShutdownTimeout time.Duration

	// Logger is the pluggable logger used for worker lifecycle and errors (default: slog.Default()).
	Logger Logger
}

// DefaultConfig returns a Config struct populated with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		MaxWorkers:      DefaultMaxWorkers,
		SubWorkerLimit:  DefaultSubWorkerLimit,
		TaskQueueSize:   DefaultTaskQueueSize,
		ShutdownTimeout: DefaultShutdownTimeout,
		Logger:          DefaultLogger(),
	}
}

// Option defines a functional option for configuring the Worker Engine.
type Option func(*Config)

// WithMaxWorkers sets the maximum number of worker goroutines.
func WithMaxWorkers(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.MaxWorkers = n
		}
	}
}

// WithSubWorkerLimit sets the maximum parallel sub-workers per task fan-out.
func WithSubWorkerLimit(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.SubWorkerLimit = n
		}
	}
}

// WithTaskQueueSize sets the buffer size for the task queue channel.
func WithTaskQueueSize(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.TaskQueueSize = n
		}
	}
}

// WithShutdownTimeout sets the maximum duration to wait during graceful shutdown.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.ShutdownTimeout = d
		}
	}
}

// WithLogger sets the logger used by the worker engine.
func WithLogger(l Logger) Option {
	return func(c *Config) {
		if l != nil {
			c.Logger = l
		}
	}
}
