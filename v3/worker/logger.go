package worker

import (
	"context"
	"log/slog"
)

// Logger defines the minimal structured logging interface required by the worker engine.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// slogLogger wraps standard library *slog.Logger to fulfill the Logger interface.
type slogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a Logger backed by Go's standard log/slog.
func NewSlogLogger(l *slog.Logger) Logger {
	if l == nil {
		l = slog.Default()
	}
	return &slogLogger{logger: l}
}

// DefaultLogger returns a Logger backed by slog.Default().
func DefaultLogger() Logger {
	return NewSlogLogger(slog.Default())
}

func (s *slogLogger) Debug(msg string, args ...any) {
	s.logger.Log(context.Background(), slog.LevelDebug, msg, args...)
}

func (s *slogLogger) Info(msg string, args ...any) {
	s.logger.Log(context.Background(), slog.LevelInfo, msg, args...)
}

func (s *slogLogger) Warn(msg string, args ...any) {
	s.logger.Log(context.Background(), slog.LevelWarn, msg, args...)
}

func (s *slogLogger) Error(msg string, args ...any) {
	s.logger.Log(context.Background(), slog.LevelError, msg, args...)
}

// nopLogger is a silent no-op logger.
type nopLogger struct{}

// NopLogger returns a silent logger that discards all log messages.
func NopLogger() Logger {
	return &nopLogger{}
}

func (n *nopLogger) Debug(msg string, args ...any) {}
func (n *nopLogger) Info(msg string, args ...any)  {}
func (n *nopLogger) Warn(msg string, args ...any)  {}
func (n *nopLogger) Error(msg string, args ...any) {}

// customLogger wraps user-provided functions to fulfill the Logger interface.
type customLogger struct {
	debug func(msg string, args ...any)
	info  func(msg string, args ...any)
	warn  func(msg string, args ...any)
	err   func(msg string, args ...any)
}

// NewCustomLogger creates a Logger from individual log functions (e.g. bridging Zap or Zerolog).
func NewCustomLogger(
	debug func(msg string, args ...any),
	info func(msg string, args ...any),
	warn func(msg string, args ...any),
	err func(msg string, args ...any),
) Logger {
	nop := func(string, ...any) {}
	if debug == nil {
		debug = nop
	}
	if info == nil {
		info = nop
	}
	if warn == nil {
		warn = nop
	}
	if err == nil {
		err = nop
	}
	return &customLogger{debug: debug, info: info, warn: warn, err: err}
}

func (c *customLogger) Debug(msg string, args ...any) { c.debug(msg, args...) }
func (c *customLogger) Info(msg string, args ...any)  { c.info(msg, args...) }
func (c *customLogger) Warn(msg string, args ...any)  { c.warn(msg, args...) }
func (c *customLogger) Error(msg string, args ...any) { c.err(msg, args...) }
