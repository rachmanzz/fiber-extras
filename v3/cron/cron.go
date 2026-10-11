// Package cron provides a production-grade scheduler on top of robfig/cron/v3.
//
// It is intentionally dependency-free apart from the cron engine itself: no
// queue, no worker, no logging framework. The module offers:
//
//   - a Task contract with lifecycle hooks (Execute/OnError/OnFail),
//   - declarative schedules (Schedule()/WriteSchedule()/Timezone()),
//   - fluent schedule builders (EveryDay, EveryMonth, ...),
//   - panic recovery and skip-if-still-running protection,
//   - graceful shutdown with a deadline,
//   - a standalone Runner with lifecycle hooks and signal handling.
//
// A minimal usage example:
//
//	s := cron.NewScheduler()
//	err := s.Register(myTask{}) // implements cron.ScheduledTask
//	if err != nil {
//		return err
//	}
//	s.Start()
//	defer func() { _ = s.Stop(context.Background()) }()
package cron

import (
	"context"
	"log/slog"
)

// Logger is the minimal structured logging interface used by the module.
// It mirrors the core queue logger so hosts can share one implementation.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type slogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger returns a Logger backed by Go's standard log/slog.
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

// NopLogger discards every log record; useful in tests and embedded setups.
func NopLogger() Logger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Debug(msg string, args ...any) {}
func (nopLogger) Info(msg string, args ...any)  {}
func (nopLogger) Warn(msg string, args ...any)  {}
func (nopLogger) Error(msg string, args ...any) {}

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
