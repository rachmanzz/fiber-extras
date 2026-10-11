package cron

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// EntryID identifies a scheduled cron job.
type EntryID = cron.EntryID

// Job is an interface for jobs submitted to the scheduler.
type Job = cron.Job

// schedulerConfig carries the resolved Scheduler options.
type schedulerConfig struct {
	location    *time.Location
	skipOverlap bool
	withSeconds bool
	logger      Logger
}

// Option configures the Scheduler.
type Option func(*schedulerConfig)

// WithLocation sets the base timezone for cron schedules (default UTC).
// Per-task zones (TimezoneAwareTask) still take precedence via CRON_TZ.
func WithLocation(loc *time.Location) Option {
	return func(c *schedulerConfig) {
		if loc != nil {
			c.location = loc
		}
	}
}

// WithSeconds enables 6-field cron expressions with second precision.
func WithSeconds(enable bool) Option {
	return func(c *schedulerConfig) {
		c.withSeconds = enable
	}
}

// WithoutSkipOverlap allows overlapping invocations of the same job. By
// default a trigger is skipped while the previous invocation is still
// running, which protects long maintenance jobs from piling up.
func WithoutSkipOverlap() Option {
	return func(c *schedulerConfig) {
		c.skipOverlap = false
	}
}

// WithLogger sets the module logger (default slog.Default()).
func WithLogger(l Logger) Option {
	return func(c *schedulerConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// Scheduler wraps robfig/cron/v3 with production defaults: panic recovery,
// skip-if-still-running protection, a pluggable logger, and a graceful
// stop that waits for in-flight jobs.
type Scheduler struct {
	cronEngine *cron.Cron
	location   *time.Location
	logger     Logger

	specMu sync.Mutex
	specs  []string
}

// cronLogger adapts the module Logger to the robfig/cron Logger interface,
// passing structured key/value pairs straight through and appending the
// error as a trailing pair so records stay balanced.
type cronLogger struct {
	logger Logger
}

func (c cronLogger) Info(msg string, keysAndValues ...any) {
	c.logger.Info(msg, keysAndValues...)
}

func (c cronLogger) Error(err error, msg string, keysAndValues ...any) {
	c.logger.Error(msg, append(keysAndValues, "error", err)...)
}

// NewScheduler creates a Scheduler with production defaults.
func NewScheduler(opts ...Option) *Scheduler {
	cfg := &schedulerConfig{
		location:    time.UTC,
		skipOverlap: true,
		withSeconds: false,
		logger:      DefaultLogger(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	wrappers := []cron.JobWrapper{cron.Recover(cronLogger{logger: cfg.logger})}
	if cfg.skipOverlap {
		wrappers = append(wrappers, cron.SkipIfStillRunning(cronLogger{logger: cfg.logger}))
	}

	cronOpts := []cron.Option{
		cron.WithLocation(cfg.location),
		cron.WithLogger(cronLogger{logger: cfg.logger}),
		cron.WithChain(wrappers...),
	}
	if cfg.withSeconds {
		cronOpts = append(cronOpts, cron.WithSeconds())
	}

	return &Scheduler{
		cronEngine: cron.New(cronOpts...),
		location:   cfg.location,
		logger:     cfg.logger,
	}
}

// AddFunc registers a bare function on the given cron schedule.
func (s *Scheduler) AddFunc(spec string, cmd func()) (EntryID, error) {
	id, err := s.cronEngine.AddFunc(spec, cmd)
	if err != nil {
		return 0, fmt.Errorf("cron: invalid schedule %q: %w", spec, err)
	}
	s.recordSpec(spec)
	return id, nil
}

// AddJob registers a cron.Job on the given cron schedule.
func (s *Scheduler) AddJob(spec string, cmd Job) (EntryID, error) {
	id, err := s.cronEngine.AddJob(spec, cmd)
	if err != nil {
		return 0, fmt.Errorf("cron: invalid schedule %q: %w", spec, err)
	}
	s.recordSpec(spec)
	return id, nil
}

func (s *Scheduler) recordSpec(spec string) {
	s.specMu.Lock()
	s.specs = append(s.specs, spec)
	s.specMu.Unlock()
}

// Registered returns the resolved specs currently registered with the
// scheduler (including any CRON_TZ prefix), in registration order.
func (s *Scheduler) Registered() []string {
	s.specMu.Lock()
	defer s.specMu.Unlock()
	out := make([]string, len(s.specs))
	copy(out, s.specs)
	return out
}

// Register binds a self-contained Task to the scheduler. The spec is resolved
// from WriteSchedule() when implemented and non-empty, otherwise from
// Schedule(). A TimezoneAwareTask with a non-UTC zone scopes the spec with a
// CRON_TZ prefix. Invalid specs are returned as errors (never a process
// exit): wire the returned error to your application's startup handling.
func (s *Scheduler) Register(task Task) error {
	spec := ""
	if fluent, ok := task.(FluentScheduledTask); ok {
		spec = fluent.WriteSchedule()
	}
	if spec == "" {
		if sched, ok := task.(ScheduledTask); ok {
			spec = sched.Schedule()
		}
	}
	if spec == "" {
		err := fmt.Errorf("cron: task %T has no schedule (implement Schedule() or WriteSchedule())", task)
		task.OnFail(context.Background(), err)
		return err
	}

	// Resolve the effective schedule zone: a typed Location wins over the
	// string form (it is already resolved and IANA-correct); "Local" and
	// fixed zones are both valid CRON_TZ values since they round-trip through
	// loc.String().
	var tzPrefix string
	if locTask, ok := task.(LocationAwareTask); ok {
		if loc := locTask.Location(); loc != nil && loc.String() != "UTC" {
			tzPrefix = loc.String()
		}
	} else if tzTask, ok := task.(TimezoneAwareTask); ok {
		// Compare case-insensitively for the UTC shortcut, but keep the
		// original spelling: IANA zone names are case-sensitive.
		tz := strings.TrimSpace(tzTask.Timezone())
		if tz != "" && !strings.EqualFold(tz, "UTC") {
			tzPrefix = tz
		}
	}
	if tzPrefix != "" {
		spec = fmt.Sprintf("CRON_TZ=%s %s", tzPrefix, spec)
	}

	id, err := s.AddJob(spec, NewTaskRunner(task))
	if err != nil {
		task.OnFail(context.Background(), err)
		return err
	}
	s.logger.Info("cron task registered", "spec", spec, "entry", id)
	return nil
}

// RegisterAll binds every task to the scheduler in order and stops at the
// first failure (the failing task's OnFail has already been invoked). It is
// the convenience form of calling Register for each task.
func (s *Scheduler) RegisterAll(tasks ...Task) error {
	for _, task := range tasks {
		if err := s.Register(task); err != nil {
			return err
		}
	}
	return nil
}

// Start begins scheduling in a background goroutine.
func (s *Scheduler) Start() {
	s.cronEngine.Start()
	s.logger.Info("cron scheduler started", "timezone", s.location.String())
}

// Stop stops the scheduler and waits for in-flight jobs to complete, or
// returns ctx.Err() when ctx expires first. Running jobs are given the
// chance to finish; they are not cancelled.
func (s *Scheduler) Stop(ctx context.Context) error {
	stopCtx := s.cronEngine.Stop()
	select {
	case <-stopCtx.Done():
		s.logger.Info("cron scheduler stopped cleanly")
		return nil
	case <-ctx.Done():
		s.logger.Warn("cron scheduler stop timed out waiting for running jobs")
		return ctx.Err()
	}
}

// Entries returns the currently scheduled jobs.
func (s *Scheduler) Entries() []cron.Entry {
	return s.cronEngine.Entries()
}
