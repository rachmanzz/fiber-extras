package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// specOf returns the resolved spec of the only registered entry.
func specOf(t *testing.T, s *Scheduler) string {
	t.Helper()
	specs := s.Registered()
	if len(specs) != 1 {
		t.Fatalf("expected 1 registered spec, got %d", len(specs))
	}
	return specs[0]
}

// recordingTask captures every lifecycle outcome for assertions.
type recordingTask struct {
	mu          sync.Mutex
	schedule    string
	timezone    string
	writeSched  string
	executeErr  error
	panicWith   any
	executions  int
	onErrorRuns int
	onFailErrs  []error
}

func (t *recordingTask) Execute(_ context.Context) error {
	t.mu.Lock()
	t.executions++
	panicValue, shouldPanic := t.panicWith, t.panicWith != nil
	execErr := t.executeErr
	t.mu.Unlock()
	if shouldPanic {
		panic(panicValue)
	}
	return execErr
}

func (t *recordingTask) OnError(_ context.Context, err error) {
	t.mu.Lock()
	t.onErrorRuns++
	t.mu.Unlock()
}

func (t *recordingTask) OnFail(_ context.Context, err error) {
	t.mu.Lock()
	t.onFailErrs = append(t.onFailErrs, err)
	t.mu.Unlock()
}

func (t *recordingTask) Schedule() string      { return t.schedule }
func (t *recordingTask) Timezone() string      { return t.timezone }
func (t *recordingTask) WriteSchedule() string { return t.writeSched }

func (t *recordingTask) snapshot() (executions, onErrorRuns, onFailRuns int, lastFail error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.executions, t.onErrorRuns, len(t.onFailErrs), func() error {
		if len(t.onFailErrs) > 0 {
			return t.onFailErrs[len(t.onFailErrs)-1]
		}
		return nil
	}()
}

func TestTaskRunner_ErrorsRouteToOnError(t *testing.T) {
	want := errors.New("boom")
	task := &recordingTask{executeErr: want}
	NewTaskRunner(task).Run()

	executions, onError, onFail, _ := task.snapshot()
	if executions != 1 || onError != 1 || onFail != 0 {
		t.Fatalf("executions=%d onError=%d onFail=%d", executions, onError, onFail)
	}
}

func TestTaskRunner_PanicsRouteToOnFail(t *testing.T) {
	task := &recordingTask{panicWith: "kaboom"}
	NewTaskRunner(task).Run()

	_, onError, onFail, lastFail := task.snapshot()
	if onError != 0 || onFail != 1 {
		t.Fatalf("onError=%d onFail=%d", onError, onFail)
	}
	if lastFail == nil || lastFail.Error() != `cron task panicked: kaboom` {
		t.Fatalf("unexpected OnFail error: %v", lastFail)
	}
}

func TestScheduler_RegisterResolvesWriteScheduleOverSchedule(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	task := &recordingTask{schedule: "not-a-spec", writeSched: "*/1 * * * * *"}
	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := specOf(t, s); got != "*/1 * * * * *" {
		t.Fatalf("spec = %q, want WriteSchedule() to win", got)
	}
}

func TestScheduler_RegisterRejectsMissingSchedule(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	task := &recordingTask{}
	err := s.Register(task)
	if err == nil {
		t.Fatal("expected error for task without schedule")
	}
	if _, _, onFail, _ := task.snapshot(); onFail != 0 {
		t.Fatalf("expected OnFail NOT to be invoked on static register error, got %d", onFail)
	}
}

func TestScheduler_RegisterScopesTimezone(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	task := &recordingTask{schedule: "0 2 * * *", timezone: "Asia/Jakarta"}
	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := specOf(t, s); got != "CRON_TZ=Asia/Jakarta 0 2 * * *" {
		t.Fatalf("spec = %q, want CRON_TZ prefix", got)
	}

	utc := NewScheduler(WithLogger(NopLogger()))
	utcTask := &recordingTask{schedule: "0 2 * * *", timezone: "UTC"}
	if err := utc.Register(utcTask); err != nil {
		t.Fatalf("register utc: %v", err)
	}
	if got := specOf(t, utc); got != "0 2 * * *" {
		t.Fatalf("UTC task spec = %q, want unprefixed", got)
	}
}

func TestScheduler_ExecutionAndSkipOverlap(t *testing.T) {
	var (
		runs       atomic.Int64
		active     atomic.Int64
		maxActive  atomic.Int64
		jobOnce    sync.Once
		jobStarted = make(chan struct{})
	)
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	if _, err := s.AddFunc("*/1 * * * * *", func() {
		jobOnce.Do(func() { close(jobStarted) })
		// Track concurrent executions: SkipIfStillRunning must keep the job
		// single-flight even though every tick fires while this sleeps.
		n := active.Add(1)
		for {
			cur := maxActive.Load()
			if n <= cur || maxActive.CompareAndSwap(cur, n) {
				break
			}
		}
		runs.Add(1)
		time.Sleep(2 * time.Second) // slower than the tick: next triggers must be skipped
		active.Add(-1)
	}); err != nil {
		t.Fatalf("addfunc: %v", err)
	}
	s.Start()

	// The scheduler must actually trigger the job within the window.
	select {
	case <-jobStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("job never executed; scheduler did not trigger it")
	}
	// Observe ~2 further ticks (one skipped while the job sleeps).
	time.Sleep(2500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if n := runs.Load(); n < 1 {
		t.Fatalf("job executed %d times, want at least 1", n)
	}
	if m := maxActive.Load(); m != 1 {
		t.Fatalf("overlap detected: %d concurrent executions of the same job", m)
	}
	if n := runs.Load(); n > 2 {
		t.Fatalf("skip-overlap failed: %d overlapping executions", n)
	}
}

func TestScheduler_StopWaitsForInFlightJobs(t *testing.T) {
	var finished atomic.Bool
	var once sync.Once
	started := make(chan struct{})
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	if _, err := s.AddFunc("*/1 * * * * *", func() {
		once.Do(func() { close(started) })
		time.Sleep(3 * time.Second)
		finished.Store(true)
	}); err != nil {
		t.Fatalf("addfunc: %v", err)
	}
	s.Start()

	// Wait until a job is genuinely in flight before stopping.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job never became in-flight")
	}

	// Stop must not return before the in-flight job finished.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !finished.Load() {
		t.Fatal("Stop returned while the in-flight job was still running")
	}
}

func TestScheduler_RegisterAll(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	err := s.RegisterAll(
		&recordingTask{schedule: "0 1 * * *"},
		&recordingTask{schedule: "0 2 * * *"},
		&recordingTask{schedule: "0 3 * * *"},
	)
	if err != nil {
		t.Fatalf("register all: %v", err)
	}
	if got := len(s.Registered()); got != 3 {
		t.Fatalf("registered %d specs, want 3", got)
	}
}

func TestScheduler_RegisterAllStopsAtFirstFailure(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	bad := &recordingTask{} // no schedule
	err := s.RegisterAll(
		&recordingTask{schedule: "0 1 * * *"},
		bad,
		&recordingTask{schedule: "0 3 * * *"}, // never reached
	)
	if err == nil {
		t.Fatal("expected error from the task without a schedule")
	}
	specs := s.Registered()
	if len(specs) != 1 {
		t.Fatalf("registered %d specs, want 1 (stop at first failure)", len(specs))
	}
	if _, _, onFail, _ := bad.snapshot(); onFail != 0 {
		t.Fatalf("failing task OnFail ran %d times, want 0 during registration", onFail)
	}
}

type contextAwareTask struct {
	schedule string
	execute  func(ctx context.Context) error
}

func (t *contextAwareTask) Schedule() string { return t.schedule }
func (t *contextAwareTask) Execute(ctx context.Context) error {
	if t.execute != nil {
		return t.execute(ctx)
	}
	return nil
}
func (t *contextAwareTask) OnError(_ context.Context, _ error) {}
func (t *contextAwareTask) OnFail(_ context.Context, _ error)  {}

func TestScheduler_StopCancelsTaskContext(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	cancelled := make(chan struct{})
	started := make(chan struct{})

	task := &contextAwareTask{
		schedule: "* * * * * *",
		execute: func(ctx context.Context) error {
			select {
			case <-started:
			default:
				close(started)
			}
			select {
			case <-ctx.Done():
				close(cancelled)
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		},
	}

	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}

	s.Start()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case <-cancelled:
	case <-time.After(1 * time.Second):
		t.Fatal("expected task to receive context cancellation on Stop")
	}
}

func TestScheduler_RestartableAfterStop(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()), WithSeconds(true))
	run1Started := make(chan struct{})
	run2Started := make(chan struct{})
	var isRun2 atomic.Bool

	task := &contextAwareTask{
		schedule: "* * * * * *",
		execute: func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Errorf("task executed with already-cancelled context: %v", ctx.Err())
				return ctx.Err()
			}
			if !isRun2.Load() {
				select {
				case <-run1Started:
				default:
					close(run1Started)
				}
			} else {
				select {
				case <-run2Started:
				default:
					close(run2Started)
				}
			}
			return nil
		},
	}

	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 1. First Start & Stop
	s.Start()
	select {
	case <-run1Started:
	case <-time.After(3 * time.Second):
		t.Fatal("first run did not start")
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	if err := s.Stop(ctx1); err != nil {
		t.Fatalf("first stop: %v", err)
	}

	// 2. Restart Start
	isRun2.Store(true)
	s.Start()
	select {
	case <-run2Started:
	case <-time.After(3 * time.Second):
		t.Fatal("second run did not start after restart")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := s.Stop(ctx2); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

