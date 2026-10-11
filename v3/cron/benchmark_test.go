package cron

import (
	"context"
	"fmt"
	"testing"
)

// capturingLogger records the last Error record for assertions.
type capturingLogger struct {
	Logger
	lastMsg  string
	lastArgs []any
}

func (c *capturingLogger) Error(msg string, args ...any) {
	c.lastMsg = msg
	c.lastArgs = args
}

func TestCronLogger_PassesStructuredFieldsThrough(t *testing.T) {
	cap := &capturingLogger{Logger: NopLogger()}
	cl := cronLogger{logger: cap}
	cl.Error(fmt.Errorf("boom"), "job failed", "spec", "0 2 * * *")

	if cap.lastMsg != "job failed" {
		t.Fatalf("msg = %q", cap.lastMsg)
	}
	if len(cap.lastArgs) != 4 || cap.lastArgs[0] != "spec" || cap.lastArgs[2] != "error" {
		t.Fatalf("args = %v, want spec pair + error pair", cap.lastArgs)
	}
}

func TestScheduler_UTCZoneCaseInsensitive(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	task := &recordingTask{schedule: "0 2 * * *", timezone: " utc "}
	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := specOf(t, s); got != "0 2 * * *" {
		t.Fatalf("spec = %q, want no CRON_TZ prefix for lowercase utc", got)
	}
}

func TestDefaultSignalsIncludeTerm(t *testing.T) {
	found := map[any]bool{}
	for _, sig := range DefaultSignals() {
		found[fmt.Sprint(sig)] = true
	}
	if !found["interrupt"] || !found["terminated"] {
		t.Fatalf("DefaultSignals = %v, want interrupt + terminated (SIGTERM)", found)
	}
}

func BenchmarkBuilders_EveryDay(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = EveryDay("02:00")
	}
}

func BenchmarkBuilders_EveryMonth(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = EveryMonth(1, "00:00")
	}
}

func BenchmarkBuilders_EveryWeekly(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = EveryWeekly("mon", "07:45")
	}
}

// BenchmarkScheduler_AddFunc measures the cost of registering a job.
func BenchmarkScheduler_AddFunc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := NewScheduler(WithLogger(NopLogger()))
		b.StartTimer()
		if _, err := s.AddFunc("0 0 1 * *", func() {}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTaskRunner_Run measures the runner overhead on a no-op task.
func BenchmarkTaskRunner_Run(b *testing.B) {
	task := &recordingTask{}
	runner := NewTaskRunner(task)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx
		runner.Run()
	}
}

// BenchmarkResolveZone_CachedHit measures a cached zone lookup.
func BenchmarkResolveZone_CachedHit(b *testing.B) {
	_, _ = ResolveZone("Asia/Jakarta") // warm the cache
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ResolveZone("Asia/Jakarta"); err != nil {
			b.Fatal(err)
		}
	}
}
