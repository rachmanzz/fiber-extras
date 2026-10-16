package cron

import (
	"context"
	"testing"
	"time"
)

type auditTask struct{}

func (auditTask) Execute(context.Context) error  { return nil }
func (auditTask) OnError(context.Context, error) {}
func (auditTask) OnFail(context.Context, error)  {}
func (auditTask) Schedule() string               { return "0 2 * * *" }
func (auditTask) Location() *time.Location       { return time.FixedZone("CustomWIB", 7*3600) }

func TestAuditFixedLocation(t *testing.T) {
	s := NewScheduler()
	if err := s.Register(auditTask{}); err != nil {
		t.Fatal(err)
	}
	next := s.Entries()[0].Schedule.Next(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
	want := time.Date(2026, 10, 16, 19, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next=%v want=%v", next, want)
	}
}
func TestAuditSecondsBuilder(t *testing.T) {
	s := NewScheduler(WithSeconds(true))
	for _, spec := range []string{EveryDay("02:00"), EveryMinutes(3), EveryHours(2), EveryMonth(1, "03:00"), EveryWeekly("MON", "04:00"), "15 * * * * *"} {
		id, err := s.AddFunc(spec, func() {})
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		next := s.Entries()[int(id)-1].Schedule.Next(time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC))
		seconds := 0
		if spec == "15 * * * * *" {
			seconds = 15
		}
		if next.Second() != seconds {
			t.Fatalf("%s: next=%v", spec, next)
		}
	}
}
