package cron

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestResolveZone_CachesAndSharesPointers(t *testing.T) {
	a, err := ResolveZone("Asia/Jakarta")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	b, err := ResolveZone("Asia/Jakarta")
	if err != nil {
		t.Fatalf("resolve again: %v", err)
	}
	if a != b {
		t.Fatal("ResolveZone must return the same pointer for the same name")
	}
	if a.String() != "Asia/Jakarta" {
		t.Fatalf("zone name = %q", a.String())
	}
}

func TestResolveZone_EmptyAndUTC(t *testing.T) {
	empty, err := ResolveZone("  ")
	if err != nil || empty != time.UTC {
		t.Fatalf("empty name = (%v, %v), want UTC", empty, err)
	}
	utc, err := ResolveZone("UTC")
	if err != nil || utc != time.UTC {
		t.Fatalf("UTC = (%v, %v), want time.UTC", utc, err)
	}
	if _, err := ResolveZone("Mars/Olympus"); err == nil {
		t.Fatal("expected error for unknown zone")
	}
}

func TestResolveZone_LocalIsValid(t *testing.T) {
	if _, err := ResolveZone("Local"); err != nil {
		t.Fatalf("Local must resolve: %v", err)
	}
}

func TestMustResolveZone_PanicsOnUnknown(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustResolveZone must panic on an unknown zone")
		}
	}()
	MustResolveZone("Mars/Olympus")
}

// TestTZConstants_FixedOffsets verifies the TZ_* constants behave exactly like
// their zones even in the no-tzdata fixed-offset fallback (all of them are
// DST-free zones, so the offset is stable year-round).
func TestTZConstants_FixedOffsets(t *testing.T) {
	probe := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		loc        *time.Location
		name       string
		offsetSecs int
	}{
		{TZ_ASIA_JAKARTA, "Asia/Jakarta", 7 * 3600},
		{TZ_ASIA_MAKASSAR, "Asia/Makassar", 8 * 3600},
		{TZ_ASIA_JAYAPURA, "Asia/Jayapura", 9 * 3600},
		{TZ_ASIA_SINGAPORE, "Asia/Singapore", 8 * 3600},
		{TZ_ASIA_TOKYO, "Asia/Tokyo", 9 * 3600},
		{TZ_ASIA_KOLKATA, "Asia/Kolkata", 5*3600 + 1800},
		{TZ_AMERICA_SAO_PAULO, "America/Sao_Paulo", -3 * 3600},
	}
	for _, tc := range cases {
		if tc.loc == nil {
			t.Fatalf("%s: nil location", tc.name)
		}
		if _, offset := probe.In(tc.loc).Zone(); offset != tc.offsetSecs {
			t.Errorf("%s: offset = %d, want %d", tc.name, offset, tc.offsetSecs)
		}
		if !strings.HasPrefix(tc.loc.String(), tc.name) {
			t.Errorf("%s: String() = %q", tc.name, tc.loc.String())
		}
	}
}

func TestScheduler_LocationAwareTaskScopesSpec(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	task := &locationTask{loc: TZ_ASIA_JAKARTA, schedule: "0 2 * * *"}
	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := specOf(t, s); got != "CRON_TZ=Asia/Jakarta 0 2 * * *" {
		t.Fatalf("spec = %q, want CRON_TZ prefix from the location", got)
	}
}

func TestScheduler_LocationWinsOverTimezoneString(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	task := &mixedZoneTask{loc: TZ_ASIA_TOKYO, timezone: "Asia/Jakarta", schedule: "0 2 * * *"}
	if err := s.Register(task); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := specOf(t, s); got != "CRON_TZ=Asia/Tokyo 0 2 * * *" {
		t.Fatalf("spec = %q, want Location() to win", got)
	}
}

func TestScheduler_NilOrUTCLocationUnprefixed(t *testing.T) {
	s := NewScheduler(WithLogger(NopLogger()))
	if err := s.Register(&locationTask{loc: nil, schedule: "0 2 * * *"}); err != nil {
		t.Fatalf("register nil loc: %v", err)
	}
	if got := specOf(t, s); got != "0 2 * * *" {
		t.Fatalf("spec = %q, want unprefixed for nil location", got)
	}

	s2 := NewScheduler(WithLogger(NopLogger()))
	if err := s2.Register(&locationTask{loc: TZ_UTC, schedule: "0 3 * * *"}); err != nil {
		t.Fatalf("register utc loc: %v", err)
	}
	if got := specOf(t, s2); got != "0 3 * * *" {
		t.Fatalf("spec = %q, want unprefixed for UTC location", got)
	}
}

// --- test helpers -----------------------------------------------------------

type locationTask struct {
	loc      *time.Location
	schedule string
}

func (t *locationTask) Execute(_ context.Context) error      { return nil }
func (t *locationTask) OnError(_ context.Context, err error) {}
func (t *locationTask) OnFail(_ context.Context, err error)  {}
func (t *locationTask) Schedule() string                     { return t.schedule }
func (t *locationTask) Location() *time.Location             { return t.loc }

type mixedZoneTask struct {
	loc      *time.Location
	timezone string
	schedule string
}

func (t *mixedZoneTask) Execute(_ context.Context) error      { return nil }
func (t *mixedZoneTask) OnError(_ context.Context, err error) {}
func (t *mixedZoneTask) OnFail(_ context.Context, err error)  {}
func (t *mixedZoneTask) Schedule() string                     { return t.schedule }
func (t *mixedZoneTask) Timezone() string                     { return t.timezone }
func (t *mixedZoneTask) Location() *time.Location             { return t.loc }
