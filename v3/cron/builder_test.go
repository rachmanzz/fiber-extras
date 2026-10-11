package cron

import (
	"testing"
)

func TestBuilders(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"minutes default", EveryMinutes(0), "* * * * *"},
		{"minutes 15", EveryMinutes(15), "*/15 * * * *"},
		{"hours default", EveryHours(0), "0 * * * *"},
		{"hours 6", EveryHours(6), "0 */6 * * *"},
		{"daily", EveryDay("02:30"), "30 2 * * *"},
		{"daily padded", EveryDay("23:05"), "5 23 * * *"},
		{"daily fallback", EveryDay("not-a-time"), "0 0 * * *"},
		{"monthly", EveryMonth(1, "00:00"), "0 0 1 * *"},
		{"monthly clamped high", EveryMonth(40, "01:30"), "30 1 1 * *"},
		{"monthly clamped low", EveryMonth(0, "05:10"), "10 5 1 * *"},
		{"weekly", EveryWeekly("mon", "07:45"), "45 7 * * 1"},
		{"weekly case-insensitive", EveryWeekly(" fri ", "06:00"), "0 6 * * 5"},
		{"weekly unknown day", EveryWeekly("funday", "06:00"), "0 6 * * 0"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}
