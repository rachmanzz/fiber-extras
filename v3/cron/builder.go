package cron

import (
	"fmt"
	"strings"
	"time"
)

// EveryMinutes returns a cron spec that runs every N minutes ("*" when N<=1).
func EveryMinutes(minutes int) string {
	if minutes <= 1 {
		return "* * * * *"
	}
	return fmt.Sprintf("*/%d * * * *", minutes)
}

// EveryHours returns a cron spec that runs every N hours at minute 0.
func EveryHours(hours int) string {
	if hours <= 1 {
		return "0 * * * *"
	}
	return fmt.Sprintf("0 */%d * * *", hours)
}

// EveryDay returns a cron spec that runs daily at "HH:MM" (e.g. "02:00").
// An unparsable time falls back to midnight.
func EveryDay(timeStr string) string {
	t, err := time.Parse("15:04", strings.TrimSpace(timeStr))
	if err != nil {
		return "0 0 * * *"
	}
	return fmt.Sprintf("%d %d * * *", t.Minute(), t.Hour())
}

// EveryMonth returns a cron spec that runs on the given day of the month at
// "HH:MM". Day is clamped to 1..31; an unparsable time falls back to midnight.
func EveryMonth(day int, timeStr string) string {
	if day < 1 || day > 31 {
		day = 1
	}
	t, err := time.Parse("15:04", strings.TrimSpace(timeStr))
	if err != nil {
		return fmt.Sprintf("0 0 %d * *", day)
	}
	return fmt.Sprintf("%d %d %d * *", t.Minute(), t.Hour(), day)
}

// EveryWeekly returns a cron spec that runs weekly on the given weekday
// ("SUN", "MON", ..., case-insensitive) at "HH:MM". An unknown weekday falls
// back to Sunday; an unparsable time falls back to midnight.
func EveryWeekly(weekday string, timeStr string) string {
	dayMap := map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}
	dow, ok := dayMap[strings.ToUpper(strings.TrimSpace(weekday))]
	if !ok {
		dow = 0
	}
	t, err := time.Parse("15:04", strings.TrimSpace(timeStr))
	if err != nil {
		return fmt.Sprintf("0 0 * * %d", dow)
	}
	return fmt.Sprintf("%d %d * * %d", t.Minute(), t.Hour(), dow)
}
