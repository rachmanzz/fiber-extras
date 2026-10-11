package cron

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// ResolveZone resolves a time zone name (IANA such as "Asia/Jakarta", or
// "UTC", or "Local") to a shared *time.Location. Results are cached, so
// repeated calls with the same name return the same pointer.
func ResolveZone(name string) (*time.Location, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return time.UTC, nil
	}

	zoneMu.RLock()
	if loc, ok := zoneCache[name]; ok {
		zoneMu.RUnlock()
		return loc, nil
	}
	zoneMu.RUnlock()

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("cron: unknown time zone %q: %w", name, err)
	}
	zoneMu.Lock()
	zoneCache[name] = loc
	zoneMu.Unlock()
	return loc, nil
}

// MustResolveZone is ResolveZone but panics on an unknown zone. Intended for
// application startup where a bad zone is a configuration bug worth crashing
// for, not for library internals.
func MustResolveZone(name string) *time.Location {
	loc, err := ResolveZone(name)
	if err != nil {
		panic(err)
	}
	return loc
}

// LocationAwareTask optionally supplies its schedule zone as a *time.Location
// (e.g. cron.TZ_ASIA_JAKARTA or cron.MustResolveZone(...)). A non-UTC location
// scopes the task's spec with a CRON_TZ prefix. When a task implements both
// LocationAwareTask and TimezoneAwareTask, Location() wins.
type LocationAwareTask interface {
	Task
	Location() *time.Location
}

var (
	zoneMu    sync.RWMutex
	zoneCache = map[string]*time.Location{}
)

// mustZone resolves at package init for the TZ_* constants below. If the host
// environment has no tzdata (e.g. a scratch container), it falls back to a
// fixed-offset zone -- which is exact for every constant here, because all of
// them are zones without daylight saving time. Importing _ "time/tzdata" in
// the host application removes the need for the fallback entirely.
func mustZone(name string, fixedOffsetSeconds int) *time.Location {
	loc, err := time.LoadLocation(name)
	if err == nil {
		return loc
	}
	return time.FixedZone(name, fixedOffsetSeconds)
}

// Pre-declared zones (*time.Location), all of them without DST so the
// no-tzdata fallback (fixed offset) is always correct. For DST zones or any
// other IANA name, use ResolveZone / MustResolveZone.
var (
	TZ_UTC               = mustZone("UTC", 0)
	TZ_ASIA_JAKARTA      = mustZone("Asia/Jakarta", 7*3600)  // WIB
	TZ_ASIA_MAKASSAR     = mustZone("Asia/Makassar", 8*3600) // WITA
	TZ_ASIA_JAYAPURA     = mustZone("Asia/Jayapura", 9*3600) // WIT
	TZ_ASIA_SINGAPORE    = mustZone("Asia/Singapore", 8*3600)
	TZ_ASIA_KUALA_LUMPUR = mustZone("Asia/Kuala_Lumpur", 8*3600)
	TZ_ASIA_BANGKOK      = mustZone("Asia/Bangkok", 7*3600)
	TZ_ASIA_HO_CHI_MINH  = mustZone("Asia/Ho_Chi_Minh", 7*3600)
	TZ_ASIA_MANILA       = mustZone("Asia/Manila", 8*3600)
	TZ_ASIA_HONG_KONG    = mustZone("Asia/Hong_Kong", 8*3600)
	TZ_ASIA_SHANGHAI     = mustZone("Asia/Shanghai", 8*3600)
	TZ_ASIA_TOKYO        = mustZone("Asia/Tokyo", 9*3600)
	TZ_ASIA_SEOUL        = mustZone("Asia/Seoul", 9*3600)
	TZ_ASIA_KOLKATA      = mustZone("Asia/Kolkata", 5*3600+1800)
	TZ_ASIA_DUBAI        = mustZone("Asia/Dubai", 4*3600)
	TZ_EUROPE_MOSCOW     = mustZone("Europe/Moscow", 3*3600)
	TZ_AMERICA_SAO_PAULO = mustZone("America/Sao_Paulo", -3*3600)
	TZ_AMERICA_LIMA      = mustZone("America/Lima", -5*3600)
)
