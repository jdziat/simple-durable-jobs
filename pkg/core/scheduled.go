package core

import "time"

// ScheduledFire records the latest claimed fire boundary for a schedule.
type ScheduledFire struct {
	Name string `gorm:"primaryKey;size:255"`
	// LastFireAt is the scheduler cursor advanced on each successful claim
	// (and set on seed); it acts as the monotonic claim guard, so a no-op poll
	// that loses the last_fire_at < fireTime race does not advance it.
	LastFireAt time.Time
	// LastFiredAt is the real last-fire timestamp. It is nil until an actual
	// fire because seeding leaves it nil, and powers the dashboard's per-schedule
	// last-run view through GetScheduledFireTimes.
	LastFiredAt *time.Time
}
