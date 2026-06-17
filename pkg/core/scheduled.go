package core

import "time"

// ScheduledFire records the latest claimed fire boundary for a schedule.
type ScheduledFire struct {
	Name string `gorm:"primaryKey;size:255"`
	// LastFireAt is the scheduler cursor advanced on each successful claim
	// (and set on seed); it acts as the monotonic claim guard, so a no-op poll
	// that loses the last_fire_at < fireTime race does not advance it.
	LastFireAt time.Time
	// LastFiredAt is the scheduled fire BOUNDARY of the last real fire (the
	// schedule's computed Next() time that was claimed), not the wall-clock
	// instant the worker actually enqueued it. Recording the boundary keeps the
	// value identical across the fleet (a wall-clock stamp would differ per worker
	// by the poll delay). It is nil until an actual fire (seeding leaves it nil)
	// and powers the dashboard's per-schedule last-run view via GetScheduledFireTimes.
	LastFiredAt *time.Time
}
