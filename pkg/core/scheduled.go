package core

import "time"

// ScheduledFire records the latest claimed fire boundary for a schedule.
type ScheduledFire struct {
	Name        string `gorm:"primaryKey;size:255"`
	LastFireAt  time.Time
	LastFiredAt *time.Time
}
