package core

import "time"

// RateLimitWindow tracks the number of admitted jobs for a named fleet-wide
// rate limit during one fixed time window.
type RateLimitWindow struct {
	LimitName string `gorm:"primaryKey;size:255"`
	// WindowStart is the truncated start of the fixed window. On MySQL it is
	// stored as datetime(3) (millisecond precision), so two windows whose starts
	// differ by less than a millisecond collapse onto one row. This never affects
	// real configs: fractional per-second rates derive a window >= 1s, and a
	// sub-millisecond explicit Window is not a meaningful rate-limit granularity.
	WindowStart time.Time `gorm:"primaryKey"`
	Count       int       `gorm:"not null;default:0"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}
