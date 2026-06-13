package core

import "time"

// RateLimitWindow tracks the number of admitted jobs for a named fleet-wide
// rate limit during one fixed time window.
type RateLimitWindow struct {
	LimitName   string    `gorm:"primaryKey;size:255"`
	WindowStart time.Time `gorm:"primaryKey"`
	Count       int       `gorm:"not null;default:0"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}
