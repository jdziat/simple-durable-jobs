package core

import "time"

// QueueState tracks the pause state of a queue.
type QueueState struct {
	Queue     string     `gorm:"primaryKey;size:255"`
	Paused    bool       `gorm:"default:false"`
	PausedAt  *time.Time
	PausedBy  string     `gorm:"size:255"`
	UpdatedAt time.Time  `gorm:"autoUpdateTime"`
}
