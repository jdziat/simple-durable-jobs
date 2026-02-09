// Package jobs provides a durable job queue with checkpointed nested calls.
package jobs

import (
	"time"
)

// JobStatus represents the current state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusRetrying  JobStatus = "retrying"
)

// Job represents a unit of work to be processed.
type Job struct {
	ID          string     `gorm:"primaryKey;size:36"`
	Type        string     `gorm:"index;size:255;not null"`
	Args        []byte     `gorm:"type:bytes"`
	Queue       string     `gorm:"index;size:255;default:'default'"`
	Priority    int        `gorm:"index;default:0"`
	Status      JobStatus  `gorm:"index;size:20;default:'pending'"`
	Attempt     int        `gorm:"default:0"`
	MaxRetries  int        `gorm:"default:3"`
	LastError   string     `gorm:"type:text"`
	RunAt       *time.Time `gorm:"index"`
	StartedAt   *time.Time
	CompletedAt *time.Time
	CreatedAt   time.Time  `gorm:"autoCreateTime"`
	UpdatedAt   time.Time  `gorm:"autoUpdateTime"`
	LockedBy    string     `gorm:"size:255"`
	LockedUntil *time.Time `gorm:"index"`
}
