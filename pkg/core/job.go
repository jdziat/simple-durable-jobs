// Package core provides the domain models and interfaces for the jobs package.
package core

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
	StatusWaiting   JobStatus = "waiting"   // Suspended waiting for sub-jobs
	StatusCancelled JobStatus = "cancelled" // Terminated before completion
	StatusPaused    JobStatus = "paused"    // Paused, won't be picked up
)

// Job represents a unit of work to be processed.
type Job struct {
	ID              string     `gorm:"primaryKey;size:36"`
	Type            string     `gorm:"index;size:255;not null"`
	Args            []byte     `gorm:"type:bytes"`
	Queue           string     `gorm:"index;size:255;default:'default'"`
	Priority        int        `gorm:"index;default:0"`
	Status          JobStatus  `gorm:"index;size:20;default:'pending'"`
	PreviousStatus  JobStatus  `gorm:"size:20"` // Status before pause, for restoration
	Attempt         int        `gorm:"default:0"`
	MaxRetries      int        `gorm:"default:3"`
	LastError       string     `gorm:"type:text"`
	RunAt           *time.Time `gorm:"index"`
	StartedAt       *time.Time
	CompletedAt     *time.Time
	CreatedAt       time.Time  `gorm:"autoCreateTime"`
	UpdatedAt       time.Time  `gorm:"autoUpdateTime"`
	LockedBy        string     `gorm:"size:255"`
	LockedUntil     *time.Time `gorm:"index"`
	LastHeartbeatAt *time.Time // Tracks when the last heartbeat was received
	UniqueKey       string     `gorm:"index;size:255"` // For job deduplication

	// Parent-child relationship
	ParentJobID *string `gorm:"index;size:36"`
	RootJobID   *string `gorm:"index;size:36"` // Top-level workflow job

	// Fan-out tracking
	FanOutID    *string `gorm:"index;size:36"` // Groups sibling sub-jobs
	FanOutIndex int     `gorm:"default:0"`     // Position in fan-out batch

	// Result storage for parent retrieval
	Result []byte `gorm:"type:bytes"` // Serialized return value
}

// Checkpoint stores the result of a durable Call() for replay.
type Checkpoint struct {
	ID        string    `gorm:"primaryKey;size:36"`
	JobID     string    `gorm:"index;size:36;not null"`
	CallIndex int       `gorm:"not null"`
	CallType  string    `gorm:"size:255;not null"`
	Result    []byte    `gorm:"type:bytes"`
	Error     string    `gorm:"type:text"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// FanOutCheckpoint stores fan-out state for job replay.
type FanOutCheckpoint struct {
	FanOutID  string `json:"fan_out_id"`
	CallIndex int    `json:"call_index"`
}
