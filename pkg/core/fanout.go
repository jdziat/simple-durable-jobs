package core

import (
	"time"
)

// FanOutStrategy defines how sub-job failures affect the parent.
type FanOutStrategy string

const (
	StrategyFailFast   FanOutStrategy = "fail_fast"   // Fail parent on first sub-job failure
	StrategyCollectAll FanOutStrategy = "collect_all" // Wait for all, return partial results
	StrategyThreshold  FanOutStrategy = "threshold"   // Ok if threshold% succeed
)

// FanOutStatus represents the state of a fan-out batch.
type FanOutStatus string

const (
	FanOutPending   FanOutStatus = "pending"
	FanOutCompleted FanOutStatus = "completed"
	FanOutFailed    FanOutStatus = "failed"
)

// FanOut tracks a batch of sub-jobs spawned by a parent job.
type FanOut struct {
	ID             string         `gorm:"primaryKey;size:36"`
	ParentJobID    string         `gorm:"index;size:36;not null"`
	TotalCount     int            `gorm:"not null"`
	CompletedCount int            `gorm:"default:0"`
	FailedCount    int            `gorm:"default:0"`
	CancelledCount int            `gorm:"default:0"`
	Strategy       FanOutStrategy `gorm:"size:20;default:'fail_fast'"`
	Threshold      float64        `gorm:"default:1.0"`
	Status         FanOutStatus   `gorm:"size:20;default:'pending'"`
	TimeoutAt      *time.Time     `gorm:"index"`
	CancelOnFail   bool           `gorm:"default:false"`
	CreatedAt      time.Time      `gorm:"autoCreateTime"`
	UpdatedAt      time.Time      `gorm:"autoUpdateTime"`
}
