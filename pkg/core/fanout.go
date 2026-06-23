package core

import (
	"math"
	"time"
)

// FanOutStrategy defines how sub-job failures affect the parent.
type FanOutStrategy string

const (
	StrategyFailFast   FanOutStrategy = "fail_fast"   // Fail parent on first sub-job failure
	StrategyCollectAll FanOutStrategy = "collect_all" // Wait for all, return partial results
	StrategyThreshold  FanOutStrategy = "threshold"   // Ok if threshold% succeed
)

// AllFanOutStrategies is the single source of truth that the migration CHECK
// and drift guard derive from.
var AllFanOutStrategies = []FanOutStrategy{StrategyFailFast, StrategyCollectAll, StrategyThreshold}

// FanOutStatus represents the state of a fan-out batch.
type FanOutStatus string

const (
	FanOutPending   FanOutStatus = "pending"
	FanOutCompleted FanOutStatus = "completed"
	FanOutFailed    FanOutStatus = "failed"
	// FanOutCancelled is the terminal status for a fan-out whose parent job was
	// terminally cancelled (CancelJobTerminal). It is set directly on the row,
	// never returned by TerminalStatus, and distinguishes an operator cancel
	// from a genuine failure.
	FanOutCancelled FanOutStatus = "cancelled"
)

// AllFanOutStatuses is the single source of truth that the migration CHECK and
// drift guard derive from.
var AllFanOutStatuses = []FanOutStatus{FanOutPending, FanOutCompleted, FanOutFailed, FanOutCancelled}

// FanOut tracks a batch of sub-jobs spawned by a parent job.
type FanOut struct {
	ID             UUID           `gorm:"primaryKey"`
	ParentJobID    UUID           `gorm:"index;index:idx_fan_outs_parent_status,priority:1;not null"`
	TotalCount     int            `gorm:"type:integer;not null"`
	CompletedCount int            `gorm:"type:integer;default:0"`
	FailedCount    int            `gorm:"type:integer;default:0"`
	CancelledCount int            `gorm:"type:integer;default:0"`
	Strategy       FanOutStrategy `gorm:"size:20;default:'fail_fast'"`
	Threshold      float64        `gorm:"type:double precision;default:1"`
	Status         FanOutStatus   `gorm:"size:20;default:'pending';index:idx_fan_outs_parent_status,priority:2"`
	TimeoutAt      *time.Time     `gorm:"index"`
	CancelOnFail   bool           `gorm:"default:false"`
	CreatedAt      time.Time      `gorm:"autoCreateTime"`
	UpdatedAt      time.Time      `gorm:"autoUpdateTime"`
}

// TerminalStatus returns whether the fan-out has reached a terminal state and,
// when done is true, the status that should be persisted.
func (f *FanOut) TerminalStatus() (done bool, status FanOutStatus) {
	accounted := f.CompletedCount + f.FailedCount + f.CancelledCount
	inFlight := f.TotalCount - accounted

	switch f.Strategy {
	case StrategyCollectAll:
		if accounted >= f.TotalCount {
			return true, FanOutCompleted
		}
	case StrategyThreshold:
		required := int(math.Ceil(float64(f.TotalCount) * f.Threshold))
		if f.Threshold <= 0 {
			required = 0
		} else if f.Threshold >= 1 {
			required = f.TotalCount
		}
		if required < 0 {
			required = 0
		}
		if required > f.TotalCount {
			required = f.TotalCount
		}

		if f.CompletedCount+inFlight < required {
			return true, FanOutFailed
		}
		if accounted >= f.TotalCount {
			if f.CompletedCount >= required {
				return true, FanOutCompleted
			}
			return true, FanOutFailed
		}
	default:
		if f.FailedCount > 0 || f.CancelledCount > 0 {
			return true, FanOutFailed
		}
		if accounted >= f.TotalCount {
			return true, FanOutCompleted
		}
	}

	return false, ""
}
