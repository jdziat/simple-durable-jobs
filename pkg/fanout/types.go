package fanout

import (
	"errors"
	"fmt"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

var (
	// ErrSubJobCancelled marks a result slot whose sub-job was cancelled.
	ErrSubJobCancelled = errors.New("fanout: sub-job cancelled")
	// ErrSubJobIncomplete marks a result slot whose sub-job did not reach a terminal result state.
	ErrSubJobIncomplete = errors.New("fanout: sub-job incomplete")
)

// SubJob represents a sub-job to be spawned.
type SubJob struct {
	Type     string
	Args     any
	Queue    string
	Priority int
	// PrioritySet is true when the sub-job's Priority was set explicitly (via a
	// Priority option), distinguishing an intentional Priority(0) from "unset"
	// so an explicit 0 is NOT overridden by the fan-out default.
	PrioritySet bool
	Retries     int
	// RetriesSet is true when Retries was set explicitly (via a queue.Retries
	// option), distinguishing an intentional Retries(0) from "unset" so an
	// explicit 0 is NOT overridden by the fan-out default. Mirrors PrioritySet.
	RetriesSet bool
	// Determinism is this sub-job's Call replay strictness. No *Set companion:
	// unlike Priority/Retries, its zero value IS the default, so explicit-zero
	// and unset are behaviourally identical.
	Determinism queue.DeterminismMode
	// Delay schedules this sub-job to become runnable after the duration,
	// measured from when FanOut creates the child rows.
	Delay time.Duration
	// RunAt schedules this sub-job at an absolute time. When both RunAt and Delay
	// are set RunAt wins, mirroring queue.Enqueue.
	RunAt *time.Time
	// DedupOptionsIgnored is true when Sub was given an enqueue-deduplication
	// option. Fan-out children carry a fan-out-owned UniqueKey that makes parent
	// replay idempotent, so a caller-supplied dedup key cannot be honoured;
	// FanOut warns rather than silently accepting it.
	DedupOptionsIgnored bool
	// Timeout bounds this sub-job's handler execution.
	Timeout time.Duration
}

// Result wraps a sub-job result with its index and potential error.
type Result[T any] struct {
	Index int   // Position in original subJobs slice
	Value T     // Result if successful
	Err   error // Error if failed
}

// Error contains details about fan-out failures.
type Error struct {
	FanOutID    core.UUID
	TotalCount  int
	FailedCount int
	Strategy    core.FanOutStrategy
	Failures    []SubJobFailure
}

func (e *Error) Error() string {
	return fmt.Sprintf("fan-out failed: %d/%d sub-jobs failed", e.FailedCount, e.TotalCount)
}

// SubJobFailure contains details about a single sub-job failure.
type SubJobFailure struct {
	Index   int
	JobID   core.UUID
	Error   string
	Attempt int
}
