package fanout

import (
	"errors"
	"fmt"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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
	Retries  int
	Timeout  time.Duration
}

// Result wraps a sub-job result with its index and potential error.
type Result[T any] struct {
	Index int   // Position in original subJobs slice
	Value T     // Result if successful
	Err   error // Error if failed
}

// Error contains details about fan-out failures.
type Error struct {
	FanOutID    string
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
	JobID   string
	Error   string
	Attempt int
}
