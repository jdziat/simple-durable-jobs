package fanout

import (
	"fmt"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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
