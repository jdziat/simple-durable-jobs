package fanout

import (
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Sub()
// ---------------------------------------------------------------------------

func TestSub_SetsTypeAndArgs(t *testing.T) {
	sj := Sub("process-item", 42)
	assert.Equal(t, "process-item", sj.Type)
	assert.Equal(t, 42, sj.Args)
}

func TestSub_DefaultRetries(t *testing.T) {
	sj := Sub("job", "args")
	assert.Equal(t, queue.DefaultJobRetries, sj.Retries)
}

func TestSub_WithQueueOption(t *testing.T) {
	sj := Sub("job", "args", queue.QueueOpt("high-priority"))
	assert.Equal(t, "high-priority", sj.Queue)
}

func TestSub_WithPriorityOption(t *testing.T) {
	sj := Sub("job", "args", queue.Priority(10))
	assert.Equal(t, 10, sj.Priority)
}

func TestSub_WithRetriesOption(t *testing.T) {
	sj := Sub("job", "args", queue.Retries(7))
	assert.Equal(t, 7, sj.Retries)
}

func TestSub_WithAllOptions(t *testing.T) {
	sj := Sub("job", "args",
		queue.QueueOpt("batch"),
		queue.Priority(5),
		queue.Retries(4),
	)
	assert.Equal(t, "batch", sj.Queue)
	assert.Equal(t, 5, sj.Priority)
	assert.Equal(t, 4, sj.Retries)
}

func TestSub_NoOptions_QueueEmpty(t *testing.T) {
	sj := Sub("job", nil)
	assert.Empty(t, sj.Queue)
	assert.Equal(t, 0, sj.Priority)
}

func TestSub_ZeroPriorityOptionIgnored(t *testing.T) {
	// queue.Priority(0) leaves Priority at 0 (not overwritten to non-zero)
	sj := Sub("job", "args", queue.Priority(0))
	assert.Equal(t, 0, sj.Priority)
}

func TestSub_ZeroRetriesOptionUsesDefault(t *testing.T) {
	// When Retries option is 0, Sub falls back to DefaultJobRetries.
	sj := Sub("job", "args", queue.Retries(0))
	assert.Equal(t, queue.DefaultJobRetries, sj.Retries)
}

// ---------------------------------------------------------------------------
// Result[T]
// ---------------------------------------------------------------------------

func TestResult_SuccessHasNoError(t *testing.T) {
	r := Result[int]{Index: 0, Value: 42, Err: nil}
	assert.Nil(t, r.Err)
	assert.Equal(t, 42, r.Value)
}

func TestResult_FailureCarriesError(t *testing.T) {
	sentinel := errors.New("sub-job failed")
	r := Result[string]{Index: 1, Err: sentinel}
	assert.ErrorIs(t, r.Err, sentinel)
	assert.Empty(t, r.Value)
}

// ---------------------------------------------------------------------------
// Error (fanout.Error type)
// ---------------------------------------------------------------------------

func TestFanOutError_ErrorMessage(t *testing.T) {
	e := &Error{
		FanOutID:    "fo-1",
		TotalCount:  10,
		FailedCount: 3,
		Strategy:    core.StrategyFailFast,
	}
	msg := e.Error()
	assert.Contains(t, msg, "3")
	assert.Contains(t, msg, "10")
	assert.Contains(t, msg, "fan-out failed")
}

func TestFanOutError_WithFailures(t *testing.T) {
	e := &Error{
		FanOutID:    "fo-2",
		TotalCount:  5,
		FailedCount: 2,
		Failures: []SubJobFailure{
			{Index: 0, JobID: "j-1", Error: "timeout", Attempt: 3},
			{Index: 4, JobID: "j-5", Error: "bad input", Attempt: 1},
		},
	}
	require.Len(t, e.Failures, 2)
	assert.Equal(t, "timeout", e.Failures[0].Error)
	assert.Equal(t, "bad input", e.Failures[1].Error)
}

func TestFanOutError_ImplementsErrorInterface(t *testing.T) {
	var err error = &Error{TotalCount: 1, FailedCount: 1}
	assert.NotNil(t, err)
}

// ---------------------------------------------------------------------------
// WaitingError
// ---------------------------------------------------------------------------

func TestWaitingError_ErrorMessage(t *testing.T) {
	e := &WaitingError{FanOutID: "fo-abc"}
	msg := e.Error()
	assert.Contains(t, msg, "fo-abc")
	assert.Contains(t, msg, "waiting")
}

func TestWaitingError_EmptyFanOutID(t *testing.T) {
	e := &WaitingError{}
	msg := e.Error()
	assert.NotEmpty(t, msg)
}

// ---------------------------------------------------------------------------
// IsWaitingError
// ---------------------------------------------------------------------------

func TestIsWaitingError_TrueForWaitingError(t *testing.T) {
	err := &WaitingError{FanOutID: "fo-1"}
	assert.True(t, IsWaitingError(err))
}

func TestIsWaitingError_FalseForRegularError(t *testing.T) {
	err := errors.New("not a waiting signal")
	assert.False(t, IsWaitingError(err))
}

func TestIsWaitingError_FalseForNil(t *testing.T) {
	assert.False(t, IsWaitingError(nil))
}

func TestIsWaitingError_FalseForFanOutError(t *testing.T) {
	err := &Error{TotalCount: 2, FailedCount: 1}
	assert.False(t, IsWaitingError(err))
}

// TestSuspendErrorAliasContract pins the additive-deprecation contract:
// SuspendError and WaitingError must be the same type so downstream code
// using either literal keeps working, and IsSuspendError / IsWaitingError
// must each recognize values built with the other name.
func TestSuspendErrorAliasContract(t *testing.T) {
	// Constructing with the deprecated name is caught by the new detector.
	//nolint:staticcheck // intentionally exercising the deprecated alias
	viaOld := &SuspendError{FanOutID: "fo-old"}
	assert.True(t, IsWaitingError(viaOld),
		"IsWaitingError must recognize values built with the deprecated name")

	// Constructing with the new name is caught by the deprecated detector.
	viaNew := &WaitingError{FanOutID: "fo-new"}
	//nolint:staticcheck // intentionally exercising the deprecated alias
	assert.True(t, IsSuspendError(viaNew),
		"IsSuspendError must keep working for callers that have not migrated")
}
