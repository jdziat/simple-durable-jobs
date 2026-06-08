package core

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Validation errors
var (
	ErrInvalidJobTypeName = errors.New("jobs: invalid job type name (must be alphanumeric, start with letter)")
	ErrJobTypeNameTooLong = errors.New("jobs: job type name too long")
	ErrInvalidQueueName   = errors.New("jobs: invalid queue name")
	ErrQueueNameTooLong   = errors.New("jobs: queue name too long")
	ErrJobArgsTooLarge    = errors.New("jobs: job arguments exceed size limit")
	ErrJobNotOwned        = errors.New("jobs: job not owned by this worker")
	ErrDuplicateJob       = errors.New("jobs: duplicate job with same unique key")
	ErrUniqueKeyTooLong   = errors.New("jobs: unique key exceeds maximum length")
	ErrJobAlreadyPaused   = errors.New("jobs: job is already paused")
	ErrJobNotPaused       = errors.New("jobs: job is not paused")
	ErrQueueAlreadyPaused = errors.New("jobs: queue is already paused")
	ErrQueueNotPaused     = errors.New("jobs: queue is not paused")
	ErrCannotPauseStatus  = errors.New("jobs: cannot pause job in current status")
	ErrJobNotFound        = errors.New("jobs: job not found")
	ErrJobNotCompleted    = errors.New("jobs: job has not completed")
	ErrJobFailed          = errors.New("jobs: job failed")
	ErrJobCancelled       = errors.New("jobs: job was cancelled")
	ErrNoResult           = errors.New("jobs: completed job has no result")
	// ErrCannotRequeueSubJob is returned by Requeue for a fan-out sub-job:
	// requeuing it directly would double-count its parent's fan-out. Requeue the
	// parent instead, which re-dispatches the whole batch.
	ErrCannotRequeueSubJob = errors.New("jobs: cannot requeue a fan-out sub-job directly; requeue its parent")
	// ErrSignalNameTooLong is returned when a signal name exceeds the limit.
	ErrSignalNameTooLong = errors.New("jobs: signal name too long")
	// ErrStorageNoSignals is returned when the storage backend does not
	// implement the signal capability.
	ErrStorageNoSignals = errors.New("jobs: storage backend does not support signals")
	// ErrStorageNoTxEnqueue is returned when the storage backend does not
	// implement caller-supplied transaction enqueue.
	ErrStorageNoTxEnqueue = errors.New("jobs: storage backend does not support transactional enqueue")
	// ErrStorageNoTxCheckpoint is returned when the storage backend does not
	// implement caller-supplied transaction checkpoints.
	ErrStorageNoTxCheckpoint = errors.New("jobs: storage backend does not support transactional checkpoints")
	// ErrStorageNoBatchDequeue is returned when the storage backend does not
	// implement batch dequeue.
	ErrStorageNoBatchDequeue = errors.New("jobs: storage backend does not support batch dequeue")
	// ErrPayloadDecode wraps codec failures while reading stored payload bytes.
	ErrPayloadDecode = errors.New("jobs: payload decode failed")
)

// WorkflowWaiter is implemented by control-flow errors that tell the worker a
// job has suspended itself into StatusWaiting (fan-out or signal waits) and the
// outcome must NOT be treated as a failure — the job is already persisted as
// waiting and will be resumed later. See IsWaiting.
type WorkflowWaiter interface {
	WorkflowWaiting() bool
}

// IsWaiting reports whether err is a self-suspension signal (the handler moved
// its job to StatusWaiting and returned). The worker treats such an error as
// "stop, do not fail" rather than a job failure.
func IsWaiting(err error) bool {
	var w WorkflowWaiter
	return errors.As(err, &w) && w.WorkflowWaiting()
}

// NoRetryError indicates an error that should not be retried.
type NoRetryError struct {
	Err error
}

func (e *NoRetryError) Error() string {
	return fmt.Sprintf("no retry: %v", e.Err)
}

func (e *NoRetryError) Unwrap() error {
	return e.Err
}

// NoRetry wraps an error to indicate it should not be retried.
func NoRetry(err error) error {
	return &NoRetryError{Err: err}
}

// RetryAfterError indicates an error that should be retried after a delay.
type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("retry after %v: %v", e.Delay, e.Err)
}

func (e *RetryAfterError) Unwrap() error {
	return e.Err
}

// RetryAfter wraps an error to indicate it should be retried after a delay.
func RetryAfter(d time.Duration, err error) error {
	return &RetryAfterError{Err: err, Delay: d}
}

const (
	CheckpointErrorKindNoRetry    = "no_retry"
	CheckpointErrorKindRetryAfter = "retry_after"
	CheckpointErrorKindSentinel   = "sentinel"
)

// sentinelsByKey maps a stable, durable key to each sentinel error. The key —
// not the message — is what gets persisted for sentinel checkpoints, so a
// future change to a sentinel's wording cannot mis-resolve an old checkpoint,
// and two sentinels could never collide on identical text. Keep the keys
// stable forever; they are written to the database.
var sentinelsByKey = map[string]error{
	"ErrInvalidJobTypeName": ErrInvalidJobTypeName,
	"ErrJobTypeNameTooLong": ErrJobTypeNameTooLong,
	"ErrInvalidQueueName":   ErrInvalidQueueName,
	"ErrQueueNameTooLong":   ErrQueueNameTooLong,
	"ErrJobArgsTooLarge":    ErrJobArgsTooLarge,
	"ErrJobNotOwned":        ErrJobNotOwned,
	"ErrDuplicateJob":       ErrDuplicateJob,
	"ErrUniqueKeyTooLong":   ErrUniqueKeyTooLong,
	"ErrJobAlreadyPaused":   ErrJobAlreadyPaused,
	"ErrJobNotPaused":       ErrJobNotPaused,
	"ErrQueueAlreadyPaused": ErrQueueAlreadyPaused,
	"ErrQueueNotPaused":     ErrQueueNotPaused,
	"ErrCannotPauseStatus":  ErrCannotPauseStatus,
	"ErrJobNotFound":        ErrJobNotFound,
	"ErrJobNotCompleted":    ErrJobNotCompleted,
	"ErrJobFailed":          ErrJobFailed,
	"ErrJobCancelled":       ErrJobCancelled,
	"ErrNoResult":           ErrNoResult,
}

// sentinelKey returns the stable key for err if it is (or wraps) a known
// sentinel. Each sentinel is independent, so at most one matches.
func sentinelKey(err error) (string, bool) {
	for key, sentinel := range sentinelsByKey {
		if errors.Is(err, sentinel) {
			return key, true
		}
	}
	return "", false
}

// CheckpointErrorKind returns the durable error discriminator for supported error types.
func CheckpointErrorKind(err error) (kind string, delay time.Duration) {
	_, _, kind, delay = CheckpointErrorFields(err)
	return kind, delay
}

// CheckpointErrorFields returns the durable fields for persisting err in a
// checkpoint: the full message (for display/logging), the reconstruction cause
// (inner cause message for no_retry/retry_after, or the stable sentinel key for
// sentinel errors), the kind discriminator, and any retry delay. Storing cause
// explicitly is what lets RehydrateCheckpointError rebuild the original error
// without parsing the formatted message prefix.
func CheckpointErrorFields(err error) (message, cause, kind string, delay time.Duration) {
	if err == nil {
		return "", "", "", 0
	}
	message = err.Error()

	var noRetry *NoRetryError
	if errors.As(err, &noRetry) {
		if noRetry.Err != nil {
			cause = noRetry.Err.Error()
		}
		return message, cause, CheckpointErrorKindNoRetry, 0
	}

	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		if retryAfter.Err != nil {
			cause = retryAfter.Err.Error()
		}
		return message, cause, CheckpointErrorKindRetryAfter, retryAfter.Delay
	}

	if key, ok := sentinelKey(err); ok {
		return message, key, CheckpointErrorKindSentinel, 0
	}

	return message, "", "", 0
}

// RehydrateCheckpointError reconstructs a checkpointed error from the legacy
// (message, kind, delay) fields by parsing the message prefix. Retained for
// backward compatibility and for checkpoints written before the cause column
// existed; new code should call RehydrateCheckpointErrorWithCause.
func RehydrateCheckpointError(message, kind string, delay time.Duration) error {
	return RehydrateCheckpointErrorWithCause(message, "", kind, delay)
}

// RehydrateCheckpointErrorWithCause reconstructs a checkpointed error using the
// explicit cause when present, falling back to message parsing for checkpoints
// written before the cause column existed (cause == "").
func RehydrateCheckpointErrorWithCause(message, cause, kind string, delay time.Duration) error {
	switch kind {
	case CheckpointErrorKindSentinel:
		// Prefer the stable key written by newer checkpoints; fall back to
		// matching the message for older ones.
		if cause != "" {
			if sentinel, ok := sentinelsByKey[cause]; ok {
				return sentinel
			}
		}
		if sentinel := SentinelErrorByMessage(message); sentinel != nil {
			return sentinel
		}
		return errors.New(message)
	case CheckpointErrorKindNoRetry:
		causeMessage := cause
		if causeMessage == "" {
			causeMessage = strings.TrimPrefix(message, "no retry: ")
		}
		return NoRetry(errors.New(causeMessage))
	case CheckpointErrorKindRetryAfter:
		causeMessage := cause
		if causeMessage == "" {
			causeMessage = strings.TrimPrefix(message, fmt.Sprintf("retry after %v: ", delay))
		}
		return RetryAfter(delay, errors.New(causeMessage))
	default:
		return errors.New(message)
	}
}

// SentinelErrorByMessage returns a known sentinel error with the same stored message.
func SentinelErrorByMessage(message string) error {
	for _, err := range sentinelsByKey {
		if err.Error() == message {
			return err
		}
	}
	return nil
}
