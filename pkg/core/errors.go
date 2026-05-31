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
	ErrJobNotCompleted    = errors.New("jobs: job has not completed")
	ErrNoResult           = errors.New("jobs: completed job has no result")
)

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

// CheckpointErrorKind returns the durable error discriminator for supported error types.
func CheckpointErrorKind(err error) (kind string, delay time.Duration) {
	var noRetry *NoRetryError
	if errors.As(err, &noRetry) {
		return CheckpointErrorKindNoRetry, 0
	}

	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		return CheckpointErrorKindRetryAfter, retryAfter.Delay
	}

	if candidate := SentinelErrorByMessage(err.Error()); candidate != nil && errors.Is(err, candidate) {
		return CheckpointErrorKindSentinel, 0
	}

	return "", 0
}

// RehydrateCheckpointError reconstructs supported checkpointed error types.
func RehydrateCheckpointError(message, kind string, delay time.Duration) error {
	switch kind {
	case CheckpointErrorKindSentinel:
		if sentinel := SentinelErrorByMessage(message); sentinel != nil {
			return sentinel
		}
		return errors.New(message)
	case CheckpointErrorKindNoRetry:
		causeMessage := strings.TrimPrefix(message, "no retry: ")
		return NoRetry(errors.New(causeMessage))
	case CheckpointErrorKindRetryAfter:
		causeMessage := strings.TrimPrefix(message, fmt.Sprintf("retry after %v: ", delay))
		return RetryAfter(delay, errors.New(causeMessage))
	default:
		return errors.New(message)
	}
}

// SentinelErrorByMessage returns a known sentinel error with the same stored message.
func SentinelErrorByMessage(message string) error {
	for _, err := range []error{
		ErrInvalidJobTypeName,
		ErrJobTypeNameTooLong,
		ErrInvalidQueueName,
		ErrQueueNameTooLong,
		ErrJobArgsTooLarge,
		ErrJobNotOwned,
		ErrDuplicateJob,
		ErrUniqueKeyTooLong,
		ErrJobAlreadyPaused,
		ErrJobNotPaused,
		ErrQueueAlreadyPaused,
		ErrQueueNotPaused,
		ErrCannotPauseStatus,
		ErrJobNotCompleted,
		ErrNoResult,
	} {
		if err.Error() == message {
			return err
		}
	}
	return nil
}
