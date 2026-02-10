package core

import (
	"errors"
	"fmt"
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
