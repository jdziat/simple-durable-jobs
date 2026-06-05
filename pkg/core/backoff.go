package core

import "time"

// BackoffPolicy computes the delay before a failed job is retried.
type BackoffPolicy interface {
	NextRetry(attempt int, err error) time.Duration
}
