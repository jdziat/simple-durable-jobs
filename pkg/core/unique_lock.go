package core

import (
	"context"
	"time"
)

// UniqueLock records a time-bounded enqueue idempotency scope.
//
// ScopeHash is a SHA-256 hex digest over canonical plaintext enqueue inputs,
// not over stored payload bytes. JobID is the original job admitted for that
// scope while ExpiresAt is live.
type UniqueLock struct {
	ScopeHash string    `gorm:"primaryKey;size:64"`
	JobID     UUID      `gorm:"not null"`
	ExpiresAt time.Time `gorm:"not null;index:idx_unique_locks_expires_at"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// UniqueLockEnqueuer is an optional storage capability for atomic windowed
// enqueue deduplication. Implementations insert the job and its unique lock in
// the same transaction, returning the existing job ID when a live lock already
// exists.
type UniqueLockEnqueuer interface {
	EnqueueWithUniqueLock(ctx context.Context, job *Job, scopeHash string, ttl time.Duration) (UUID, error)
}

// UniqueLockSweeper is an optional storage capability for pruning expired
// windowed enqueue deduplication locks.
type UniqueLockSweeper interface {
	DeleteExpiredUniqueLocks(ctx context.Context, limit int) (int64, error)
}
