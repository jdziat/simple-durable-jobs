package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
)

func TestUniqueLockSweepGate(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0))

	assert.True(t, w.config.UniqueLockSweep.enabled())
	w.config.UniqueLockSweep.Disabled = true
	assert.False(t, w.config.UniqueLockSweep.enabled())
}

func TestWithUniqueLockSweepPopulatesConfig(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithUniqueLockSweep(
			UniqueLockSweepInterval(2*time.Hour),
			UniqueLockSweepBatchSize(123),
		),
		WithOwnershipAuditInterval(0),
	)

	assert.False(t, w.config.UniqueLockSweep.Disabled)
	assert.Equal(t, 2*time.Hour, w.config.UniqueLockSweep.Interval)
	assert.Equal(t, 123, w.config.UniqueLockSweep.BatchSize)

	w = NewWorker(q,
		WithUniqueLockSweep(
			UniqueLockSweepInterval(time.Nanosecond),
			UniqueLockSweepBatchSize(0),
		),
		WithOwnershipAuditInterval(0),
	)
	assert.Equal(t, minUniqueLockSweepInterval, w.config.UniqueLockSweep.Interval)
	assert.Equal(t, defaultUniqueLockSweepBatch, w.config.UniqueLockSweep.BatchSize)

	w = NewWorker(q,
		WithUniqueLockSweep(
			UniqueLockSweepInterval(2*time.Hour),
			UniqueLockSweepBatchSize(123),
			UniqueLockSweepDisabled(),
		),
		WithOwnershipAuditInterval(0),
	)
	assert.True(t, w.config.UniqueLockSweep.Disabled)
	assert.Equal(t, 2*time.Hour, w.config.UniqueLockSweep.Interval)
	assert.Equal(t, 123, w.config.UniqueLockSweep.BatchSize)
}

func TestWorkerUniqueLockSweepDeletesExpiredLocks(t *testing.T) {
	db, store := newWorkerRetentionStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	require.NoError(t, db.Create(&core.UniqueLock{
		ScopeHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		JobID:     "expired",
		ExpiresAt: now.Add(-time.Hour),
	}).Error)
	require.NoError(t, db.Create(&core.UniqueLock{
		ScopeHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		JobID:     "live",
		ExpiresAt: now.Add(time.Hour),
	}).Error)

	q := queue.New(store)
	w := NewWorker(q,
		WorkerQueue("unique-lock-empty", Concurrency(1)),
		WithOwnershipAuditInterval(0),
	)
	w.config.UniqueLockSweep.Interval = 10 * time.Millisecond
	w.config.UniqueLockSweep.BatchSize = 1

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Start(runCtx) }()
	require.Eventually(t, func() bool {
		return !workerUniqueLockExists(t, db, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	}, 5*time.Second, 50*time.Millisecond)

	assert.True(t, workerUniqueLockExists(t, db, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"))

	cancel()
	require.Eventually(t, func() bool {
		select {
		case err := <-done:
			return assert.ErrorIs(t, err, context.Canceled)
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond)
}

func workerUniqueLockExists(t *testing.T, db *gorm.DB, scopeHash string) bool {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(&core.UniqueLock{}).Where("scope_hash = ?", scopeHash).Count(&count).Error)
	return count == 1
}
