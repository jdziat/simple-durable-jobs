package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestEnqueueWithUniqueLockDedupesWithinWindowAndRefreshesAfterExpiry(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	first := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":1}`)}
	firstID, err := s.EnqueueWithUniqueLock(ctx, first, scope, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, first.ID, firstID)

	second := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":2}`)}
	secondID, err := s.EnqueueWithUniqueLock(ctx, second, scope, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, first.ID, secondID)

	var jobCount int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id IN ?", []core.UUID{first.ID, second.ID}).
		Count(&jobCount).Error)
	assert.EqualValues(t, 1, jobCount)

	require.NoError(t, s.db.WithContext(ctx).Model(&core.UniqueLock{}).
		Where("scope_hash = ?", scope).
		Update("expires_at", time.Now().Add(-time.Hour).UTC()).Error)

	third := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":3}`)}
	thirdID, err := s.EnqueueWithUniqueLock(ctx, third, scope, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, third.ID, thirdID)

	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id IN ?", []core.UUID{first.ID, second.ID, third.ID}).
		Count(&jobCount).Error)
	assert.EqualValues(t, 2, jobCount)
}

func TestEnqueueWithUniqueLockConcurrentSameKeyCreatesOneJobAndLock(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()

	const concurrency = 32
	const scope = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	start := make(chan struct{})
	ids := make(chan core.UUID, concurrency)
	errs := make(chan error, concurrency)
	var wg sync.WaitGroup

	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := &core.Job{
				ID:    core.NewID(),
				Type:  "unique-lock-race",
				Queue: "default",
				Args:  []byte(`{"race":true}`),
			}
			<-start
			id, err := s.EnqueueWithUniqueLock(ctx, job, scope, time.Hour)
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}(i)
	}

	close(start)
	wg.Wait()
	close(ids)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	seenIDs := map[core.UUID]struct{}{}
	for id := range ids {
		seenIDs[id] = struct{}{}
	}
	require.Len(t, seenIDs, 1, "all duplicate enqueues should return the original job ID")

	var jobCount int64
	require.NoError(t, s.DB().Model(&core.Job{}).
		Where("type = ?", "unique-lock-race").
		Count(&jobCount).Error)
	assert.EqualValues(t, 1, jobCount)

	var lockCount int64
	require.NoError(t, s.DB().Model(&core.UniqueLock{}).
		Where("scope_hash = ?", scope).
		Count(&lockCount).Error)
	assert.EqualValues(t, 1, lockCount)
}

func TestEnqueueWithUniqueLockStealsWindowFromTerminalJob(t *testing.T) {
	for _, terminal := range []core.JobStatus{core.StatusFailed, core.StatusCancelled} {
		t.Run(string(terminal), func(t *testing.T) {
			s := newTestStorage(t)
			ctx := context.Background()
			scope := "1111111111111111111111111111111111111111111111111111111111111111"

			first := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":1}`)}
			firstID, err := s.EnqueueWithUniqueLock(ctx, first, scope, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, first.ID, firstID)

			// Drive the original job to a terminal status while the lock stays LIVE.
			require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
				Where("id = ?", first.ID).
				Update("status", terminal).Error)

			// Re-enqueue the same scope within the window: a terminally dead
			// reference must NOT dedup; a fresh job must be admitted.
			second := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":2}`)}
			secondID, err := s.EnqueueWithUniqueLock(ctx, second, scope, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, second.ID, secondID, "steal must return the new job id, not the dead one")
			assert.NotEqual(t, first.ID, secondID)

			var newJob core.Job
			require.NoError(t, s.db.WithContext(ctx).First(&newJob, "id = ?", second.ID).Error)
			assert.Equal(t, core.StatusPending, newJob.Status)

			// The lock now points at the new job.
			var lock core.UniqueLock
			require.NoError(t, s.db.WithContext(ctx).First(&lock, "scope_hash = ?", scope).Error)
			assert.Equal(t, second.ID, lock.JobID)
		})
	}
}

func TestEnqueueWithUniqueLockStillDedupesLiveJob(t *testing.T) {
	for _, live := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusWaiting, core.StatusCompleted, core.StatusPaused} {
		t.Run(string(live), func(t *testing.T) {
			s := newTestStorage(t)
			ctx := context.Background()
			scope := "2222222222222222222222222222222222222222222222222222222222222222"

			first := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":1}`)}
			firstID, err := s.EnqueueWithUniqueLock(ctx, first, scope, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, first.ID, firstID)

			require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
				Where("id = ?", first.ID).
				Update("status", live).Error)

			second := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":2}`)}
			secondID, err := s.EnqueueWithUniqueLock(ctx, second, scope, time.Hour)
			require.NoError(t, err)
			assert.Equal(t, first.ID, secondID, "a live reference must still dedup")

			var jobCount int64
			require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
				Where("id IN ?", []core.UUID{first.ID, second.ID}).
				Count(&jobCount).Error)
			assert.EqualValues(t, 1, jobCount, "no new job should be created while deduping")
		})
	}
}

func TestEnqueueWithUniqueLockStealsWindowFromMissingJob(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	scope := "3333333333333333333333333333333333333333333333333333333333333333"

	first := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":1}`)}
	firstID, err := s.EnqueueWithUniqueLock(ctx, first, scope, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, first.ID, firstID)

	// Delete the job row directly (as retention would), leaving the live lock
	// dangling at a job that no longer exists.
	require.NoError(t, s.db.WithContext(ctx).Delete(&core.Job{}, "id = ?", first.ID).Error)

	second := &core.Job{ID: core.NewID(), Type: "work", Queue: "default", Args: []byte(`{"n":2}`)}
	secondID, err := s.EnqueueWithUniqueLock(ctx, second, scope, time.Hour)
	require.NoError(t, err)
	assert.Equal(t, second.ID, secondID, "a missing reference must steal the window")

	var newJobCount int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", second.ID).Count(&newJobCount).Error)
	assert.EqualValues(t, 1, newJobCount, "the new job must exist after a steal-on-missing")

	var lock core.UniqueLock
	require.NoError(t, s.db.WithContext(ctx).First(&lock, "scope_hash = ?", scope).Error)
	assert.Equal(t, second.ID, lock.JobID)
}

func TestDeleteExpiredUniqueLocks(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	now := time.Now().UTC()

	require.NoError(t, s.db.WithContext(ctx).Create(&core.UniqueLock{
		ScopeHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		JobID:     testUUID("expired"),
		ExpiresAt: now.Add(-time.Hour),
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.UniqueLock{
		ScopeHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		JobID:     testUUID("live"),
		ExpiresAt: now.Add(time.Hour),
	}).Error)

	deleted, err := s.DeleteExpiredUniqueLocks(ctx, 100)
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted)

	var remaining []core.UniqueLock
	require.NoError(t, s.db.WithContext(ctx).Order("scope_hash ASC").Find(&remaining).Error)
	require.Len(t, remaining, 1)
	assert.Equal(t, testUUID("live"), remaining[0].JobID)
}
