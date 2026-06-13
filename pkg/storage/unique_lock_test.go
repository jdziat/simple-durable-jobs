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
