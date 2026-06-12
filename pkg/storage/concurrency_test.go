package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

func uniqueSlotName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestConcurrencySlotLimitReleaseAndExpiry(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, "job-1", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, "job-2", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, "job-3", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.False(t, ok, "third live slot must be denied at limit 2")

	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, "job-1", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "re-acquiring the same job renews instead of double-counting")

	require.NoError(t, s.ReleaseConcurrencySlot(ctx, slot, "job-2"))
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, "job-3", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "released slot should free capacity")

	expiredSlot := slot + "-expired"
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  expiredSlot,
		JobID:     "expired-job",
		WorkerID:  "worker-1",
		ExpiresAt: time.Now().Add(-time.Hour),
	}).Error)
	ok, err = s.TryAcquireConcurrencySlot(ctx, expiredSlot, "fresh-job", "worker-2", 1, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "expired slots must not count against the cap")
}

func TestConcurrencySlotConcurrentLastSlotRace(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, "job-existing", "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			acquired, acquireErr := s.TryAcquireConcurrencySlot(
				ctx,
				slot,
				fmt.Sprintf("job-racer-%d", i),
				fmt.Sprintf("worker-%d", i),
				2,
				time.Hour,
			)
			if acquireErr != nil {
				errs <- acquireErr
				return
			}
			results <- acquired
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	winners := 0
	for acquired := range results {
		if acquired {
			winners++
		}
	}
	assert.Equal(t, 1, winners, "exactly one racer should acquire the final slot")
}

func TestDeleteExpiredConcurrencySlotsPreservesLiveAndSentinel(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)
	now := time.Now().UTC()

	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     "",
		WorkerID:  "",
		ExpiresAt: time.Unix(0, 0).UTC(),
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     "expired-job",
		WorkerID:  "worker-1",
		ExpiresAt: now.Add(-time.Hour),
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     "live-job",
		WorkerID:  "worker-1",
		ExpiresAt: now.Add(time.Hour),
	}).Error)

	deleted, err := s.DeleteExpiredConcurrencySlots(ctx, now)
	require.NoError(t, err)
	// DeleteExpiredConcurrencySlots is a global GC sweep; on a shared external
	// test DB it may also reap other tests' expired slots, so assert it removed
	// at least our expired row. The slot-scoped remaining check below is the
	// precise correctness assertion (sentinel + live row preserved).
	assert.GreaterOrEqual(t, deleted, int64(1))

	var remaining []core.ConcurrencySlot
	require.NoError(t, s.db.WithContext(ctx).
		Where("slot_name = ?", slot).
		Order("job_id ASC").
		Find(&remaining).Error)
	require.Len(t, remaining, 2)
	assert.Equal(t, "", remaining[0].JobID, "sentinel row must not be deleted")
	assert.Equal(t, "live-job", remaining[1].JobID, "live held slot must not be deleted")
}
