package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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
