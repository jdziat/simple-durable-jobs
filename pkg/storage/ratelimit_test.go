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

func uniqueRateLimitName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestRateLimitWindowCeilingNamesAndExpiry(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	window := time.Second
	now := time.Unix(1_000, 0).UTC()
	name := uniqueRateLimitName(t)

	for i := 0; i < 3; i++ {
		ok, err := s.TryConsumeRate(ctx, name, 2.1, window, now)
		require.NoError(t, err)
		assert.True(t, ok)
	}
	ok, err := s.TryConsumeRate(ctx, name, 2.1, window, now)
	require.NoError(t, err)
	assert.False(t, ok, "ceil(2.1/sec * 1s) allows at most 3 consumptions")

	other, err := s.TryConsumeRate(ctx, name+":tenant-b", 2.1, window, now)
	require.NoError(t, err)
	assert.True(t, other, "distinct names/keys are independent")

	fresh, err := s.TryConsumeRate(ctx, name, 2.1, window, now.Add(window))
	require.NoError(t, err)
	assert.True(t, fresh, "a fresh window admits again")
}

func TestRateLimitConcurrentBurstDoesNotOverAdmit(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	name := uniqueRateLimitName(t)
	window := time.Second
	now := time.Unix(2_000, 0).UTC()
	const limit = 4
	const contenders = 16

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan bool, contenders)
	errs := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := s.TryConsumeRate(ctx, name, limit, window, now)
			if err != nil {
				errs <- err
				return
			}
			results <- ok
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	admitted := 0
	for ok := range results {
		if ok {
			admitted++
		}
	}
	assert.Equal(t, limit, admitted)
}

func TestRateLimitWindowCleanupPurgesOldWindows(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	name := uniqueRateLimitName(t)
	window := time.Second
	now := time.Unix(3_000, 0).UTC()
	currentWindow := now.Truncate(window)
	oldWindow := currentWindow.Add(-5 * window)

	require.NoError(t, s.db.WithContext(ctx).Create(&core.RateLimitWindow{
		LimitName:   name,
		WindowStart: oldWindow,
		Count:       1,
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.RateLimitWindow{
		LimitName:   name,
		WindowStart: currentWindow,
		Count:       1,
	}).Error)

	ok, err := s.TryConsumeRate(ctx, name, 10, window, now)
	require.NoError(t, err)
	assert.True(t, ok)

	var oldCount int64
	require.NoError(t, s.db.WithContext(ctx).Model(&core.RateLimitWindow{}).
		Where("limit_name = ? AND window_start = ?", name, oldWindow).
		Count(&oldCount).Error)
	assert.Zero(t, oldCount, "expired windows should be garbage-collected")

	var current core.RateLimitWindow
	require.NoError(t, s.db.WithContext(ctx).First(&current,
		"limit_name = ? AND window_start = ?", name, currentWindow).Error)
	assert.Equal(t, 2, current.Count, "current window should be preserved and incremented")
}

func TestRateLimitInvalidInputsAreDenied(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	now := time.Now()

	ok, err := s.TryConsumeRate(ctx, "", 1, time.Second, now)
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = s.TryConsumeRate(ctx, "x", 0, time.Second, now)
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = s.TryConsumeRate(ctx, "x", 1, 0, now)
	require.NoError(t, err)
	assert.False(t, ok)
}
