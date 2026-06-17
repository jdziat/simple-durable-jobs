package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTryConsumeRateLimits_RefundsEarlierLimitOnLaterDenial is the regression test
// for teardown g4 (HIGH): with two fleet rate limits, a denial by the LATER limit
// left the EARLIER limit's already-consumed unit committed with no refund,
// permanently draining it within the window for a job that never runs. The earlier
// limit must now be refunded.
//
// A uses a whole-number rate (1s window, ceiling 100 — never denies; its consume
// and refund in the same call net to zero). B uses a fractional rate so its derived
// window is long (100s, ceiling 1), making "denies on the second call" deterministic
// without a 1s window-boundary flake.
func TestTryConsumeRateLimits_RefundsEarlierLimitOnLaterDenial(t *testing.T) {
	db := newInMemoryDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)
	w := NewWorker(q, DisableRetry(), RateLimit("A", 100), RateLimit("B", 0.01))
	ctx := context.Background()
	job := &core.Job{ID: core.NewID(), Type: "t", Queue: "default"}

	// Call 1: A and B both admit.
	ok, _ := w.tryConsumeRateLimits(ctx, job)
	require.True(t, ok)
	// Call 2: B is exhausted and denies; A's consume from THIS call must be refunded.
	ok2, reason := w.tryConsumeRateLimits(ctx, job)
	require.False(t, ok2)
	require.Equal(t, bounceFleetRate, reason)

	// A's total consumed count must be 1 (only call 1). A leaked count of 2 means
	// the later denial did not refund the earlier limit.
	var total int64
	require.NoError(t, db.Model(&core.RateLimitWindow{}).
		Where("limit_name = ?", "A").
		Select("COALESCE(SUM(count),0)").Scan(&total).Error)
	assert.Equal(t, int64(1), total,
		"limit A must be refunded when later limit B denies (g4); a leaked count means no refund")
}

// TestResolveRateLimitWindow covers the fractional-rate window derivation (g4).
func TestResolveRateLimitWindow(t *testing.T) {
	w := &Worker{}
	assert.Equal(t, 10*time.Second, w.resolveRateLimitWindow(RateLimitConfig{PerSecond: 0.1}),
		"0.1/sec with no explicit window -> 10s so the per-window ceiling is exact")
	assert.Equal(t, 2*time.Second, w.resolveRateLimitWindow(RateLimitConfig{PerSecond: 0.5}),
		"0.5/sec -> 2s")
	assert.Equal(t, defaultRateLimitWindow, w.resolveRateLimitWindow(RateLimitConfig{PerSecond: 5}),
		"a whole-number rate keeps the 1s default")
	assert.Equal(t, 30*time.Second, w.resolveRateLimitWindow(RateLimitConfig{PerSecond: 0.1, Window: 30 * time.Second}),
		"an explicit window is always honored")
}

// TestBoundRateLimitName covers the over-long-key bounding (g4).
func TestBoundRateLimitName(t *testing.T) {
	short := "limit:tenant-A"
	assert.Equal(t, short, boundRateLimitName(short), "short names pass through unchanged")

	long := "limit:" + strings.Repeat("x", 300)
	bounded := boundRateLimitName(long)
	assert.LessOrEqual(t, len(bounded), maxRateLimitNameLen, "an over-long name must be bounded to the column width")
	assert.Equal(t, bounded, boundRateLimitName(long), "bounding must be stable for the same input")
	assert.NotEqual(t, boundRateLimitName(long), boundRateLimitName(long+"y"),
		"distinct over-long keys must bucket distinctly")
}
