package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestReleaseRateAt_RefundsExactConsumeWindowAcrossRollover guards E6: a refund
// must return the unit to the EXACT window the consume incremented, even when the
// clock has rolled into a later window between consume and refund.
//
// The old now-relative ReleaseRate recomputes now.Truncate(window) at refund time,
// so a unit consumed in window W0 but refunded once the clock is in W1 decrements
// W1 (a window this job never consumed) and permanently strands W0's counter.
// ReleaseRateAt targets the committed (limit_name, window_start) row instead.
//
// Deterministic: consume windows are pinned via explicit `now` (no wall-clock or
// boundary flake), anchored in the past so "now" is provably a different window.
func TestReleaseRateAt_RefundsExactConsumeWindowAcrossRollover(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	const name = "e6.rollover"
	window := time.Second

	// Two adjacent windows, both firmly in the past so a now-relative refund lands
	// on neither.
	base := time.Now().UTC().Truncate(window).Add(-time.Hour)

	allowed0, ws0, err := s.TryConsumeRateWindow(ctx, name, 10, window, base)
	require.NoError(t, err)
	require.True(t, allowed0, "consume in W0 must be allowed (ceiling 10)")

	allowed1, ws1, err := s.TryConsumeRateWindow(ctx, name, 10, window, base.Add(window))
	require.NoError(t, err)
	require.True(t, allowed1, "consume in W1 must be allowed")
	require.False(t, ws0.Equal(ws1), "the two consumes must land in distinct windows")

	require.Equal(t, int64(1), rateWindowCount(t, ctx, s, name, ws0), "W0 starts at 1")
	require.Equal(t, int64(1), rateWindowCount(t, ctx, s, name, ws1), "W1 starts at 1")

	// Contrast — the now-relative refund cannot reach the past consume window: it
	// decrements "now"'s window (which has no row / count 0), leaving W0 drained.
	require.NoError(t, s.ReleaseRate(ctx, name, window))
	require.Equal(t, int64(1), rateWindowCount(t, ctx, s, name, ws0),
		"ReleaseRate (now-relative) must NOT have refunded the past consume window W0")

	// The fix — ReleaseRateAt refunds the exact committed window W0, W1 untouched.
	require.NoError(t, s.ReleaseRateAt(ctx, name, ws0))
	require.Equal(t, int64(0), rateWindowCount(t, ctx, s, name, ws0), "ReleaseRateAt must refund the consume window W0")
	require.Equal(t, int64(1), rateWindowCount(t, ctx, s, name, ws1), "W1 must be untouched")

	// Guard: a refund never drives a counter negative.
	require.NoError(t, s.ReleaseRateAt(ctx, name, ws0))
	require.Equal(t, int64(0), rateWindowCount(t, ctx, s, name, ws0), "count>0 guard keeps W0 at 0, never negative")
}

func rateWindowCount(t *testing.T, ctx context.Context, s *GormStorage, name string, windowStart time.Time) int64 {
	t.Helper()
	var row core.RateLimitWindow
	err := s.db.WithContext(ctx).
		Where("limit_name = ? AND window_start = ?", name, windowStart).
		First(&row).Error
	require.NoError(t, err, "window row must exist")
	return int64(row.Count)
}
