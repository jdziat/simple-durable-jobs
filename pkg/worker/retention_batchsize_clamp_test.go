package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRetentionBatchSize_ClampsToCeiling pins the option's contract. An
// unclamped batch size in the tens of thousands makes every retention pass
// exceed the driver's bind-parameter ceiling, so the sweep deletes nothing at
// all — the exact outcome retention exists to prevent, and worst precisely
// during the backlog an operator raises the batch size for.
func TestRetentionBatchSize_ClampsToCeiling(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int
	}{
		{"typical", 5000, 5000},
		{"at ceiling", maxRetentionBatchSize, maxRetentionBatchSize},
		{"above ceiling", 40000, maxRetentionBatchSize},
		{"absurd", 1 << 30, maxRetentionBatchSize},
		{"one", 1, 1},
		{"unset passes through so the default applies", 0, 0},
		{"negative passes through so the default applies", -5, -5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RetentionConfig{}
			RetentionBatchSize(tc.in)(&cfg)
			require.Equal(t, tc.want, cfg.BatchSize)
		})
	}
}

// TestWithRetention_ClampsBatchSizeEndToEnd proves the clamp survives the
// WithRetention normalization an operator actually goes through, and that a
// non-positive value still resolves to the documented default.
func TestWithRetention_ClampsBatchSizeEndToEnd(t *testing.T) {
	var c WorkerConfig
	WithRetention(RetentionCompletedAfter(defaultRetentionCompletedAfter), RetentionBatchSize(40000)).ApplyWorker(&c)
	require.Equal(t, maxRetentionBatchSize, c.Retention.BatchSize)

	var d WorkerConfig
	WithRetention(RetentionCompletedAfter(defaultRetentionCompletedAfter), RetentionBatchSize(0)).ApplyWorker(&d)
	require.Equal(t, defaultRetentionBatchSize, d.Retention.BatchSize)
}

// TestUniqueLockSweepBatchSize_ClampsToCeiling mirrors the retention clamp on the
// expired-window sweep. That sweep matters more now that a live window pins its
// job row: if it silently stops collecting, unique_locks grows forever.
func TestUniqueLockSweepBatchSize_ClampsToCeiling(t *testing.T) {
	var cfg UniqueLockSweepConfig
	UniqueLockSweepBatchSize(40000)(&cfg)
	require.Equal(t, maxRetentionBatchSize, cfg.BatchSize)

	UniqueLockSweepBatchSize(500)(&cfg)
	require.Equal(t, 500, cfg.BatchSize)
}
