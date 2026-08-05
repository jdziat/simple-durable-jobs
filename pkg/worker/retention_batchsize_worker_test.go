package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// retentionLimitRecorder is a storage that records the limit the retention loop
// asks it for and otherwise does nothing.
type retentionLimitRecorder struct {
	*mockStorage
	mu     sync.Mutex
	limits []int
}

func (r *retentionLimitRecorder) DeleteTerminalJobsOlderThan(_ context.Context, _ core.JobStatus, _ time.Duration, limit int) (int64, error) {
	r.mu.Lock()
	r.limits = append(r.limits, limit)
	r.mu.Unlock()
	return 0, nil
}

func (r *retentionLimitRecorder) seen() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.limits...)
}

var _ retentionStorage = (*retentionLimitRecorder)(nil)

// TestRunRetention_ClampsDirectlyAssignedBatchSize pins the second clamp site.
// WorkerConfig.Retention is an exported struct and the existing retention tests
// themselves assign BatchSize directly (retention_test.go sets
// w.config.Retention.BatchSize = 1), which bypasses the RetentionBatchSize option
// entirely — so the option-level clamp alone would leave that door open.
func TestRunRetention_ClampsDirectlyAssignedBatchSize(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  int
		want int
	}{
		{"above ceiling is clamped", 40000, maxRetentionBatchSize},
		{"unset falls back to the default", 0, defaultRetentionBatchSize},
		{"in-range value is passed through", 250, 250},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &retentionLimitRecorder{mockStorage: &mockStorage{}}
			w := NewWorker(queue.New(rec), WithOwnershipAuditInterval(0))
			w.config.Retention.CompletedAfter = time.Hour
			w.config.Retention.Interval = 5 * time.Millisecond
			w.config.Retention.BatchSize = tc.set

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { w.runRetention(ctx); close(done) }()
			require.Eventually(t, func() bool { return len(rec.seen()) > 0 },
				3*time.Second, 5*time.Millisecond)
			cancel()
			<-done

			for _, got := range rec.seen() {
				require.Equal(t, tc.want, got)
			}
		})
	}
}
