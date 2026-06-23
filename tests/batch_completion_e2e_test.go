package jobs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchCompletion_EndToEnd drives the worker with group-committed leaf
// completion (WithBatchCompletion) end-to-end against the configured backend
// (SQLite by default; Postgres/MySQL when TEST_DATABASE_URL / TEST_MYSQL_URL is
// set). It proves the accumulator -> BatchComplete path completes every leaf job
// with the correct persisted result on a real storage backend.
func TestBatchCompletion_EndToEnd(t *testing.T) {
	queue, _ := openIntegrationQueue(t)

	const n = 50
	var ran atomic.Int32
	queue.Register("leaf.double", func(_ context.Context, x int) (int, error) {
		ran.Add(1)
		return x * 2, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ids := make([]jobs.UUID, n)
	for i := 0; i < n; i++ {
		id, err := queue.Enqueue(ctx, "leaf.double", i)
		require.NoError(t, err)
		ids[i] = id
	}

	// Small batch + short delay so batches actually form under modest concurrency.
	w := queue.NewWorker(
		jobs.WithScheduler(false),
		jobs.Concurrency(8),
		jobs.WithDequeueBatchSize(16),
		jobs.WithBatchCompletion(16, 25*time.Millisecond),
	)
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		for _, id := range ids {
			status, err := queue.LoadStatus(ctx, id)
			if err != nil || status != jobs.StatusCompleted {
				return false
			}
		}
		return true
	}, 25*time.Second, 100*time.Millisecond, "all leaf jobs must reach completed via batched completion")

	assert.Equal(t, int32(n), ran.Load(), "every leaf handler runs exactly once")

	// Results persisted correctly through the batched path.
	for i, id := range ids {
		got, err := jobs.LoadResult[int](ctx, queue, id)
		require.NoError(t, err, "load result for job %d", i)
		assert.Equal(t, i*2, got, "job %s result", fmt.Sprint(id))
	}
}
