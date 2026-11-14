package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// MySQL reports changed rows, not matched rows, and the heartbeat columns are
// datetime(3). Heartbeats whose statements start inside the same millisecond write
// identical values, so the later UPDATE reports 0 rows for a job the worker owns.
// Heartbeat must not read that as lost ownership.
//
// Goroutines released together start their statements in the same millisecond
// whatever the disk speed, which a free-running loop does not (each statement
// starts after the previous commit). The connection pool is widened because the
// shared test helper opens only two connections on MySQL and Postgres.
func TestHeartbeat_SimultaneousHeartbeatsOnAnOwnedJobNeverReportItLost(t *testing.T) {
	ctx := context.Background()
	s := newConcurrentTestStorage(t)
	sqlDB, err := s.db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(16)
	sqlDB.SetMaxIdleConns(16)

	job := newTestJob("default", "task.heartbeat")
	require.NoError(t, s.Enqueue(ctx, job))
	running, err := s.Dequeue(ctx, []string{"default"}, "worker-hb")
	require.NoError(t, err)
	require.NotNil(t, running)

	const goroutines, rounds = 16, 25
	var lost atomic.Int64
	var firstErr atomic.Value
	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if err := s.Heartbeat(ctx, running.ID, "worker-hb"); err != nil {
					lost.Add(1)
					firstErr.CompareAndSwap(nil, err)
				}
			}()
		}
		close(start)
		wg.Wait()
	}

	if n := lost.Load(); n != 0 {
		assert.Failf(t, "an owned job was reported lost",
			"%d of %d heartbeats failed; first error: %v", n, goroutines*rounds, firstErr.Load())
	}
}

func TestHeartbeat_ReportsALostJobAsNotOwned(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := newTestJob("default", "task.heartbeat")
	require.NoError(t, s.Enqueue(ctx, job))
	running, err := s.Dequeue(ctx, []string{"default"}, "worker-hb")
	require.NoError(t, err)
	require.NotNil(t, running)

	require.ErrorIs(t, s.Heartbeat(ctx, running.ID, "some-other-worker"), core.ErrJobNotOwned)
	require.NoError(t, s.Release(ctx, running.ID, "worker-hb"))
	require.ErrorIs(t, s.Heartbeat(ctx, running.ID, "worker-hb"), core.ErrJobNotOwned,
		"a released job is no longer running, so it is not owned")
}
