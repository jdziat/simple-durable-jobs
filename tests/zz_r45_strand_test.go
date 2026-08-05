package jobs_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestR45_NoStrandedClaimOnWorkerShutdown pins the contract published in
// docs/content/docs/advanced/batch-dequeue.md: "If the worker shuts down while
// claimed jobs are still buffered or not yet delivered to a handler, those jobs
// are released back to `pending` immediately instead of waiting for stale-lock
// recovery."
//
// Before the fix, the batch RETURNING claim path (Postgres/SQLite default) ran
// its `WITH claimed AS MATERIALIZED (...) UPDATE ... RETURNING *` as ONE
// autocommit statement. A ctx cancellation landing between the server-side
// COMMIT and the client-side scan of the RETURNING rows left the rows durably
// status='running', locked_by=<dead worker>, attempt+1 — with the ids unknown to
// the caller, so releaseClaimedOnAbort was never reached. Recovery waited for
// the stale-lock reaper (StaleLockAge, 45m by default).
//
// The test cancels workers mid-claim over many generations and asserts that once
// Start has returned (drain complete) NO job is left running.
func TestR45_NoStrandedClaimOnWorkerShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("churn test")
	}
	queue, store := openIntegrationQueue(t)
	gdb := store.(interface{ DB() *gorm.DB }).DB()

	queue.Register("r45strand", func(ctx context.Context, args map[string]string) error {
		return nil
	})

	const generations = 20
	const workersPerGen = 3
	const jobsPerGen = 300

	for gen := 0; gen < generations; gen++ {
		for i := 0; i < jobsPerGen; i++ {
			_, err := queue.Enqueue(context.Background(), "r45strand",
				map[string]string{"g": fmt.Sprintf("%d-%d", gen, i)})
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		for w := 0; w < workersPerGen; w++ {
			wk := jobs.NewWorker(queue,
				jobs.Concurrency(4),
				jobs.WithDequeueBatchSize(20),
				jobs.WithPollInterval(5*time.Millisecond),
			)
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = wk.Start(ctx)
			}()
		}
		// Stagger the cancel across the poll cycle so it lands inside a claim
		// statement's round-trip rather than always between polls.
		time.Sleep(time.Duration(40+gen*3) * time.Millisecond)
		cancel()
		wg.Wait()

		var stranded []core.Job
		require.NoError(t, gdb.Where("status = ?", core.StatusRunning).Find(&stranded).Error)
		if len(stranded) > 0 {
			for _, j := range stranded {
				t.Logf("gen %d stranded id=%s locked_by=%s attempt=%d heartbeat=%v",
					gen, j.ID, j.LockedBy, j.Attempt, j.LastHeartbeatAt)
			}
			t.Fatalf("gen %d: %d job(s) left status=running after every worker's Start returned; "+
				"batch-dequeue.md promises they are released to pending immediately", gen, len(stranded))
		}
	}
}
