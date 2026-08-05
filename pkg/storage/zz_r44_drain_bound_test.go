package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestDrainSignalsIsBindParameterBounded pins the IN-list bound on the bulk
// signal consume.
//
// Nothing caps how many signals may buffer for a job, so `ids` here is
// user-data-unbounded. Unchunked, a backlog past the driver's bind-parameter
// ceiling failed the statement outright — and because DrainSignals is the ONLY
// bulk consume, the backlog could never be cleared: the handler errors, the job
// retries, fails identically, and dead-letters. The single-signal control in this
// test is what isolates the IN-list as the cause rather than row volume.
//
// The fixture inserts in 200-row batches on purpose: a batch INSERT binds every
// column of every row, so a 1000-row batch exceeds the same ceiling during
// SEEDING and the test fails before reaching the code under test. That mistake
// cost a round.
func TestDrainSignalsIsBindParameterBounded(t *testing.T) {
	const backlog = 33000 // above SQLite's 32766 ceiling

	seed := func(t *testing.T, s *GormStorage) core.UUID {
		t.Helper()
		ctx := context.Background()
		owner := &core.Job{
			ID: core.NewID(), Type: "waiter", Queue: "default", Status: core.StatusWaiting,
		}
		require.NoError(t, s.Enqueue(ctx, owner))
		now := time.Now().UTC()
		batch := make([]core.Signal, 0, 200)
		flush := func() {
			if len(batch) > 0 {
				require.NoError(t, s.db.Create(batch).Error)
				batch = batch[:0]
			}
		}
		for i := 0; i < backlog; i++ {
			batch = append(batch, core.Signal{
				ID: core.NewID(), JobID: owner.ID, Name: "tick",
				Payload: []byte(`"v"`), CreatedAt: now,
			})
			if len(batch) == 200 {
				flush()
			}
		}
		flush()
		return owner.ID
	}

	t.Run("bulk drain clears the whole backlog", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		jobID := seed(t, s)

		got, err := s.DrainSignals(ctx, jobID, "tick")
		require.NoErrorf(t, err, "a %d-signal backlog must not exceed the bind-parameter ceiling", backlog)
		require.Len(t, got, backlog, "every buffered signal must be drained")

		var left int64
		require.NoError(t, s.db.Model(&core.Signal{}).
			Where("job_id = ? AND consumed_at IS NULL", jobID).Count(&left).Error)
		require.Zero(t, left, "no signal may be left unconsumed")
	})

	t.Run("handler-facing drain clears the whole backlog", func(t *testing.T) {
		// DrainSignalsTxOwned is the path jobs.DrainSignals actually reaches, so a
		// bound on the sibling above does not cover it. The first version of this
		// test exercised only the sibling, and unchunking THIS site survived.
		ctx := context.Background()
		s := newTestStorage(t)
		jobID := seed(t, s)
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", jobID).
			Updates(map[string]any{"status": core.StatusRunning, "locked_by": "worker-A"}).Error)

		got, err := s.DrainSignalsTxOwned(ctx, jobID, "worker-A", "tick",
			func(sigs []*core.Signal) (*core.Checkpoint, error) { return nil, nil })
		require.NoErrorf(t, err, "handler-facing drain of %d signals must not exceed the ceiling", backlog)
		require.Len(t, got, backlog)
	})

	t.Run("control: single-signal consume works on the same backlog", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		jobID := seed(t, s)

		// Isolates the IN-list as the cause: the same row volume is fine one at a time.
		sig, err := s.ConsumeSignal(ctx, jobID, "tick")
		require.NoError(t, err)
		require.NotNil(t, sig)
	})
}
