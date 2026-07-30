package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestScopedMutations_DoNotTouchABystander covers a family of DELETE/UPDATE
// scope clauses that were each individually FREE: neutralizing any one of them
// left the whole of ./pkg/storage green.
//
// They were free for one structural reason, not seven unrelated ones. Every
// existing test for these methods seeds exactly ONE job (or one queue) and then
// asserts that job's dependent rows are gone. With a single row in the table,
// "delete the rows belonging to this job" and "delete every row in the table"
// produce an identical observable result, so the scope predicate carries no
// weight. The mutation that exposes this is `WHERE (job_id = ?) OR 1=1`.
//
// The blast radius if one of these were ever dropped by a refactor:
//
//	Complete            -> deletes EVERY job's checkpoints, not just this job's
//	CancelSubJob        -> deletes EVERY concurrency slot in the fleet, so every
//	                       fleet-wide cap is released at once and unrelated jobs
//	                       are admitted past their limits
//	CancelJobTerminal   -> same fleet-wide slot wipe
//	PauseQueue          -> pauses EVERY queue, halting the whole deployment
//	DeleteJob           -> deletes EVERY job's checkpoints and buffered signals
//
// Each subtest therefore seeds a BYSTANDER that the operation must not touch,
// and asserts the bystander's rows survive. Verified red-before-green: with the
// corresponding scope clause neutralized, each subtest fails, and it is the
// only failure.
//
// This is a scope/blast-radius test, not a behaviour test — the behaviour of
// each method on its own rows is already covered elsewhere. Adding a bystander
// is the cheapest thing that makes a scope clause load-bearing under test.
func TestScopedMutations_DoNotTouchABystander(t *testing.T) {
	t.Run("Complete's checkpoint GC spares another job's checkpoints", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		s.SetDeleteCheckpointsOnComplete(true)

		target, bystander := core.NewID(), core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: target, Type: "fixture.job", Queue: "default",
			Status: core.StatusRunning, LockedBy: "w1",
		}).Error)
		seedTestJob(t, ctx, s, bystander, core.StatusRunning)
		for _, id := range []core.UUID{target, bystander} {
			require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
				JobID: id, CallIndex: 0, CallType: "x", Result: []byte(`"r"`),
			}))
		}

		require.NoError(t, s.Complete(ctx, target, "w1"))

		require.Equal(t, int64(0), countRowsForJob(t, s, &core.Checkpoint{}, target),
			"the completed job's own checkpoints should be GC'd")
		require.Equal(t, int64(1), countRowsForJob(t, s, &core.Checkpoint{}, bystander),
			"another job's checkpoints must survive; an unscoped delete wipes the whole table")
	})

	t.Run("CancelSubJob releases only its own concurrency slot", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		fanOutID := core.NewID()

		target, bystander := core.NewID(), core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: target, Type: "fixture.job", Queue: "default",
			Status: core.StatusRunning, FanOutID: &fanOutID,
		}).Error)
		// fan_outs.parent_job_id carries a real FK (fk_fanouts_parent) that
		// Postgres and MySQL enforce and SQLite does not, so the parent must be a
		// row that exists rather than a fresh UUID.
		fanOutParent := core.NewID()
		seedTestJob(t, ctx, s, fanOutParent, core.StatusWaiting)
		require.NoError(t, s.CreateFanOut(ctx, &core.FanOut{
			ID: fanOutID, ParentJobID: fanOutParent, TotalCount: 1, Status: core.FanOutPending,
		}))
		seedTestJob(t, ctx, s, bystander, core.StatusRunning)
		acquireSlotFor(t, ctx, s, "bystander-cap-"+string(bystander), bystander)
		acquireSlotFor(t, ctx, s, "target-cap-"+string(target), target)

		_, err := s.CancelSubJob(ctx, target)
		require.NoError(t, err)

		require.Equal(t, int64(0), countRowsForJob(t, s, &core.ConcurrencySlot{}, target),
			"the cancelled sub-job's own slot should be released")
		require.Equal(t, int64(1), countRowsForJob(t, s, &core.ConcurrencySlot{}, bystander),
			"an unrelated job's slot must survive; an unscoped delete releases every cap in the fleet")
	})

	t.Run("CancelJobTerminal releases only its own concurrency slot", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		target, bystander := core.NewID(), core.NewID()
		seedTestJob(t, ctx, s, target, core.StatusRunning)
		seedTestJob(t, ctx, s, bystander, core.StatusRunning)
		acquireSlotFor(t, ctx, s, "bystander-cap-"+string(bystander), bystander)
		acquireSlotFor(t, ctx, s, "target-cap-"+string(target), target)

		require.NoError(t, s.CancelJobTerminal(ctx, target))

		require.Equal(t, int64(0), countRowsForJob(t, s, &core.ConcurrencySlot{}, target),
			"the cancelled job's own slot should be released")
		require.Equal(t, int64(1), countRowsForJob(t, s, &core.ConcurrencySlot{}, bystander),
			"an unrelated job's slot must survive; an unscoped delete releases every cap in the fleet")
	})

	t.Run("PauseQueue pauses only the named queue", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		// BOTH queues must already have a queue_states row, and that is
		// load-bearing for the TARGET as much as the bystander. PauseQueue only
		// reaches the scoped UPDATE when a row already exists; on a queue it has
		// never seen it takes the Create branch instead, where there is no scope
		// clause to get wrong. Pausing a fresh "target-queue" here made this
		// subtest unable to fail — the control mutation left it green. Pause and
		// unpause each queue first so the assertion below exercises the UPDATE.
		for _, q := range []string{"bystander-queue", "target-queue"} {
			require.NoError(t, s.PauseQueue(ctx, q))
			require.NoError(t, s.UnpauseQueue(ctx, q))
		}

		require.NoError(t, s.PauseQueue(ctx, "target-queue"))

		require.True(t, queuePaused(t, s, "target-queue"), "the named queue should be paused")
		require.False(t, queuePaused(t, s, "bystander-queue"),
			"another queue must not be paused; an unscoped update halts the whole deployment")
	})

	t.Run("DeleteJob spares another job's checkpoints and signals", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		target, bystander := core.NewID(), core.NewID()
		seedTestJob(t, ctx, s, target, core.StatusCompleted)
		seedTestJob(t, ctx, s, bystander, core.StatusCompleted)
		for _, id := range []core.UUID{target, bystander} {
			require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
				JobID: id, CallIndex: 0, CallType: "x", Result: []byte(`"r"`),
			}))
			require.NoError(t, s.db.Create(&core.Signal{
				ID: core.NewID(), JobID: id, Name: "sig", Payload: []byte(`"p"`),
				CreatedAt: time.Now(),
			}).Error)
		}

		require.NoError(t, s.DeleteJob(ctx, target))

		require.Equal(t, int64(0), countRowsForJob(t, s, &core.Checkpoint{}, target))
		require.Equal(t, int64(0), countRowsForJob(t, s, &core.Signal{}, target))
		require.Equal(t, int64(1), countRowsForJob(t, s, &core.Checkpoint{}, bystander),
			"another job's checkpoints must survive; an unscoped delete wipes the whole table")
		require.Equal(t, int64(1), countRowsForJob(t, s, &core.Signal{}, bystander),
			"another job's buffered signals must survive; an unscoped delete wipes the whole table")
	})
}

// TestCascadeDeletes_DoNotTouchABystander is the same blast-radius family as
// above, for the three methods that delete dependent rows by a PLUCKED ID LIST
// rather than a single job id: retention GC, PurgeJobs, and DeleteWorkflowSubtree.
// Each resolves a set of job ids and then deletes checkpoints, signals and unique
// locks `WHERE job_id IN (those ids)`.
//
// Those IN-list scopes were free for the same reason as the single-id ones: the
// existing tests contain only rows belonging to jobs that are being deleted, so
// "delete the dependent rows of these ids" and "delete every dependent row"
// cannot be told apart. Neutralized, each of these wipes the checkpoints,
// buffered signals and dedup locks of jobs that were deliberately SPARED — a
// surviving job silently loses its replay state, so on its next attempt it
// re-executes work its checkpoints had already recorded.
//
// Each subtest keeps a bystander that the operation is required to spare, and
// asserts its dependent rows survive alongside it.
func TestCascadeDeletes_DoNotTouchABystander(t *testing.T) {
	t.Run("retention GC spares a within-window job's dependent rows", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)
		old := time.Now().Add(-2 * time.Hour).UTC()
		recent := time.Now().Add(-10 * time.Minute).UTC()

		// Purged: terminal and older than the window.
		purged := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: purged, Type: "wf", Queue: "default",
			Status: core.StatusCompleted, CompletedAt: &old,
		}).Error)
		seedDependentRows(t, ctx, s, purged, "scope-purged")

		// Spared: terminal but INSIDE the retention window, so it survives — and
		// so must everything hanging off it.
		spared := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: spared, Type: "wf", Queue: "default",
			Status: core.StatusCompleted, CompletedAt: &recent,
		}).Error)
		seedDependentRows(t, ctx, s, spared, "scope-spared")

		deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted, "only the out-of-window job should be collected")

		assertDependentRowsSurvive(t, s, spared,
			"retention collected an out-of-window job and took a within-window job's replay state with it; that job now re-executes work its checkpoints had recorded")
	})

	t.Run("PurgeJobs spares another queue's dependent rows", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		purged := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: purged, Type: "wf", Queue: "target-queue", Status: core.StatusFailed,
		}).Error)
		seedDependentRows(t, ctx, s, purged, "purge-target")

		spared := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: spared, Type: "wf", Queue: "other-queue", Status: core.StatusFailed,
		}).Error)
		seedDependentRows(t, ctx, s, spared, "purge-spared")

		n, err := s.PurgeJobs(ctx, "target-queue", core.StatusFailed)
		require.NoError(t, err)
		require.Equal(t, int64(1), n, "only the target queue should be purged")

		assertDependentRowsSurvive(t, s, spared,
			"purging one queue deleted another queue's checkpoints, signals or dedup locks")
	})

	t.Run("DeleteWorkflowSubtree spares an unrelated job's dependent rows", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		root := core.NewID()
		seedTestJob(t, ctx, s, root, core.StatusCompleted)
		seedDependentRows(t, ctx, s, root, "subtree-root")

		spared := core.NewID()
		seedTestJob(t, ctx, s, spared, core.StatusCompleted)
		seedDependentRows(t, ctx, s, spared, "subtree-spared")

		require.NoError(t, s.DeleteWorkflowSubtree(ctx, root))

		assertDependentRowsSurvive(t, s, spared,
			"deleting one workflow subtree deleted an unrelated job's checkpoints, signals or dedup locks")
	})
}

// seedDependentRows gives a job one of every dependent row the cascade paths
// delete, so a scope regression shows up whichever table it reaches.
func seedDependentRows(t *testing.T, ctx context.Context, s *GormStorage, jobID core.UUID, scopeHash string) {
	t.Helper()
	require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{
		JobID: jobID, CallIndex: 0, CallType: "x", Result: []byte(`"r"`),
	}))
	require.NoError(t, s.db.Create(&core.Signal{
		ID: core.NewID(), JobID: jobID, Name: "sig", Payload: []byte(`"p"`), CreatedAt: time.Now(),
	}).Error)
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: scopeHash, JobID: jobID, ExpiresAt: time.Now().Add(time.Hour),
	}).Error)
}

func assertDependentRowsSurvive(t *testing.T, s *GormStorage, jobID core.UUID, msg string) {
	t.Helper()
	var jobs int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", jobID).Count(&jobs).Error)
	require.Equal(t, int64(1), jobs, "the bystander job itself must survive: %s", msg)
	require.Equal(t, int64(1), countRowsForJob(t, s, &core.Checkpoint{}, jobID), "checkpoints: %s", msg)
	require.Equal(t, int64(1), countRowsForJob(t, s, &core.Signal{}, jobID), "signals: %s", msg)
	require.Equal(t, int64(1), countRowsForJob(t, s, &core.UniqueLock{}, jobID), "unique locks: %s", msg)
}

func countRowsForJob(t *testing.T, s *GormStorage, model any, jobID core.UUID) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(model).Where("job_id = ?", jobID).Count(&n).Error)
	return n
}

func queuePaused(t *testing.T, s *GormStorage, queue string) bool {
	t.Helper()
	var qs core.QueueState
	require.NoError(t, s.db.Where("queue = ?", queue).First(&qs).Error)
	return qs.Paused
}

func acquireSlotFor(t *testing.T, ctx context.Context, s *GormStorage, slot string, jobID core.UUID) {
	t.Helper()
	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobID, "w-"+string(jobID), 5, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
}
