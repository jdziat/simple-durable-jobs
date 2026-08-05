package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// The signal-resume correlation has a WRITER (which records the awaited name at
// suspend) and a READER (the resume poll, which correlates against it). Both were
// tested, and neither test could fail if the writer stopped writing:
//
//   - the writer only against FAKES in pkg/signal (markerStore, and
//     fakeSignalStore.awaitedName), never against GormStorage;
//   - the reader only against rows a test INSERTED with the column already set.
//
// So degrading both production writers to record "" left seven packages green,
// including the 311s tests/ integration suite, while silently reverting the whole
// fix to the defect it closed: a pending signal the handler will never consume
// resuming and fully replaying a waiting job on every poll tick, forever.
//
// This drives the real writers on real storage and then asks the real reader, so
// the two are pinned against each other rather than against fixtures. It runs on
// every dialect the suite is pointed at, which also means the MySQL collation the
// comparison depends on is exercised through the production write path.
func TestSignalWaitName_ProductionWritersFeedTheResumeReader(t *testing.T) {
	ctx := context.Background()

	runningJob := func(t *testing.T, s *GormStorage) core.UUID {
		t.Helper()
		id := core.NewID()
		require.NoError(t, s.db.Create(&core.Job{
			ID: id, Type: "wf", Queue: "default",
			Status: core.StatusRunning, LockedBy: "w1",
		}).Error)
		return id
	}
	awaitedName := func(t *testing.T, s *GormStorage, id core.UUID) string {
		t.Helper()
		var got string
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", id).
			Pluck("waiting_signal_name", &got).Error)
		return got
	}

	// MarkWaitingForSignal — the indefinite WaitForSignal suspend.
	t.Run("MarkWaitingForSignal records the name the reader correlates on", func(t *testing.T) {
		s := newTestStorage(t)
		job := runningJob(t, s)

		require.NoError(t, s.MarkWaitingForSignal(ctx, job, "w1", "approval"))
		require.Equal(t, "approval", awaitedName(t, s, job),
			"the production writer must persist the awaited name; recording an empty string silently returns this job to the permissive resume path")

		// A signal it is not waiting for must not wake it...
		seedPendingSignal(t, s, job, "unrelated")
		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job,
			"writer and reader disagree: the name was written but the poll still woke the job for an unrelated signal")

		// ...and the one it is waiting for must.
		seedPendingSignal(t, s, job, "approval")
		got, err = s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job,
			"the awaited signal must wake the job; if this fails the correlation costs liveness rather than saving work")
	})

	// SaveCheckpointAndMarkWaitingForSignal — the WaitForSignalTimeout suspend.
	t.Run("the timeout suspend records the name and still wakes on its deadline", func(t *testing.T) {
		s := newTestStorage(t)
		job := runningJob(t, s)

		require.NoError(t, s.SaveCheckpointAndMarkWaitingForSignal(ctx, &core.Checkpoint{
			ID: core.NewID(), JobID: job, CallIndex: 0,
			CallType: core.CheckpointTypeSignalTimeoutPrefix + "approval",
			Result:   []byte(`{"deadline":1,"resolved":false}`),
		}, job, "w1", time.Hour, "approval"))

		require.Equal(t, "approval", awaitedName(t, s, job))

		seedPendingSignal(t, s, job, "unrelated")
		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job,
			"a timed wait must not be woken by a signal it is not parked on")

		seedPendingSignal(t, s, job, "approval")
		got, err = s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job)
	})

	// A durable Sleep records the reserved sleep type, so no user signal matches it.
	t.Run("the sleep suspend records the reserved type so no signal wakes it", func(t *testing.T) {
		s := newTestStorage(t)
		job := runningJob(t, s)

		require.NoError(t, s.SaveCheckpointAndMarkWaitingForSignal(ctx, &core.Checkpoint{
			ID: core.NewID(), JobID: job, CallIndex: 0,
			CallType: core.CheckpointTypeSleep,
			Result:   []byte(`{"deadline":1}`),
		}, job, "w1", time.Hour, core.CheckpointTypeSleep))

		require.Equal(t, core.CheckpointTypeSleep, awaitedName(t, s, job))

		seedPendingSignal(t, s, job, "anything")
		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.NotContains(t, resumeIDs(got), job,
			"a buffered signal must not wake a sleeping job before its run_at")
	})

	// The fan-out suspend deliberately records NOTHING, which is what keeps it on
	// the permissive resume path. Asserted here so a future change that starts
	// recording a name there — and would strand fan-out parents — is caught.
	t.Run("MarkWaiting records no name, keeping fan-out suspends permissive", func(t *testing.T) {
		s := newTestStorage(t)
		job := runningJob(t, s)

		require.NoError(t, s.MarkWaiting(ctx, job, "w1"))
		require.Equal(t, "", awaitedName(t, s, job),
			"MarkWaiting is the fan-out/unnamed path and must leave the name empty")

		seedPendingSignal(t, s, job, "anything")
		got, err := s.GetSignalWaitingJobsToResume(ctx)
		require.NoError(t, err)
		require.Contains(t, resumeIDs(got), job,
			"with no recorded name the poll must stay permissive, or an unnamed wait never resumes")
	})

	// A named wait followed by MarkWaiting must CLEAR the name, not leave the old
	// one narrowing the new wait.
	t.Run("MarkWaiting clears a name left by an earlier named wait", func(t *testing.T) {
		s := newTestStorage(t)
		job := runningJob(t, s)

		require.NoError(t, s.MarkWaitingForSignal(ctx, job, "w1", "approval"))
		require.Equal(t, "approval", awaitedName(t, s, job))

		// Back to running, then suspend again through the unnamed path.
		require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", job).
			Updates(map[string]any{"status": core.StatusRunning, "locked_by": "w1"}).Error)
		require.NoError(t, s.MarkWaiting(ctx, job, "w1"))

		require.Equal(t, "", awaitedName(t, s, job),
			"a stale name from a previous wait would narrow this one to a signal the handler is no longer parked on")
	})
}
