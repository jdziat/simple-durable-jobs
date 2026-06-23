package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startManagedWorker(q *jobs.Queue, opts ...jobs.WorkerOption) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	w := jobs.NewWorker(q, opts...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Start(ctx)
	}()
	return cancel, done
}

func TestSleep_SuspendsAndResumesAfterDeadline(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs atomic.Int32
	q.Register("timer.sleep", func(ctx context.Context, _ struct{}) error {
		runs.Add(1)
		return jobs.Sleep(ctx, 200*time.Millisecond)
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "timer.sleep", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond, "sleeping job should suspend to waiting")

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job.RunAt, "sleep deadline should be stored in run_at")
	assert.Equal(t, int32(1), runs.Load())

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 20*time.Second, 100*time.Millisecond, "sleeping job should resume after deadline")
	assert.Equal(t, int32(2), runs.Load(), "handler should run once to suspend and once to complete")
}

func TestSleep_DoesNotEmitResumedBySignalOnDeadlineWake(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	q.Register("timer.no-signal-event", func(ctx context.Context, _ struct{}) error {
		return jobs.Sleep(ctx, 200*time.Millisecond)
	})
	startWorker(t, q)

	events := q.Events()
	ctx := context.Background()
	id, err := q.Enqueue(ctx, "timer.no-signal-event", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 20*time.Second, 100*time.Millisecond)

	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case e := <-events:
			if resumed, ok := e.(*jobs.JobResumedBySignal); ok && resumed.JobID == id {
				t.Fatalf("durable timer wake emitted JobResumedBySignal: %#v", resumed)
			}
		case <-deadline:
			return
		}
	}
}

func TestSleep_FreesWorkerSlotWhileWaiting(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var quickDone atomic.Bool
	q.Register("timer.long-sleep", func(ctx context.Context, _ struct{}) error {
		return jobs.Sleep(ctx, 5*time.Second)
	})
	q.Register("timer.quick", func(ctx context.Context, _ struct{}) error {
		quickDone.Store(true)
		return nil
	})
	cancel, done := startManagedWorker(q, jobs.WorkerQueue("default", jobs.Concurrency(1)))
	t.Cleanup(func() {
		cancel()
		<-done
	})

	ctx := context.Background()
	sleepID, err := q.Enqueue(ctx, "timer.long-sleep", struct{}{})
	require.NoError(t, err)
	quickID, err := q.Enqueue(ctx, "timer.quick", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, sleepID)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, quickID)
		return quickDone.Load() && st == jobs.StatusCompleted
	}, 10*time.Second, 50*time.Millisecond, "a waiting sleep must not occupy the only worker slot")
}

func TestSleepUntil_PastFastPathCompletes(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs atomic.Int32
	q.Register("timer.past", func(ctx context.Context, _ struct{}) error {
		runs.Add(1)
		return jobs.SleepUntil(ctx, time.Now().Add(-time.Second))
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "timer.past", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 10*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(1), runs.Load(), "past SleepUntil should not suspend and replay")
}

func TestSleep_CrashResumeKeepsOriginalDeadline(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs atomic.Int32
	q.Register("timer.crash", func(ctx context.Context, _ struct{}) error {
		runs.Add(1)
		return jobs.Sleep(ctx, 500*time.Millisecond)
	})

	cancel1, done1 := startManagedWorker(q, jobs.WorkerQueue("default", jobs.Concurrency(1)))
	t.Cleanup(func() {
		cancel1()
		<-done1
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "timer.crash", struct{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)
	cancel1()
	<-done1

	time.Sleep(700 * time.Millisecond)

	cancel2, done2 := startManagedWorker(q, jobs.WorkerQueue("default", jobs.Concurrency(1)))
	t.Cleanup(func() {
		cancel2()
		<-done2
	})

	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusCompleted
	}, 20*time.Second, 100*time.Millisecond)
	assert.Equal(t, int32(2), runs.Load(), "resume should complete from the original deadline, not suspend for a restarted duration")
}

func TestSleep_UserSignalCannotWakeSleep(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs atomic.Int32
	q.Register("timer.isolated", func(ctx context.Context, _ struct{}) error {
		runs.Add(1)
		return jobs.Sleep(ctx, 10*time.Second)
	})
	startWorker(t, q)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "timer.isolated", struct{}{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, q.Signal(ctx, id, "unrelated", "payload"))
	deadline := time.After(6 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			goto checked
		case <-ticker.C:
			st, err := q.LoadStatus(ctx, id)
			require.NoError(t, err)
			require.Equal(t, jobs.StatusWaiting, st, "unrelated user signals must not wake a future sleep")
			require.Equal(t, int32(1), runs.Load(), "sleep handler must not replay before its deadline")
		}
	}

checked:
	require.Eventually(t, func() bool {
		st, _ := q.LoadStatus(ctx, id)
		return st == jobs.StatusWaiting && runs.Load() == 1
	}, time.Second, 100*time.Millisecond, "sleep should still be waiting after the signal isolation window")

	err = q.Signal(ctx, id, "_sleep", "payload")
	require.Error(t, err)
	assert.True(t, errors.Is(err, jobs.ErrSignalNameReserved))
}
