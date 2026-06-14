package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type signalTestStorage struct {
	*jobs.GormStorage
	sendCount atomic.Int64
	resumeErr error
}

func (s *signalTestStorage) SendSignal(ctx context.Context, jobID core.UUID, name string, payload []byte) error {
	s.sendCount.Add(1)
	return s.GormStorage.SendSignal(ctx, jobID, name, payload)
}

func (s *signalTestStorage) ResumeSignalWaitingJob(ctx context.Context, jobID core.UUID) (bool, error) {
	if s.resumeErr != nil {
		return false, s.resumeErr
	}
	return s.GormStorage.ResumeSignalWaitingJob(ctx, jobID)
}

func newSignalTestQueue(t *testing.T) (*jobs.Queue, *signalTestStorage) {
	t.Helper()
	dsn := fmt.Sprintf("file:signaltest_%d?mode=memory&cache=shared&_busy_timeout=5000", facadeTestDBSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := &signalTestStorage{GormStorage: jobs.NewGormStorage(db)}
	require.NoError(t, store.Migrate(context.Background()))
	return jobs.New(store), store
}

func seedWaitingSignalJob(t *testing.T, ctx context.Context, store *signalTestStorage) *jobs.Job {
	t.Helper()
	job := &jobs.Job{
		Type:   "signal.wait",
		Queue:  "default",
		Status: jobs.StatusWaiting,
	}
	require.NoError(t, store.Enqueue(ctx, job))
	return job
}

func waitForJobResumedBySignal(t *testing.T, events <-chan core.Event, jobID core.UUID, name string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-events:
			resumed, ok := event.(*jobs.JobResumedBySignal)
			if ok && resumed.JobID == jobID && resumed.SignalName == name {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for JobResumedBySignal(%s, %s)", jobID, name)
		}
	}
}

func TestSignal_ReturnsNilWhenImmediateResumeFailsAfterDelivery(t *testing.T) {
	ctx := context.Background()
	q, store := newSignalTestQueue(t)
	store.resumeErr = errors.New("resume unavailable")
	job := seedWaitingSignalJob(t, ctx, store)

	err := q.Signal(ctx, job.ID, "ready", map[string]string{"ok": "true"})

	require.NoError(t, err)
	assert.EqualValues(t, 1, store.sendCount.Load(), "Signal must not ask callers to retry a durably delivered signal")

	var count int64
	require.NoError(t, store.DB().Model(&core.Signal{}).
		Where("job_id = ? AND name = ?", job.ID, "ready").
		Count(&count).Error)
	assert.EqualValues(t, 1, count, "signal should be persisted exactly once")
}

func TestSignal_HappyPathEmitsJobResumedBySignal(t *testing.T) {
	ctx := context.Background()
	q, store := newSignalTestQueue(t)
	events := q.Events()
	defer q.Unsubscribe(events)
	job := seedWaitingSignalJob(t, ctx, store)

	require.NoError(t, q.Signal(ctx, job.ID, "ready", "payload"))
	assert.EqualValues(t, 1, store.sendCount.Load())
	waitForJobResumedBySignal(t, events, job.ID, "ready")

	after, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, jobs.StatusPending, after.Status)
}
