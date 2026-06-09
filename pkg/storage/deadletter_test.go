package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
)

func TestDeadLetterMetadata_TerminalFailureOnlyAndRequeueClears(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	exhausted := &core.Job{Type: "dlq.exhausted", Queue: "critical", MaxRetries: 1}
	require.NoError(t, s.Enqueue(ctx, exhausted))
	got, err := s.Dequeue(ctx, []string{"critical"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "database password=secret exploded", nil))

	failed, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, failed)
	assert.Equal(t, core.StatusFailed, failed.Status)
	require.NotNil(t, failed.DeadLetteredAt)
	assert.NotEmpty(t, failed.DeadLetterReason)
	assert.Contains(t, failed.DeadLetterReason, "max retries exhausted:")
	assert.NotContains(t, failed.DeadLetterReason, "password=secret")
	require.NotNil(t, failed.CompletedAt)
	assert.WithinDuration(t, *failed.CompletedAt, *failed.DeadLetteredAt, time.Second)

	retryable := &core.Job{Type: "dlq.retryable", Queue: "critical", MaxRetries: 2}
	require.NoError(t, s.Enqueue(ctx, retryable))
	got, err = s.Dequeue(ctx, []string{"critical"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	retryAt := time.Now().Add(time.Minute)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-1", "try again", &retryAt))
	pendingRetry, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, pendingRetry)
	assert.Equal(t, core.StatusPending, pendingRetry.Status)
	assert.Nil(t, pendingRetry.DeadLetteredAt)
	assert.Empty(t, pendingRetry.DeadLetterReason)

	cancelled := &core.Job{Type: "dlq.cancelled", Queue: "critical", MaxRetries: 1}
	require.NoError(t, s.Enqueue(ctx, cancelled))
	got, err = s.Dequeue(ctx, []string{"critical"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.PauseJob(ctx, got.ID))
	cancelledJob, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	require.NotNil(t, cancelledJob)
	assert.Equal(t, core.StatusCancelled, cancelledJob.Status)
	assert.Nil(t, cancelledJob.DeadLetteredAt)
	assert.Empty(t, cancelledJob.DeadLetterReason)

	requeued, err := s.Requeue(ctx, failed.ID)
	require.NoError(t, err)
	assert.True(t, requeued)
	replayed, err := s.GetJob(ctx, failed.ID)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	assert.Equal(t, core.StatusPending, replayed.Status)
	assert.Nil(t, replayed.DeadLetteredAt)
	assert.Empty(t, replayed.DeadLetterReason)
}

func TestDeadLetterQueries_FilterPaginationAndCount(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedDeadLetterJob(t, s, "old-email", "email", "send", base.Add(time.Minute))
	seedDeadLetterJob(t, s, "new-email", "email", "send", base.Add(3*time.Minute))
	seedDeadLetterJob(t, s, "new-import", "imports", "import", base.Add(2*time.Minute))
	require.NoError(t, s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Create(&core.Job{
			ID:     "failed-without-dlq",
			Type:   "send",
			Queue:  "email",
			Status: core.StatusFailed,
		}).Error
	}))

	all, err := s.ListDeadLettered(ctx, core.DeadLetterFilter{Limit: 10})
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []string{"new-email", "new-import", "old-email"}, deadLetterJobIDs(all))

	email, err := s.ListDeadLettered(ctx, core.DeadLetterFilter{Queue: "email", Limit: 10})
	require.NoError(t, err)
	require.Len(t, email, 2)
	assert.Equal(t, []string{"new-email", "old-email"}, deadLetterJobIDs(email))

	sendPage, err := s.ListDeadLettered(ctx, core.DeadLetterFilter{Type: "send", Limit: 1, Offset: 1})
	require.NoError(t, err)
	require.Len(t, sendPage, 1)
	assert.Equal(t, "old-email", sendPage[0].ID)

	count, err := s.CountDeadLettered(ctx, core.DeadLetterFilter{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, count)

	count, err = s.CountDeadLettered(ctx, core.DeadLetterFilter{Queue: "email", Type: "send"})
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)
}

func seedDeadLetterJob(t *testing.T, s *GormStorage, id, queue, jobType string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Create(&core.Job{
			ID:               id,
			Type:             jobType,
			Queue:            queue,
			Status:           core.StatusFailed,
			Attempt:          1,
			MaxRetries:       1,
			LastError:        "boom",
			DeadLetteredAt:   &at,
			DeadLetterReason: "max retries exhausted: boom",
			CompletedAt:      &at,
		}).Error
	}))
}

func deadLetterJobIDs(jobs []*core.Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids
}
