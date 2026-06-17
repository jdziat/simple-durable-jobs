package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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
	// completed_at is now written on the DB clock (retention parity) while
	// dead_lettered_at remains wall-clock; both are set in the same terminal
	// UPDATE, so a generous tolerance absorbs any worker<->DB skew without
	// asserting two distinct clocks agree to the second.
	assert.WithinDuration(t, *failed.CompletedAt, *failed.DeadLetteredAt, time.Minute)

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
			ID:     testUUID("failed-without-dlq"),
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
	assert.Equal(t, testUUID("old-email"), sendPage[0].ID)

	count, err := s.CountDeadLettered(ctx, core.DeadLetterFilter{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, count)

	count, err = s.CountDeadLettered(ctx, core.DeadLetterFilter{Queue: "email", Type: "send"})
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)
}

func TestDeadLetterQueries_TenantMetadataAndSearch(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-acme-prod"),
		Type:     "sync",
		Queue:    "default",
		Tenant:   "tenant-a",
		Metadata: core.MetadataMap{"env": "prod", "region": "us"},
		Args:     []byte(`{"account":"needle"}`),
	}, base.Add(3*time.Minute))
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-acme-dev"),
		Type:     "sync",
		Queue:    "default",
		Tenant:   "tenant-a",
		Metadata: core.MetadataMap{"env": "dev", "region": "us"},
		Args:     []byte(`{"account":"other"}`),
	}, base.Add(2*time.Minute))
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-globex-prod"),
		Type:     "sync",
		Queue:    "default",
		Tenant:   "tenant-b",
		Metadata: core.MetadataMap{"env": "prod", "region": "eu"},
		Args:     []byte(`{"account":"needle"}`),
	}, base.Add(time.Minute))

	filter := core.DeadLetterFilter{
		Tenant:       "tenant-a",
		MetaContains: &core.MetadataMap{"env": "prod"},
		Search:       "needle",
		Limit:        10,
	}
	matches, err := s.ListDeadLettered(ctx, filter)
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, testUUID("dlq-acme-prod"), matches[0].ID)

	total, err := s.CountDeadLettered(ctx, filter)
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
}

func TestDeadLetterQueries_MetaContainsRequiresAllPairs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-region-us-prod"),
		Type:     "sync",
		Queue:    "default",
		Metadata: core.MetadataMap{"env": "prod", "region": "us", "tier": "gold"},
	}, base.Add(3*time.Minute))
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-region-us-dev"),
		Type:     "sync",
		Queue:    "default",
		Metadata: core.MetadataMap{"env": "dev", "region": "us", "tier": "gold"},
	}, base.Add(2*time.Minute))
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-region-eu-prod"),
		Type:     "sync",
		Queue:    "default",
		Metadata: core.MetadataMap{"env": "prod", "region": "eu", "tier": "silver"},
	}, base.Add(time.Minute))
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		ID:       testUUID("dlq-no-match"),
		Type:     "sync",
		Queue:    "default",
		Metadata: core.MetadataMap{"env": "stage", "region": "ap"},
	}, base)

	regionFilter := core.DeadLetterFilter{
		MetaContains: &core.MetadataMap{"region": "us"},
		Limit:        10,
	}
	regionMatches, err := s.ListDeadLettered(ctx, regionFilter)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{string(testUUID("dlq-region-us-prod")), string(testUUID("dlq-region-us-dev"))}, deadLetterJobIDs(regionMatches))

	multiPairFilter := core.DeadLetterFilter{
		MetaContains: &core.MetadataMap{"env": "prod", "region": "us"},
		Limit:        10,
	}
	multiPairMatches, err := s.ListDeadLettered(ctx, multiPairFilter)
	require.NoError(t, err)
	require.Len(t, multiPairMatches, 1)
	assert.Equal(t, testUUID("dlq-region-us-prod"), multiPairMatches[0].ID)

	total, err := s.CountDeadLettered(ctx, multiPairFilter)
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
}

func seedDeadLetterJob(t *testing.T, s *GormStorage, id, queue, jobType string, at time.Time) {
	t.Helper()
	seedDeadLetterJobWithDetails(t, s, &core.Job{
		Type:  jobType,
		Queue: queue,
		Metadata: map[string]string{
			"test_id": id,
		},
	}, at)
}

func seedDeadLetterJobWithDetails(t *testing.T, s *GormStorage, job *core.Job, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if job.ID == "" {
		if job.Metadata != nil && job.Metadata["test_id"] != "" {
			job.ID = testUUID(job.Metadata["test_id"])
		} else {
			job.ID = core.NewID()
		}
	}
	job.Status = core.StatusFailed
	job.Attempt = 1
	job.MaxRetries = 1
	job.LastError = "boom"
	job.DeadLetteredAt = &at
	job.DeadLetterReason = "max retries exhausted: boom"
	job.CompletedAt = &at
	require.NoError(t, s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Create(job).Error
	}))
}

func deadLetterJobIDs(jobs []*core.Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		if job.Metadata != nil && job.Metadata["test_id"] != "" {
			ids[i] = job.Metadata["test_id"]
		} else {
			ids[i] = string(job.ID)
		}
	}
	return ids
}
