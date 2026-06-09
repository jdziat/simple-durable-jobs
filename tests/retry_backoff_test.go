package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v2"
)

func TestRetryBackoff_PersistsConfiguredNextRunAcrossBackends(t *testing.T) {
	store := openIntegrationStorage(t)
	queue := jobs.New(store)
	delay := 20 * time.Second

	queue.Register("always-fails", func(context.Context, struct{}) error {
		return errors.New("dependency unavailable")
	}, jobs.WithHandlerBackoff(jobs.ExponentialBackoff{
		InitialInterval: delay,
		Multiplier:      2,
		MaxInterval:     time.Hour,
		JitterFraction:  0,
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	jobID, err := queue.Enqueue(ctx, "always-fails", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)

	before := time.Now()
	worker := queue.NewWorker(jobs.WithPollInterval(50 * time.Millisecond))
	go func() { _ = worker.Start(ctx) }()

	job := waitForRetryRunAt(t, store, jobID, 3*time.Second)
	require.NotNil(t, job.RunAt)
	assert.GreaterOrEqual(t, job.RunAt.Sub(before), 19*time.Second)
}

func TestRetryBackoff_PrecedenceRetryAfterAndNoRetry(t *testing.T) {
	store := openIntegrationStorage(t)
	queue := jobs.New(store)

	queue.Register("retry-after-wins", func(context.Context, struct{}) error {
		return jobs.RetryAfter(2*time.Second, errors.New("rate limited"))
	}, jobs.WithHandlerBackoff(jobs.BackoffFunc(func(int, error) time.Duration {
		return 30 * time.Second
	})))
	queue.Register("no-retry-wins", func(context.Context, struct{}) error {
		return jobs.NoRetry(errors.New("permanent"))
	}, jobs.WithHandlerBackoff(jobs.BackoffFunc(func(int, error) time.Duration {
		return 30 * time.Second
	})))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retryAfterID, err := queue.Enqueue(ctx, "retry-after-wins", struct{}{}, jobs.Retries(2))
	require.NoError(t, err)
	noRetryID, err := queue.Enqueue(ctx, "no-retry-wins", struct{}{}, jobs.Retries(5))
	require.NoError(t, err)

	before := time.Now()
	worker := queue.NewWorker(jobs.WithPollInterval(50 * time.Millisecond))
	go func() { _ = worker.Start(ctx) }()

	retryAfterJob := waitForRetryRunAt(t, store, retryAfterID, 3*time.Second)
	require.NotNil(t, retryAfterJob.RunAt)
	assert.GreaterOrEqual(t, retryAfterJob.RunAt.Sub(before), 2*time.Second)
	assert.Less(t, retryAfterJob.RunAt.Sub(before), 10*time.Second)

	failed := waitForStatus(t, store, noRetryID, jobs.StatusFailed, 3*time.Second)
	assert.Nil(t, failed.RunAt)
	assert.Equal(t, 1, failed.Attempt)
}

func waitForRetryRunAt(t *testing.T, store jobs.Storage, jobID string, timeout time.Duration) *jobs.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		require.NoError(t, err)
		if job != nil && job.RunAt != nil {
			return job
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %s did not get a retry RunAt", jobID)
	return nil
}

func waitForStatus(t *testing.T, store jobs.Storage, jobID string, status jobs.JobStatus, timeout time.Duration) *jobs.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		require.NoError(t, err)
		if job != nil && job.Status == status {
			return job
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach status %s", jobID, status)
	return nil
}
