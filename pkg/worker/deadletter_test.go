package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

func TestWorkerDeadLetter_MetadataSetAfterRetriesExhausted(t *testing.T) {
	q, cleanup := newDeadLetterSQLiteQueue(t)
	defer cleanup()

	q.Register("dlq.always-fails", func(context.Context, struct{}) error {
		return errors.New("poison payload")
	})

	failed := make(chan error, 1)
	q.OnJobFail(func(_ context.Context, job *core.Job, err error) {
		assert.Equal(t, "dlq.always-fails", job.Type)
		failed <- err
	})

	jobID, err := q.Enqueue(context.Background(), "dlq.always-fails", struct{}{}, queue.Retries(1))
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0), WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case err := <-failed:
		assert.Contains(t, err.Error(), "poison payload")
	case <-time.After(4 * time.Second):
		t.Fatal("exhausted job did not call failure hook")
	}

	job, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusFailed, job.Status)
	require.NotNil(t, job.DeadLetteredAt)
	assert.Contains(t, job.DeadLetterReason, "max retries exhausted: poison payload")
}

func TestWorkerDeadLetter_NoRetryWithAttemptsRemainingIsDeadLettered(t *testing.T) {
	q, cleanup := newDeadLetterSQLiteQueue(t)
	defer cleanup()

	q.Register("dlq.no-retry", func(context.Context, struct{}) error {
		return core.NoRetry(errors.New("invalid input password=secret"))
	})

	failed := make(chan error, 1)
	q.OnJobFail(func(_ context.Context, job *core.Job, err error) {
		assert.Equal(t, "dlq.no-retry", job.Type)
		failed <- err
	})

	jobID, err := q.Enqueue(context.Background(), "dlq.no-retry", struct{}{}, queue.Retries(3))
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0), WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case err := <-failed:
		assert.Contains(t, err.Error(), "invalid input")
	case <-time.After(4 * time.Second):
		t.Fatal("non-retryable job did not call failure hook")
	}

	job, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusFailed, job.Status)
	assert.Less(t, job.Attempt, job.MaxRetries)
	require.NotNil(t, job.DeadLetteredAt)
	assert.Contains(t, job.DeadLetterReason, "non-retryable failure:")
	assert.NotContains(t, job.DeadLetterReason, "max retries exhausted")
	assert.NotContains(t, job.DeadLetterReason, "password=secret")
}

func TestWorkerDeadLetter_RetryingJobNotDeadLetteredYet(t *testing.T) {
	q, cleanup := newDeadLetterSQLiteQueue(t)
	defer cleanup()

	q.Register("dlq.retry-first", func(context.Context, struct{}) error {
		return errors.New("transient")
	})

	retried := make(chan string, 1)
	q.OnRetry(func(_ context.Context, job *core.Job, _ int, _ error) {
		retried <- string(job.ID)
	})

	jobID, err := q.Enqueue(context.Background(), "dlq.retry-first", struct{}{}, queue.Retries(2))
	require.NoError(t, err)

	w := NewWorker(q, WithStaleLockInterval(0), WithPollInterval(50*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	select {
	case gotID := <-retried:
		assert.Equal(t, string(jobID), gotID)
	case <-time.After(4 * time.Second):
		t.Fatal("retryable job did not call retry hook")
	}
	cancel()

	job, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Nil(t, job.DeadLetteredAt)
	assert.Empty(t, job.DeadLetterReason)
}

func newDeadLetterSQLiteQueue(t *testing.T) (*queue.Queue, func()) {
	t.Helper()
	dbFile := t.TempDir() + "/worker-deadletter.db"
	dsn := "file:" + dbFile + "?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("PRAGMA busy_timeout=10000").Error)
	require.NoError(t, db.Exec("PRAGMA synchronous=NORMAL").Error)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store), func() { _ = sqlDB.Close() }
}
