package jobs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// dbCounter provides unique database names so each test gets an isolated
// in-memory SQLite database rather than the shared cache used by facade_test.go.
var dbCounter uint64

func setupIsolatedStorage(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	n := atomic.AddUint64(&dbCounter, 1)
	dsn := fmt.Sprintf("file:pause_test_%d?mode=memory&cache=private", n)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))

	q := jobs.New(store)
	return q, store
}

// ---------------------------------------------------------------------------
// PauseJob / IsJobPaused / ResumeJob lifecycle
// ---------------------------------------------------------------------------

func TestPauseJob_PausesJob(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	ctx := context.Background()

	q.Register("pause-test-job", func(_ context.Context, _ string) error { return nil })
	id, err := q.Enqueue(ctx, "pause-test-job", "hello")
	require.NoError(t, err)

	err = jobs.PauseJob(ctx, store, id)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, id)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestResumeJob_ResumesJob(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	ctx := context.Background()

	q.Register("resume-test-job", func(_ context.Context, _ string) error { return nil })
	id, err := q.Enqueue(ctx, "resume-test-job", "hello")
	require.NoError(t, err)

	// Pause first, then resume.
	require.NoError(t, jobs.PauseJob(ctx, store, id))

	err = jobs.ResumeJob(ctx, store, id)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, id)
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestIsJobPaused_FalseWhenNotPaused(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	ctx := context.Background()

	q.Register("not-paused-job", func(_ context.Context, _ int) error { return nil })
	id, err := q.Enqueue(ctx, "not-paused-job", 1)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, id)
	require.NoError(t, err)
	assert.False(t, paused)
}

// ---------------------------------------------------------------------------
// GetPausedJobs
// ---------------------------------------------------------------------------

func TestGetPausedJobs_ReturnsPausedJobIDs(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	ctx := context.Background()

	q.Register("gpj-job", func(_ context.Context, _ string) error { return nil })

	id1, err := q.Enqueue(ctx, "gpj-job", "a", jobs.QueueOpt("emails"))
	require.NoError(t, err)
	id2, err := q.Enqueue(ctx, "gpj-job", "b", jobs.QueueOpt("emails"))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, "gpj-job", "c", jobs.QueueOpt("emails"))
	require.NoError(t, err)

	require.NoError(t, jobs.PauseJob(ctx, store, id1))
	require.NoError(t, jobs.PauseJob(ctx, store, id2))

	paused, err := jobs.GetPausedJobs(ctx, store, "emails")
	require.NoError(t, err)
	require.Len(t, paused, 2)

	ids := make([]string, 0, len(paused))
	for _, j := range paused {
		ids = append(ids, j.ID)
	}
	assert.Contains(t, ids, id1)
	assert.Contains(t, ids, id2)
}

func TestGetPausedJobs_EmptyWhenNonePaused(t *testing.T) {
	q, store := setupIsolatedStorage(t)
	ctx := context.Background()

	q.Register("gpj-none-job", func(_ context.Context, _ string) error { return nil })
	_, err := q.Enqueue(ctx, "gpj-none-job", "x", jobs.QueueOpt("myqueue"))
	require.NoError(t, err)

	paused, err := jobs.GetPausedJobs(ctx, store, "myqueue")
	require.NoError(t, err)
	assert.Empty(t, paused)
}

func TestGetPausedJobs_InvalidQueueName_ReturnsError(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	_, err := jobs.GetPausedJobs(ctx, store, "")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// PauseQueue / IsQueuePaused / ResumeQueue lifecycle
// ---------------------------------------------------------------------------

func TestPauseQueue_PausesQueue(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	err := jobs.PauseQueue(ctx, store, "notifications")
	require.NoError(t, err)

	paused, err := jobs.IsQueuePaused(ctx, store, "notifications")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestResumeQueue_ResumesQueue(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	require.NoError(t, jobs.PauseQueue(ctx, store, "notifications"))

	err := jobs.ResumeQueue(ctx, store, "notifications")
	require.NoError(t, err)

	paused, err := jobs.IsQueuePaused(ctx, store, "notifications")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestIsQueuePaused_FalseWhenNotPaused(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	paused, err := jobs.IsQueuePaused(ctx, store, "default")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestPauseQueue_InvalidQueueName_ReturnsError(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	err := jobs.PauseQueue(ctx, store, "")
	assert.Error(t, err)
}

func TestResumeQueue_InvalidQueueName_ReturnsError(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	err := jobs.ResumeQueue(ctx, store, "")
	assert.Error(t, err)
}

func TestIsQueuePaused_InvalidQueueName_ReturnsError(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	_, err := jobs.IsQueuePaused(ctx, store, "")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// GetPausedQueues
// ---------------------------------------------------------------------------

func TestGetPausedQueues_ReturnsPausedQueueNames(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	require.NoError(t, jobs.PauseQueue(ctx, store, "alpha"))
	require.NoError(t, jobs.PauseQueue(ctx, store, "beta"))

	queues, err := jobs.GetPausedQueues(ctx, store)
	require.NoError(t, err)
	assert.Contains(t, queues, "alpha")
	assert.Contains(t, queues, "beta")
}

func TestGetPausedQueues_EmptyWhenNonePaused(t *testing.T) {
	_, store := setupIsolatedStorage(t)
	ctx := context.Background()

	queues, err := jobs.GetPausedQueues(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

// ---------------------------------------------------------------------------
// WithPauseMode option
// ---------------------------------------------------------------------------

func TestWithPauseMode_ReturnsNonNilOption(t *testing.T) {
	opt := jobs.WithPauseMode(jobs.PauseModeGraceful)
	assert.NotNil(t, opt)
}

func TestWithPauseMode_GracefulAndAggressive(t *testing.T) {
	// Both modes should produce valid, non-nil options.
	graceful := jobs.WithPauseMode(jobs.PauseModeGraceful)
	assert.NotNil(t, graceful)

	aggressive := jobs.WithPauseMode(jobs.PauseModeAggressive)
	assert.NotNil(t, aggressive)
}

func TestWithPauseMode_ConstantsAreDistinct(t *testing.T) {
	assert.NotEqual(t, jobs.PauseModeGraceful, jobs.PauseModeAggressive)
}
