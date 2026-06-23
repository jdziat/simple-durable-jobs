package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

type batchCountingStorage struct {
	*mockStorage
	enqueueCalls       int
	enqueueUniqueCalls int
	enqueueBatchCalls  int
	batchLen           int
	batchJobs          []*core.Job
}

func (s *batchCountingStorage) Enqueue(ctx context.Context, job *core.Job) error {
	s.enqueueCalls++
	return s.mockStorage.Enqueue(ctx, job)
}

func (s *batchCountingStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	s.enqueueUniqueCalls++
	return s.mockStorage.EnqueueUnique(ctx, job, uniqueKey)
}

func (s *batchCountingStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	s.enqueueBatchCalls++
	s.batchLen = len(jobs)
	s.batchJobs = append([]*core.Job(nil), jobs...)
	return s.mockStorage.EnqueueBatch(ctx, jobs)
}

func newQueueBatchTestStorage(t *testing.T) *storage.GormStorage {
	t.Helper()

	var db *gorm.DB
	external := false

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		var err error
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql queue batch test db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		external = true
	} else if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		var err error
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres queue batch test db")

		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		external = true
	} else {
		dbPath := t.TempDir() + "/queue-batch.db"
		var err error
		db, err = gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open sqlite queue batch test db")
	}

	s := storage.NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	if external {
		cleanupQueueBatchExternalDB(t, db)
		t.Cleanup(func() { cleanupQueueBatchExternalDB(t, db) })
	}
	return s
}

func cleanupQueueBatchExternalDB(t *testing.T, db *gorm.DB) {
	t.Helper()

	tables := []string{"checkpoints", "fan_outs", "queue_states", "jobs", "scheduled_fires", "leases"}
	for _, tbl := range tables {
		require.NoError(t, db.Exec("DELETE FROM "+tbl).Error)
	}
}

func TestBatchConstructor(t *testing.T) {
	entry := Batch("email.send", map[string]string{"to": "user@example.com"}, QueueOpt("emails"), Priority(7))

	assert.Equal(t, "email.send", entry.Name)
	assert.Equal(t, map[string]string{"to": "user@example.com"}, entry.Args)
	require.Len(t, entry.Options, 2)
}

func TestQueue_EnqueueBatch_EmptyReturnsEmptySlice(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)

	ids, err := q.EnqueueBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, ids)
	assert.NotNil(t, ids)
	assert.Zero(t, store.enqueueBatchCalls)
}

func TestQueue_EnqueueBatch_UsesSingleStorageBatchCall(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)

	ids, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a"),
		Batch("job.b", map[string]int{"b": 2}, Unique("batch-key-b")),
		Batch("job.c", "c", QueueOpt("critical"), Priority(10), Retries(500), Timeout(30*time.Second)),
	})
	require.NoError(t, err)
	require.Len(t, ids, 3)

	assert.Equal(t, 1, store.enqueueBatchCalls)
	assert.Equal(t, 3, store.batchLen)
	assert.Zero(t, store.enqueueCalls)
	assert.Zero(t, store.enqueueUniqueCalls)
	require.Len(t, store.batchJobs, 3)
	assert.Equal(t, "job.a", store.batchJobs[0].Type)
	assert.Equal(t, "batch-key-b", store.batchJobs[1].UniqueKey)
	assert.Equal(t, "critical", store.batchJobs[2].Queue)
	assert.Equal(t, 10, store.batchJobs[2].Priority)
	assert.Equal(t, 100, store.batchJobs[2].MaxRetries)
	assert.Equal(t, 30*time.Second, store.batchJobs[2].Timeout)
}

func TestQueue_EnqueueBatch_RunsMiddlewarePerJobBeforePersistence(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)
	var seen []string
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		seen = append(seen, job.Type)
		job.Priority += 100
		return next(ctx, job)
	})

	ids, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a", Priority(1)),
		Batch("job.b", "b", Priority(2)),
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)

	assert.Equal(t, []string{"job.a", "job.b"}, seen)
	require.Len(t, store.batchJobs, 2)
	assert.Equal(t, 101, store.batchJobs[0].Priority)
	assert.Equal(t, 102, store.batchJobs[1].Priority)
}

func TestQueue_EnqueueBatch_MiddlewareErrorPersistsNothing(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)
	sentinel := errors.New("reject")
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		if job.Type == "job.b" {
			return sentinel
		}
		return next(ctx, job)
	})

	_, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a"),
		Batch("job.b", "b"),
	})
	require.ErrorIs(t, err, sentinel)
	assert.Zero(t, store.enqueueBatchCalls)
	assert.Empty(t, store.jobs)
}

func TestQueue_EnqueueBatch_BuildErrorPersistsNothing(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a"),
		Batch("job.b", "b", QueueOpt("")),
	})
	require.Error(t, err)
	assert.Zero(t, store.enqueueBatchCalls)
	assert.Empty(t, store.jobs)
}

// F2: EnqueueBatch must reject IdempotencyKey/UniqueFor entries rather than
// silently drop the windowed dedup (it has no batch unique-lock variant — only
// the active-unique UniqueKey works in a batch).
func TestQueue_EnqueueBatch_RejectsWindowedDedupOptions(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a"),
		Batch("job.b", "b", IdempotencyKey("k", time.Minute)),
	})
	require.ErrorIs(t, err, core.ErrBatchWindowedDedup)
	assert.Zero(t, store.enqueueBatchCalls, "nothing persisted when an entry is rejected")
	assert.Empty(t, store.batchJobs)

	_, err = q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a", UniqueFor(time.Minute)),
	})
	require.ErrorIs(t, err, core.ErrBatchWindowedDedup)
	assert.Zero(t, store.enqueueBatchCalls)

	// UniqueKey (active-unique) is still allowed within a batch.
	ids, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a", Unique("uk-1")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, 1, store.enqueueBatchCalls)
}

func TestQueue_EnqueueBatch_ValidatesJobTypeName(t *testing.T) {
	store := &batchCountingStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.a", "a"),
		Batch("has space", "b"),
	})
	require.Error(t, err)
	assert.Zero(t, store.enqueueBatchCalls)
	assert.Empty(t, store.jobs)

	ids, err := q.EnqueueBatch(context.Background(), []BatchEntry{
		Batch("job.valid", "ok"),
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, 1, store.enqueueBatchCalls)
	require.Len(t, store.batchJobs, 1)
	assert.Equal(t, "job.valid", store.batchJobs[0].Type)
}

func TestQueue_EnqueueBatch_DoesNotRequireRegisteredHandlers(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	ids, err := q.EnqueueBatch(ctx, []BatchEntry{
		Batch("remote.a", map[string]string{"a": "1"}),
		Batch("remote.b", map[string]string{"b": "2"}, QueueOpt("remote")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)
	assert.False(t, q.HasHandler("remote.a"))
	assert.False(t, q.HasHandler("remote.b"))

	for _, id := range ids {
		job, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, job)
	}
}

func TestQueue_EnqueueBatch_InsertsRowsAndReturnsPersistedIDs(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)
	runAt := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)

	ids, err := q.EnqueueBatch(ctx, []BatchEntry{
		Batch("batch.a", map[string]string{"a": "1"}, QueueOpt("alpha"), Priority(3)),
		Batch("batch.b", []int{1, 2, 3}, QueueOpt("beta"), At(runAt)),
		Batch("batch.c", "payload", Delay(time.Hour), Timeout(10*time.Second), Determinism(BestEffort)),
	})
	require.NoError(t, err)
	require.Len(t, ids, 3)

	for _, id := range ids {
		job, err := store.GetJob(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, id, job.ID)
		assert.Equal(t, core.StatusPending, job.Status)
	}

	var count int64
	require.NoError(t, store.DB().Model(&core.Job{}).Where("id IN ?", ids).Count(&count).Error)
	assert.EqualValues(t, 3, count)
}

func TestQueue_EnqueueBatch_DuplicateUniqueKeyInOneBatchReturnsInputLengthAndInsertsOneRow(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	ids, err := q.EnqueueBatch(ctx, []BatchEntry{
		Batch("batch.unique", "first", Unique("same-key")),
		Batch("batch.unique", "second", Unique("same-key")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)
	assert.Equal(t, ids[0], ids[1])

	job, err := store.GetJob(ctx, ids[0])
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "same-key", job.UniqueKey)

	var count int64
	require.NoError(t, store.DB().Model(&core.Job{}).Where("unique_key = ?", "same-key").Count(&count).Error)
	assert.EqualValues(t, 1, count)
}

func TestQueue_EnqueueBatch_CrossCallUniqueCollisionReturnsExistingID(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)
	q.Register("batch.seed", func(context.Context, string) error { return nil })

	firstID, err := q.Enqueue(ctx, "batch.seed", "first", Unique("cross-call-key"))
	require.NoError(t, err)

	ids, err := q.EnqueueBatch(ctx, []BatchEntry{
		Batch("batch.duplicate", "second", Unique("cross-call-key")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, firstID, ids[0])

	status, err := q.LoadStatus(ctx, ids[0])
	require.NoError(t, err)
	assert.Equal(t, core.StatusPending, status)
}

func TestQueue_EnqueueBatch_UniqueKeyCompletedJobDoesNotDedup(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)
	q.Register("batch.seed.completed", func(context.Context, string) error { return nil })

	firstID, err := q.Enqueue(ctx, "batch.seed.completed", "first", Unique("completed-key"))
	require.NoError(t, err)
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.Equal(t, firstID, dequeued.ID)
	require.NoError(t, store.Complete(ctx, dequeued.ID, "worker-1"))

	ids, err := q.EnqueueBatch(ctx, []BatchEntry{
		Batch("batch.after.completed", "second", Unique("completed-key")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.NotEqual(t, firstID, ids[0])

	job, err := store.GetJob(ctx, ids[0])
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusPending, job.Status)
}

func TestQueue_EnqueueBatchTx_ValidatesJobTypeName(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	tx := store.DB().Begin()
	require.NoError(t, tx.Error)
	_, err := q.EnqueueBatchTx(ctx, tx, []BatchEntry{
		Batch("batch.valid", "a"),
		Batch("has space", "b"),
	})
	require.Error(t, err)
	require.NoError(t, tx.Rollback().Error)

	tx = store.DB().Begin()
	require.NoError(t, tx.Error)
	ids, err := q.EnqueueBatchTx(ctx, tx, []BatchEntry{
		Batch("batch.tx.valid", "ok"),
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.NoError(t, tx.Commit().Error)

	job, err := store.GetJob(ctx, ids[0])
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "batch.tx.valid", job.Type)
}
