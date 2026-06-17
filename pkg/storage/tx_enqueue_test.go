package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func newTxEnqueueTestStorage(t *testing.T) *GormStorage {
	t.Helper()

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql tx enqueue test db")
		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		cleanupExternalDB(t, db)
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		s := NewGormStorage(db)
		require.NoError(t, s.Migrate(context.Background()))
		return s
	}

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres tx enqueue test db")
		sqlDB, err := db.DB()
		require.NoError(t, err, "get underlying sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		cleanupExternalDB(t, db)
		t.Cleanup(func() {
			cleanupExternalDB(t, db)
			_ = sqlDB.Close()
		})
		s := NewGormStorage(db)
		require.NoError(t, s.Migrate(context.Background()))
		return s
	}

	dbPath := t.TempDir() + "/tx-enqueue.db"
	db, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open sqlite tx enqueue test db")
	sqlDB, err := db.DB()
	require.NoError(t, err, "get underlying sql.DB")
	sqlDB.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = sqlDB.Close() })

	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	return s
}

func TestGormStorage_EnqueueTx_CommitAndRollbackVisibility(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)

	t.Run("commit", func(t *testing.T) {
		tx := s.DB().Begin()
		require.NoError(t, tx.Error)
		job := &core.Job{Type: "tx.commit"}
		require.NoError(t, s.EnqueueTx(ctx, tx, job))

		before, err := s.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.Nil(t, before)

		require.NoError(t, tx.Commit().Error)

		after, err := s.GetJob(ctx, job.ID)
		require.NoError(t, err)
		require.NotNil(t, after)
		assert.Equal(t, core.StatusPending, after.Status)

		dequeued, err := s.Dequeue(ctx, []string{"default"}, "worker")
		require.NoError(t, err)
		require.NotNil(t, dequeued)
		assert.Equal(t, job.ID, dequeued.ID)
	})

	t.Run("rollback", func(t *testing.T) {
		tx := s.DB().Begin()
		require.NoError(t, tx.Error)
		job := &core.Job{Type: "tx.rollback"}
		require.NoError(t, s.EnqueueTx(ctx, tx, job))
		require.NoError(t, tx.Rollback().Error)

		after, err := s.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.Nil(t, after)
	})
}

func TestGormStorage_EnqueueUniqueTx_DuplicateActiveJob(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)

	tx1 := s.DB().Begin()
	require.NoError(t, tx1.Error)
	require.NoError(t, s.EnqueueUniqueTx(ctx, tx1, &core.Job{Type: "tx.unique"}, "tx-key"))
	require.NoError(t, tx1.Commit().Error)

	tx2 := s.DB().Begin()
	require.NoError(t, tx2.Error)
	err := s.EnqueueUniqueTx(ctx, tx2, &core.Job{Type: "tx.unique"}, "tx-key")
	require.ErrorIs(t, err, core.ErrDuplicateJob)
	require.NoError(t, tx2.Rollback().Error)
}

func TestGormStorage_EnqueueBatchTx_DeduplicatesCommittedAndInBatchKeys(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)

	seed := &core.Job{Type: "seed", UniqueKey: "existing-key"}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{seed}))

	tx := s.DB().Begin()
	require.NoError(t, tx.Error)
	jobs := []*core.Job{
		{Type: "skip", UniqueKey: "existing-key"},
		{Type: "create", UniqueKey: "new-key"},
		{Type: "skip-in-batch", UniqueKey: "new-key"},
	}
	require.NoError(t, s.EnqueueBatchTx(ctx, tx, jobs))
	require.NoError(t, tx.Commit().Error)

	assert.Equal(t, seed.ID, jobs[0].ID)
	assert.Equal(t, jobs[1].ID, jobs[2].ID)

	existing, err := s.GetJob(ctx, jobs[0].ID)
	require.NoError(t, err)
	require.NotNil(t, existing)

	var total int64
	require.NoError(t, s.DB().Model(&core.Job{}).Where("unique_key IN ?", []string{"existing-key", "new-key"}).Count(&total).Error)
	assert.EqualValues(t, 2, total)

	var newKeyCount int64
	require.NoError(t, s.DB().Model(&core.Job{}).Where("unique_key = ?", "new-key").Count(&newKeyCount).Error)
	assert.EqualValues(t, 1, newKeyCount)
}

func TestGormStorage_EnqueueTx_Defaults(t *testing.T) {
	ctx := context.Background()
	s := newTxEnqueueTestStorage(t)
	tx := s.DB().Begin()
	require.NoError(t, tx.Error)

	job := &core.Job{Type: "tx.defaults"}
	require.NoError(t, s.EnqueueTx(ctx, tx, job))
	require.NoError(t, tx.Commit().Error)

	assert.NotEmpty(t, job.ID)
	assert.Equal(t, core.StatusPending, job.Status)
	assert.Equal(t, "default", job.Queue)
}

func TestTxEnqueueGodocMentionsMySQLSerializationRetry(t *testing.T) {
	src, err := os.ReadFile("tx_enqueue.go")
	require.NoError(t, err)
	text := string(src)

	for _, method := range []string{"EnqueueTx", "EnqueueUniqueTx", "EnqueueBatchTx"} {
		funcIdx := strings.Index(text, "func (s *GormStorage) "+method)
		require.NotEqualf(t, -1, funcIdx, "missing %s implementation", method)
		docStart := strings.LastIndex(text[:funcIdx], "// "+method)
		require.NotEqualf(t, -1, docStart, "missing %s godoc", method)
		doc := text[docStart:funcIdx]
		assert.Contains(t, doc, "Under MySQL")
		assert.Contains(t, doc, "MUST wrap the owning transaction")
		assert.Contains(t, doc, "error 1213")
		assert.Contains(t, doc, "GormStorage.WithSerializationRetry")
	}
}
