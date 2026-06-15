package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

func newPromoterSQLiteQueue(t *testing.T) *queue.Queue {
	t.Helper()
	dbFile := t.TempDir() + "/worker-promoter.db"
	dsn := "file:" + dbFile + "?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.Exec("PRAGMA busy_timeout=10000").Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sqlDB.Close() })

	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store)
}

// A delayed job is inserted dq_ready=false (future run_at), so Dequeue — which
// now requires dq_ready=true — cannot pick it up until the ready-promoter flips
// it once run_at passes. If the job runs, the promoter loop must have run: this
// is an end-to-end proof of the backstop wiring, not just of dq_eligible_at.
func TestWorker_ReadyPromoter_RunsDelayedJob(t *testing.T) {
	q := newPromoterSQLiteQueue(t)

	var ran atomic.Bool
	q.Register("delayed", func(_ context.Context, _ map[string]string) error {
		ran.Store(true)
		return nil
	})

	runAt := time.Now().Add(400 * time.Millisecond)
	_, err := q.Enqueue(context.Background(), "delayed", map[string]string{}, queue.At(runAt))
	require.NoError(t, err)

	w := NewWorker(q,
		WithReadyPromoteInterval(40*time.Millisecond),
		WithPollInterval(20*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, ran.Load, 3*time.Second, 25*time.Millisecond,
		"delayed job should run once the ready-promoter makes it dequeue-visible")
}
