package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Exercises the SELECT ... FOR UPDATE on the real dialects. The package's other
// scheduled-fire tests are SQLite-only, where the lock clause is deliberately
// skipped — so without this, an invalid FOR UPDATE would ship untested.
// openRealDialectDB opens the Postgres or MySQL under test, skipping on SQLite:
// the lock semantics these tests exist for only exist there.
func openRealDialectDB(t *testing.T) *gorm.DB {
	t.Helper()
	var db *gorm.DB
	var err error
	switch {
	case os.Getenv("TEST_DATABASE_URL") != "":
		db, err = gorm.Open(postgres.Open(os.Getenv("TEST_DATABASE_URL")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	case os.Getenv("TEST_MYSQL_URL") != "":
		db, err = gorm.Open(mysql.Open(os.Getenv("TEST_MYSQL_URL")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	default:
		t.Skip("needs a real dialect")
	}
	require.NoError(t, err)
	return db
}

func TestEnqueueScheduledFire_DedupRestoreOnRealDialects(t *testing.T) {
	db := openRealDialectDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := New(store)
	q.Register("probejob", func(context.Context, struct{}) error { return nil })
	ctx := context.Background()

	name := "probe-sched-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduled_fires WHERE name = ?`, name) })

	// Real fire, then a dedup-skip on the next boundary.
	b1 := time.Now().Truncate(time.Minute)
	claimed, _, err := q.EnqueueScheduledFire(ctx, name, b1, "probejob", nil, Unique(name+"-key"))
	require.NoError(t, err, "FOR UPDATE must be valid SQL on %s", db.Name())
	require.True(t, claimed)

	claimed2, id2, err2 := q.EnqueueScheduledFire(ctx, name, b1.Add(time.Minute), "probejob", nil, Unique(name+"-key"))
	require.ErrorIs(t, err2, core.ErrDuplicateJob)
	assert.True(t, claimed2)
	assert.Equal(t, core.NilUUID, id2)

	var row core.ScheduledFire
	require.NoError(t, db.Where("name = ?", name).First(&row).Error)
	require.NotNil(t, row.LastFiredAt)
	assert.True(t, row.LastFiredAt.UTC().Truncate(time.Second).Equal(b1.UTC().Truncate(time.Second)),
		"the real-fire marker must stay at the last ACTUAL fire on %s", db.Name())
	assert.True(t, row.LastFireAt.After(*row.LastFiredAt), "the cursor must have advanced past it")
}

// TestEnqueueScheduledFire_ConcurrentFreshSchedulesDoNotDeadlock is the test the
// single-threaded one above could not be.
//
// The FOR UPDATE on the prior-fire read was added to close a lock-free
// read-modify-write. On MySQL under REPEATABLE READ — the default, which this
// project does not override — a locking read for a key that does NOT exist takes a
// next-key/GAP lock. Two transactions firing two different schedule names that
// land in the same InnoDB gap each lock that gap, then each need an
// insert-intention lock inside it: deadlock, retries exhausted, and BOUNDARIES
// SILENTLY LOST — a scheduled run no worker claimed and nobody will retry.
//
// Measured before the fix, 12 goroutines x 20 fresh names on live MySQL 8.0.42:
// 6 of 9 runs produced deadlock storms and 4 of those claimed only 6-9 of 20.
//
// FALSE-GREEN TRAPS, both of which the neighbouring test falls into:
//   - single-threaded: proves the FOR UPDATE parses, never exercises its locking;
//   - a schedule whose row already EXISTS: the read then takes a record lock and
//     there is no gap to fight over. The names must be fresh.
func TestEnqueueScheduledFire_ConcurrentFreshSchedulesDoNotDeadlock(t *testing.T) {
	db := openRealDialectDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := New(store)
	q.Register("probejob", func(context.Context, struct{}) error { return nil })

	// Tuned for a reliable RED, not for speed. The hazard needs two fresh names in
	// one InnoDB gap with their transactions overlapping, so both the name count
	// and the parallelism matter: at 20 names x 12 workers the pre-fix code failed
	// about one run in five, which is too weak to guard anything. Several rounds at
	// higher parallelism make it near-certain while staying well under a second.
	const names = 60
	const workers = 24
	const rounds = 3
	prefix := "dl-" + time.Now().Format("150405.000000") + "-"
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduled_fires WHERE name LIKE ?`, prefix+"%") })

	boundary := time.Now().Truncate(time.Minute)
	var claimed, failed atomic.Int64
	for round := range rounds {
		work := make(chan int, names)
		for i := range names {
			work <- i
		}
		close(work)

		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					name := fmt.Sprintf("%s%d-%03d", prefix, round, i)
					won, _, err := q.EnqueueScheduledFire(context.Background(), name, boundary, "probejob", nil)
					switch {
					case err != nil:
						failed.Add(1)
						t.Logf("EnqueueScheduledFire(%s): %v", name, err)
					case won:
						claimed.Add(1)
					}
				}
			}()
		}
		wg.Wait()
	}

	assert.Zero(t, failed.Load(),
		"a fresh schedule name must not deadlock against another fresh name: on MySQL a locking "+
			"read for an absent key takes a GAP lock, and two names in one gap deadlock")
	assert.Equal(t, int64(names*rounds), claimed.Load(),
		"every boundary must be claimed exactly once — a lost boundary is a scheduled run that "+
			"nobody ran and nobody will retry")
}
