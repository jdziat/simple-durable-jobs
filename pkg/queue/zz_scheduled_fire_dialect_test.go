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
	"gorm.io/gorm/clause"
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
	// FAIL when BOTH are set rather than silently preferring one. The MySQL
	// gap-lock deadlock test in this file guards a hazard that exists ONLY on
	// InnoDB — run it against Postgres and it passes while proving nothing. CI is
	// safe (each backend leg sets exactly one DSN and unsets the other), but the
	// repo's own local workflow exports both, so a maintainer verifying that fix by
	// hand would get a green that means nothing. That is the false-green shape this
	// whole file exists to close.
	if os.Getenv("TEST_DATABASE_URL") != "" && os.Getenv("TEST_MYSQL_URL") != "" {
		t.Fatal("both TEST_DATABASE_URL and TEST_MYSQL_URL are set: this file's tests are " +
			"dialect-specific and would silently run against Postgres only. Unset one.")
	}
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

// TestEnqueueScheduledFire_PriorFireReadTakesTheRowLock covers the FOR UPDATE on
// the prior-fire read, which had NO coverage at all: deleting the clause left
// ./pkg/queue green on SQLite, live Postgres AND live MySQL.
//
// FALSE-GREEN TRAP, and the test above is it. Its message says "FOR UPDATE must be
// valid SQL on %s" — but it only ever asserts that the call SUCCEEDS, and a query
// with no locking clause succeeds too. It cannot tell "the lock is taken" from
// "the clause was never emitted", which is precisely the difference that matters.
//
// The discriminating setup is a fire the cursor guard will REJECT. On that path
// ClaimScheduledFireTx's UPDATE matches no row and therefore takes no lock of its
// own, so the ONLY thing that can block is the explicit locking read. Holding a
// competing row lock from the test then separates the two cases exactly: with the
// clause the call blocks until we commit, without it the call sails through.
//
// The property is what the comment in queue.go claims — that the read takes the
// SAME row lock the claim is about to take, rather than reading first and locking
// after, which would leave a window in which a peer's real fire is invisible and
// the skip-restore writes a marker from before it.
//
// WHERE THIS TEST DISCRIMINATES, MEASURED, because "it passes on both dialects"
// is not the same as "it covers both dialects":
//
//	Postgres — DISCRIMINATING. Drop the locking clause and this test fails: PG
//	  locks only rows an UPDATE actually modifies, so a rejected claim takes no
//	  lock and the call returns immediately.
//	MySQL — NOT DISCRIMINATING, and the test still passes with the clause removed.
//	  InnoDB locks rows it SCANS, not just rows it modifies, so the claim's own
//	  UPDATE blocks on the held lock regardless. I confirmed this by removing the
//	  FOR UPDATE and the ODKU pre-insert together and watching it still block, which
//	  leaves the claim's UPDATE as the only statement that can be taking the lock.
//	  So on MySQL the clause is belt-and-braces; the Postgres leg is what pins it.
//
// That asymmetry is the reason this is written as a lock-visibility test rather
// than a concurrent read-modify-write race: the race needs an interleave inside a
// closure in EnqueueScheduledFire that no external caller can schedule, and a
// hammer-and-hope version would be both flaky and — on MySQL — vacuous.
func TestEnqueueScheduledFire_PriorFireReadTakesTheRowLock(t *testing.T) {
	db := openRealDialectDB(t)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := New(store)
	q.Register("probejob", func(context.Context, struct{}) error { return nil })
	ctx := context.Background()

	name := "lockprobe-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduled_fires WHERE name = ?`, name) })

	// Establish the cursor so the fire below is REJECTED, which is what keeps the
	// claim's own UPDATE from taking a lock and muddying the signal.
	boundary := time.Now().Truncate(time.Minute)
	claimed, _, err := q.EnqueueScheduledFire(ctx, name, boundary, "probejob", nil)
	require.NoError(t, err)
	require.True(t, claimed)

	// Hold a conflicting row lock in a transaction of our own.
	//
	// The rollback is registered BEFORE anything can fail. Without it, a tripped
	// require below would FailNow with this transaction still open, holding a
	// FOR UPDATE lock on scheduled_fires — and the cleanup DELETE on that same row,
	// plus every other test sharing this database, would then block until the
	// process exited. Rollback after a commit is a harmless no-op.
	held := db.Begin()
	require.NoError(t, held.Error)
	t.Cleanup(func() { _ = held.Rollback() })
	var locked core.ScheduledFire
	require.NoError(t, held.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("name = ?", name).First(&locked).Error)

	type result struct {
		claimed bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		// An EARLIER boundary: the cursor guard rejects it, so no UPDATE matches.
		c, _, e := q.EnqueueScheduledFire(ctx, name, boundary.Add(-time.Minute), "probejob", nil)
		done <- result{c, e}
	}()

	select {
	case r := <-done:
		held.Rollback()
		t.Fatalf("the prior-fire read completed (claimed=%v err=%v) while another transaction "+
			"held this row's lock — it is not taking the row lock, so a peer committing a real "+
			"fire between the read and the claim stays invisible and the skip-restore loses it",
			r.claimed, r.err)
	case <-time.After(750 * time.Millisecond):
		// Blocked, which is the point.
	}

	require.NoError(t, held.Commit().Error)

	select {
	case r := <-done:
		require.NoError(t, r.err)
		assert.False(t, r.claimed, "an earlier boundary must still be rejected by the cursor guard")
	case <-time.After(15 * time.Second):
		t.Fatal("the fire never completed after the competing lock was released")
	}
}
