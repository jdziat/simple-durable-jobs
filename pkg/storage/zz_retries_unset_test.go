package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// An OMITTED MaxRetries is not a request for zero retries, and the storage layer
// has to be able to tell the difference.
//
// core.Storage is exported and documented, and enqueuing straight through it is a
// supported way to use this library:
//
//	store.Enqueue(ctx, &core.Job{Type: "charge", Queue: "default", Args: args})
//
// That job's MaxRetries is 0 because the caller never mentioned the field. On
// every shipped release it took max_retries = 3 from the column default and the
// handler ran three times on a transient failure. A correction that keys off
// `MaxRetries == 0` alone cannot see the difference between that caller and one
// who wrote `MaxRetries: 0` deliberately, so it silently turns every direct
// storage user's retries off — a job that used to survive a blip now dead-letters
// on the first one.
//
// core.Job.MaxRetriesSet carries the intent that queue.Options already tracked
// internally as retriesSet and had no way to pass down. This test is the "unset"
// half of the contract; TestExplicitZeroRetriesSurvivesEveryEnqueuePath is the
// "explicit zero" half. Both run over the SAME eight paths on purpose: the bug
// this pair guards is a path that honours one and not the other.
func TestOmittedMaxRetriesKeepsTheColumnDefaultOnEveryEnqueuePath(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	// MaxRetries is NOT mentioned, exactly as an application that does not care
	// about retries would write it.
	newJob := func(n string) *core.Job {
		return &core.Job{
			ID: core.NewID(), Type: "charge-unset-" + n, Queue: "default",
			Status: core.StatusPending,
		}
	}

	for i, p := range enqueuePathsUnderTest(ctx, s, db) {
		t.Run(p.name, func(t *testing.T) {
			job := newJob(fmt.Sprintf("%d", i))
			require.Zero(t, job.MaxRetries, "fixture: the field must be left at its zero value")
			require.False(t, job.MaxRetriesSet, "fixture: intent must be absent")

			p.run(t, job)

			var raw int
			require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", job.ID).Scan(&raw).Error)
			require.Equal(t, 3, raw,
				"%s stored max_retries=%d for a job that never mentioned the field; every "+
					"shipped release gave it the column default of 3, and storing 0 turns "+
					"retries OFF for every direct core.Storage caller", p.name, raw)
		})
	}
}

// TestDeliberateNonZeroMaxRetriesRoundTripsOnEveryEnqueuePath is the third leg,
// and it exists because the other two cannot see the correction misfiring.
//
// The corrective UPDATE writes a literal 0. Both the "explicit zero" and the
// "omitted" tests would stay green if its guard checked only the intent flag and
// forgot the value, because neither one enqueues a job that is BOTH deliberate and
// non-zero — and that job is the common case: every single enqueue through
// queue.Enqueue carries MaxRetriesSet with the default 3. Losing the value check
// would silently set max_retries = 0 on essentially every job in the system.
func TestDeliberateNonZeroMaxRetriesRoundTripsOnEveryEnqueuePath(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	for i, p := range enqueuePathsUnderTest(ctx, s, db) {
		t.Run(p.name, func(t *testing.T) {
			job := &core.Job{
				ID: core.NewID(), Type: fmt.Sprintf("charge-seven-%d", i), Queue: "default",
				Status: core.StatusPending, MaxRetries: 7, MaxRetriesSet: true,
			}
			p.run(t, job)

			var raw int
			require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", job.ID).Scan(&raw).Error)
			require.Equal(t, 7, raw,
				"%s stored max_retries=%d for a DELIBERATE 7; the zero-retry correction must "+
					"key off the value as well as the intent, or every job enqueued through "+
					"queue.Enqueue loses its retries", p.name, raw)
		})
	}
}

// TestMaxRetriesSetIsNotAColumn pins that the intent flag stays out of the schema.
//
// core.Job is the AutoMigrate model, so an exported field without `gorm:"-"`
// becomes a COLUMN — and this package's own comments record what that costs on
// SQLite: a changed jobs-table definition makes AutoMigrate rebuild the table and
// take the versioned migrations' indexes with it (measured at 14 before, 4 after).
// A flag that is write-side intent rather than job state must never get there, and
// dropping the tag is a one-character edit.
func TestMaxRetriesSetIsNotAColumn(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))

	cols, err := db.Migrator().ColumnTypes(&core.Job{})
	require.NoError(t, err)
	for _, c := range cols {
		require.NotEqual(t, "max_retries_set", c.Name(),
			"MaxRetriesSet lost its gorm:\"-\" tag and became a column; on SQLite that "+
				"makes AutoMigrate rebuild the jobs table and drop the migration-created indexes")
	}

	// And the flag genuinely does not survive a round trip, which is what the
	// godoc promises callers who re-enqueue a job they read back.
	job := &core.Job{
		ID: core.NewID(), Type: "roundtrip", Queue: "default",
		Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
	}
	require.NoError(t, s.Enqueue(context.Background(), job))
	read, err := s.GetJob(context.Background(), job.ID)
	require.NoError(t, err)
	require.Equal(t, 0, read.MaxRetries, "the deliberate zero is what persists")
	require.False(t, read.MaxRetriesSet,
		"the flag is write-side intent; a job read back must not claim an intent the row cannot carry")
}

// enqueuePathsUnderTest is every public way a job row is INSERTED. The retry-intent
// correction is applied per path, so a new path that forgets it reintroduces the
// bug for that path alone — which is what makes enumerating them worth the
// duplication.
type enqueuePathUnderTest struct {
	name string
	run  func(t *testing.T, job *core.Job)
}

func enqueuePathsUnderTest(ctx context.Context, s *GormStorage, db *gorm.DB) []enqueuePathUnderTest {
	return []enqueuePathUnderTest{
		{"Enqueue", func(t *testing.T, j *core.Job) {
			require.NoError(t, s.Enqueue(ctx, j))
		}},
		{"EnqueueUnique", func(t *testing.T, j *core.Job) {
			require.NoError(t, s.EnqueueUnique(ctx, j, "uk-"+string(j.ID)))
		}},
		{"EnqueueBatch", func(t *testing.T, j *core.Job) {
			require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{j}))
		}},
		{"EnqueueWithUniqueLock", func(t *testing.T, j *core.Job) {
			_, err := s.EnqueueWithUniqueLock(ctx, j, "scope-"+string(j.ID), time.Minute)
			require.NoError(t, err)
		}},
		{"EnqueueTx", func(t *testing.T, j *core.Job) {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error { return s.EnqueueTx(ctx, tx, j) }))
		}},
		{"EnqueueUniqueTx", func(t *testing.T, j *core.Job) {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				return s.EnqueueUniqueTx(ctx, tx, j, "uk-"+string(j.ID))
			}))
		}},
		{"EnqueueBatchTx", func(t *testing.T, j *core.Job) {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				return s.EnqueueBatchTx(ctx, tx, []*core.Job{j})
			}))
		}},
		{"EnqueueWithUniqueLockTx", func(t *testing.T, j *core.Job) {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				_, err := s.EnqueueWithUniqueLockTx(ctx, tx, j, "scope-"+string(j.ID), time.Minute)
				return err
			}))
		}},
	}
}
