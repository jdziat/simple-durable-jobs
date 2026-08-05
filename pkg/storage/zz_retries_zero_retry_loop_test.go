package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// A serialization retry must not eat jobs.Retries(0).
//
// TestExplicitZeroRetriesSurvivesEveryEnqueuePath walks the same eight paths but
// lets every insert succeed on its FIRST attempt, and that is exactly the case
// the defect pinned here does not touch. The intent capture reads
// core.Job.MaxRetries; GORM substitutes the column's declared `default:3` for a
// zero and writes the substituted value BACK into the struct it was handed — and
// it does so even when the statement then FAILS and the transaction rolls back.
// So a second attempt re-reads 3, `MaxRetriesSet && MaxRetries == 0` is false,
// the corrective UPDATE never arms, the row commits max_retries=3, and a handler
// marked do-not-retry runs three times while the caller is handed a nil error.
//
// All eight paths are reachable this way. Five retry internally
// (withSerializationRetry); the Tx forms document that the CALLER must wrap the
// owning transaction in WithSerializationRetry, which re-enters the function and
// re-runs the capture against the already-clobbered struct. SQLITE_BUSY, MySQL
// 1213 and Postgres 40001 are all documented in this package as EXPECTED
// transient outcomes, not exotic ones.
//
// The premise is asserted, not assumed: the harness counts `jobs` inserts and
// fails the case if no retry actually happened, so it cannot pass or fail
// vacuously.
func TestExplicitZeroRetriesSurvivesASerializationRetryOnEveryEnqueuePath(t *testing.T) {
	for i, p := range retriedEnqueuePathsUnderTest(context.Background(), nil, nil) {
		name, idx := p.name, i
		t.Run(name, func(t *testing.T) {
			db := openTestDB(t)
			s := NewGormStorage(db)
			require.NoError(t, s.Migrate(context.Background()))
			ctx := context.Background()

			inserts := failNthJobInsert(t, db, 1)

			job := &core.Job{
				ID: core.NewID(), Type: fmt.Sprintf("charge-%d", idx), Queue: "default",
				Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
			}

			path := retriedEnqueuePathsUnderTest(ctx, s, db)[idx]
			require.NoError(t, s.WithSerializationRetry(ctx, func() error {
				return path.run(job)
			}))

			require.Greater(t, *inserts, 1,
				"PREMISE: the fault injector never fired, so this case proves nothing")

			var raw int
			require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", job.ID).Scan(&raw).Error)
			require.Equal(t, 0, raw,
				"%s lost an explicit Retries(0) across a serialization retry: the row persists "+
					"max_retries=%d, so a do-not-retry handler runs that many times", name, raw)
		})
	}
}

// The dq_ready restore has the identical shape: it too is derived from the
// caller's struct inside the retried transaction, and GORM substitutes dq_ready's
// declared `default:true` for a delayed job's false in the same write-back. A
// retried enqueue of a future-dated job therefore committed dq_ready=true.
//
// That one is a performance hint rather than a correctness gate (Dequeue still
// fences on dq_eligible_at), so it costs latency, not data — but it is the SAME
// aliasing, and pinning it is what stops a fix that shields only max_retries.
func TestDelayedEnqueueKeepsDQReadyFalseAcrossASerializationRetry(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	inserts := failNthJobInsert(t, db, 1)

	runAt := time.Now().Add(time.Hour)
	job := &core.Job{
		ID: core.NewID(), Type: "later", Queue: "default",
		Status: core.StatusPending, RunAt: &runAt,
	}
	require.NoError(t, s.EnqueueBatch(ctx, []*core.Job{job}))
	require.Greater(t, *inserts, 1, "PREMISE: the fault injector never fired")

	var ready bool
	require.NoError(t, db.Raw("SELECT dq_ready FROM jobs WHERE id = ?", job.ID).Scan(&ready).Error)
	require.False(t, ready,
		"a future-dated job committed dq_ready=true after a serialization retry")
}

// SuspendForFanOut inserts its children LAST — after the fan_outs row, the
// checkpoint and the ownership-checked parent flip — so a serialization failure
// landing on the children is the case that survives all three. It gets its own
// case rather than riding the table above.
func TestSuspendForFanOutKeepsRetriesZeroAcrossASerializationRetry(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	until := time.Now().Add(time.Minute)
	parentID := core.NewID()
	require.NoError(t, db.Create(&core.Job{
		ID: parentID, Type: "wf.run", Queue: "default", Status: core.StatusRunning,
		LockedBy: "w1", LockedUntil: &until,
	}).Error)

	// Registered AFTER the parent is in, so insert #1 is the CHILDREN's — the one
	// that lands past the fan_outs row, the checkpoint and the parent flip.
	inserts := failNthJobInsert(t, db, 1)

	foID := core.NewID()
	child := &core.Job{
		ID: core.NewID(), Type: "wf.sub", Queue: "default", FanOutID: &foID,
		MaxRetries: 0, MaxRetriesSet: true,
	}
	fanOut := &core.FanOut{ID: foID, ParentJobID: parentID, TotalCount: 1}
	cp := &core.Checkpoint{JobID: parentID, CallIndex: 0, CallType: "fanout", Result: []byte(`{}`)}

	require.NoError(t, s.SuspendForFanOut(ctx, parentID, "w1", fanOut, cp, []*core.Job{child}))
	require.Greater(t, *inserts, 1, "PREMISE: the fault injector never forced a child retry")

	var raw int
	require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", child.ID).Scan(&raw).Error)
	require.Equal(t, 0, raw,
		"SuspendForFanOut lost a sub-job's Retries(0) across a serialization retry")
}

// The invariant the three tests above rest on, asserted directly so a regression
// names its own cause instead of surfacing as a puzzling max_retries=3.
//
// GORM owns the struct it is handed: it substitutes declared column defaults into
// it while building the INSERT and leaves them there when the statement fails.
// The row builder must therefore hand it something the CALLER does not own — on
// every dialect and under every codec, including the identity codec, which is the
// default and which used to short-circuit to `return job, nil`.
func TestRowForCreateNeverHandsGORMTheCallersJob(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))

	job := &core.Job{
		ID: core.NewID(), Type: "charge", Queue: "default", Status: core.StatusPending,
		Args: []byte(`{"a":1}`), MaxRetries: 0, MaxRetriesSet: true, DQReady: false,
	}

	row, err := s.encodedJobForCreate(job)
	require.NoError(t, err)
	require.NotSame(t, job, row, "the row handed to GORM aliases the caller's job")

	// Prove the shield holds against the actual mutation, not merely against
	// pointer identity: a FAILED create must leave the caller's intent readable.
	require.Error(t, db.Table("zz_no_such_table_r30").Create(row).Error)
	require.Equal(t, 0, job.MaxRetries, "GORM's default substitution reached the caller's job")
	require.True(t, job.MaxRetriesSet)
	require.False(t, job.DQReady, "GORM's default substitution reached the caller's dq_ready")
	require.Equal(t, []byte(`{"a":1}`), job.Args, "the row builder mutated the caller's args")
}

// --- harness ---

// retriedEnqueuePathsUnderTest mirrors enqueuePathsUnderTest, but each entry
// returns its error instead of asserting, so a transient failure on attempt one
// can be handed back to WithSerializationRetry rather than ending the test.
type retriedEnqueuePath struct {
	name string
	run  func(*core.Job) error
}

func retriedEnqueuePathsUnderTest(ctx context.Context, s *GormStorage, db *gorm.DB) []retriedEnqueuePath {
	return []retriedEnqueuePath{
		{"Enqueue", func(j *core.Job) error { return s.Enqueue(ctx, j) }},
		{"EnqueueUnique", func(j *core.Job) error { return s.EnqueueUnique(ctx, j, "uk-"+string(j.ID)) }},
		{"EnqueueBatch", func(j *core.Job) error { return s.EnqueueBatch(ctx, []*core.Job{j}) }},
		{"EnqueueWithUniqueLock", func(j *core.Job) error {
			_, err := s.EnqueueWithUniqueLock(ctx, j, "scope-"+string(j.ID), time.Minute)
			return err
		}},
		{"EnqueueTx", func(j *core.Job) error {
			return db.Transaction(func(tx *gorm.DB) error { return s.EnqueueTx(ctx, tx, j) })
		}},
		{"EnqueueUniqueTx", func(j *core.Job) error {
			return db.Transaction(func(tx *gorm.DB) error {
				return s.EnqueueUniqueTx(ctx, tx, j, "uk-"+string(j.ID))
			})
		}},
		{"EnqueueBatchTx", func(j *core.Job) error {
			return db.Transaction(func(tx *gorm.DB) error {
				return s.EnqueueBatchTx(ctx, tx, []*core.Job{j})
			})
		}},
		{"EnqueueWithUniqueLockTx", func(j *core.Job) error {
			return db.Transaction(func(tx *gorm.DB) error {
				_, err := s.EnqueueWithUniqueLockTx(ctx, tx, j, "scope-"+string(j.ID), time.Minute)
				return err
			})
		}},
	}
}

// failNthJobInsert makes the Nth insert into `jobs` fail with the driver text
// isSerializationFailure classifies as transient. It returns a pointer to the
// live insert count so a caller can assert that a retry really happened.
//
// The callback is registered AFTER "gorm:create" on purpose: GORM's default
// substitution happens while the statement is being built, so by this point the
// struct has already been written to — which is exactly the state a real
// SQLITE_BUSY / 1213 / 40001 leaves behind.
func failNthJobInsert(t *testing.T, db *gorm.DB, n int) *int {
	t.Helper()
	count := 0
	const name = "zz_r30_fail_nth_job_insert"
	require.NoError(t, db.Callback().Create().After("gorm:create").Register(name, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != "jobs" {
			return
		}
		count++
		if count == n {
			_ = tx.AddError(errors.New("database is locked"))
		}
	}))
	t.Cleanup(func() { _ = db.Callback().Create().Remove(name) })
	return &count
}
