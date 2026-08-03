package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestEnqueueUniqueTxDoesNotCorrectARowItDidNotInsert pins the
// `result.RowsAffected > 0` guard on EnqueueUniqueTx, the one enqueue path where
// that guard is actually load-bearing.
//
// WHY IT IS SHAPED LIKE THIS. Both paths insert under OnConflict{DoNothing} and
// then run a corrective `UPDATE ... WHERE id IN (...)` to re-apply an explicit
// Retries(0), because the schema default overrides a bound zero. When the insert
// is suppressed by a PRIMARY KEY conflict the id belongs to a live stranger row,
// so an unguarded corrective UPDATE rewrites that stranger's max_retries — the
// same corruption an adversarial round confirmed on the BATCH path.
//
// The obvious version of this test does NOT work, and the reason is the point.
// Both paths return ErrDuplicateJob immediately after the corrective UPDATE, so
// a caller that rolls back on error never sees the damage — the rollback undoes
// it. Deleting the guard is therefore invisible to any test whose caller aborts,
// which is why dropping it broke nothing on SQLite, Postgres or MySQL. The guard
// is load-bearing only for a caller that treats a duplicate as benign and COMMITS
// anyway ("enqueue unless already queued; duplicates are fine") — a realistic
// outbox shape, and the one modelled here.
//
// Mutation-tested: deleting `&& result.RowsAffected > 0` from either site fails
// this test.
//
// THREE SIBLING PATHS ARE DELIBERATELY NOT COVERED, because on each of them the
// corruption is unreachable and a test would be one that cannot fail:
//   - EnqueueUnique and createUniqueLockedJob own their transaction and return
//     ErrDuplicateJob from inside it, so the rollback is unconditional.
//   - EnqueueTx does not suppress a primary-key collision at all; it surfaces the
//     driver error ("UNIQUE constraint failed: jobs.id"), so the corrective UPDATE
//     is never reached with a suppressed id.
//
// That was established by experiment, not by reading: dropping the guard at each
// of those three sites changes no observable behaviour on SQLite, Postgres or
// MySQL. Only EnqueueUniqueTx both suppresses AND can have its error swallowed by
// the caller, so it is the only site where this guard is load-bearing.
func TestEnqueueUniqueTxDoesNotCorrectARowItDidNotInsert(t *testing.T) {
	const strangerRetries = 7

	cases := []struct {
		name string
		call func(s *GormStorage, ctx context.Context, tx *gorm.DB, job *core.Job) error
	}{
		{"EnqueueUniqueTx", func(s *GormStorage, ctx context.Context, tx *gorm.DB, job *core.Job) error {
			return s.EnqueueUniqueTx(ctx, tx, job, "collide-key-tx")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := newTestStorage(t)

			stranger := &core.Job{
				ID: core.NewID(), Type: "stranger", Queue: "default",
				Status: core.StatusPending, MaxRetries: strangerRetries,
			}
			require.NoError(t, s.Enqueue(ctx, stranger), "seed the stranger row")

			intruder := &core.Job{
				ID: stranger.ID, Type: "intruder", Queue: "other",
				Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
			}

			// The caller swallows ErrDuplicateJob and commits, as an outbox that
			// considers "already enqueued" a success would.
			tx := s.db.Begin()
			require.NoError(t, tx.Error)
			err := tc.call(s, ctx, tx, intruder)
			if err != nil && !errors.Is(err, core.ErrDuplicateJob) {
				tx.Rollback()
				t.Fatalf("%s returned an unexpected error: %v", tc.name, err)
			}
			require.NoError(t, tx.Commit().Error)

			var got core.Job
			require.NoError(t, s.db.Where("id = ?", stranger.ID).First(&got).Error)
			require.Equalf(t, "stranger", got.Type,
				"%s overwrote a live row instead of being suppressed", tc.name)
			require.Equalf(t, strangerRetries, got.MaxRetries,
				"%s applied the intruder's Retries(0) to a row it did not insert: the corrective "+
					"UPDATE ran unguarded against a suppressed id and the caller committed it", tc.name)
		})
	}
}
