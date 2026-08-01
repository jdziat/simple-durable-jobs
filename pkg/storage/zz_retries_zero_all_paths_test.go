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

// core.Job.MaxRetries carries no gorm `default:` tag, because GORM substitutes a
// declared default for any zero value and that turned an explicit Retries(0) into
// three attempts. The claim that makes this safe is "every Go write path writes the
// column explicitly" — which is only worth as much as the paths actually checked.
//
// So check them all. A future path that inserts a job through a narrowed column set
// would reintroduce the bug for that path alone, and this is what would catch it.
func TestExplicitZeroRetriesSurvivesEveryEnqueuePath(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	newJob := func(n string) *core.Job {
		return &core.Job{
			ID: core.NewID(), Type: "charge-" + n, Queue: "default",
			Status: core.StatusPending, MaxRetries: 0,
		}
	}

	paths := []struct {
		name string
		run  func(t *testing.T, job *core.Job)
	}{
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

	for i, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			job := newJob(fmt.Sprintf("%d", i))
			p.run(t, job)

			var raw int
			require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", job.ID).Scan(&raw).Error)
			require.Equal(t, 0, raw,
				"%s did not persist an explicit Retries(0); a handler marked do-not-retry "+
					"would run three times through this path", p.name)
		})
	}
}
