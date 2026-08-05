package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// core.Job.MaxRetries keeps its gorm `default:3` tag (dropping it made AutoMigrate
// rebuild the SQLite jobs table and destroy the migration-created indexes), and
// GORM substitutes a declared default for any zero value — which turned an explicit
// Retries(0) into three attempts. The claim that makes this safe is "every Go write
// path corrects a DELIBERATE zero back, inside the same transaction" — which is
// only worth as much as the paths actually checked.
//
// So check them all. A future path that inserts a job through a narrowed column set
// would reintroduce the bug for that path alone, and this is what would catch it.
//
// The intent flag is what makes the zero deliberate. Its mirror —
// TestOmittedMaxRetriesKeepsTheColumnDefaultOnEveryEnqueuePath — walks the SAME
// table asserting the OPPOSITE for a job that never mentioned the field, because a
// path honouring one and not the other is the exact defect this pair exists for.
func TestExplicitZeroRetriesSurvivesEveryEnqueuePath(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	newJob := func(n string) *core.Job {
		return &core.Job{
			ID: core.NewID(), Type: "charge-" + n, Queue: "default",
			Status: core.StatusPending, MaxRetries: 0, MaxRetriesSet: true,
		}
	}

	for i, p := range enqueuePathsUnderTest(ctx, s, db) {
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
