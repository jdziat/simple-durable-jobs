package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// UPGRADE.md's Rollback section promises that every migration in this line is
// additive, so "an older binary runs correctly against the newer schema". For a
// column that promise has one concrete meaning: an INSERT that does not mention
// the column — which is exactly what a binary compiled before the column existed
// emits — must still succeed. A NOT NULL column without a usable default breaks
// that, and it breaks it at write time in production, not at migration time.
//
// The section had gone stale (it listed three migrations when the ledger head was
// v40), so this pins the claim to the columns that are actually there.
func TestOlderBinaryInsertsWithoutTheNewColumns(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, NewGormStorage(db).Migrate(context.Background()), "migrate schema")

	now := time.Now().UTC()

	// v39 added jobs.waiting_signal_name. An older binary's INSERT omits it.
	jobID := core.NewID()
	require.NoError(t, db.Exec(
		`INSERT INTO jobs (id, type, queue, status, attempt, max_retries, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID, "email", "default", core.StatusPending, 0, 3, now, now,
	).Error, "an INSERT omitting jobs.waiting_signal_name must succeed; "+
		"v39 is documented as additive, so a pre-v39 binary has to keep writing")

	// v40 added checkpoints.result_shape. Same contract.
	require.NoError(t, db.Exec(
		`INSERT INTO checkpoints (id, job_id, call_index, call_type, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		core.NewID(), jobID, 0, "settle", now,
	).Error, "an INSERT omitting checkpoints.result_shape must succeed; "+
		"v40 is documented as additive, so a pre-v40 binary has to keep writing")

	// And the rows must read back through the current model without error, which
	// is what makes the column's default (not NULL) load-bearing.
	var job core.Job
	require.NoError(t, db.First(&job, "id = ?", jobID).Error)
	require.Equal(t, "", job.WaitingSignalName)

	var cp core.Checkpoint
	require.NoError(t, db.First(&cp, "job_id = ?", jobID).Error)
	require.Equal(t, "", cp.ResultShape)
}
