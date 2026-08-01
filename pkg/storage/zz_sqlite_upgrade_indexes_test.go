package storage

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// The jobs.max_retries column definition as every shipped release has written it.
// It is spelled out rather than derived, because the point of this test is to catch
// the current model DRIFTING away from it.
const shippedMaxRetriesColumn = "`max_retries` integer NOT NULL DEFAULT 3"

var maxRetriesColumnRE = regexp.MustCompile("`max_retries`[^,)]*")

// A SQLite upgrade must not rebuild the jobs table.
//
// GORM's SQLite migrator implements a column-definition change as DROP + CREATE, and
// only the indexes GORM itself knows about survive — the ones added by versioned
// migrations do not. Removing core.Job.MaxRetries' `default:3` tag (an attempted fix
// for an explicit Retries(0) being stored as 3) did exactly that: 14 indexes before
// the upgrade, 4 after, the dequeue index among them.
//
// So model the upgrade: build the table as a shipped release left it, restore its
// indexes, then run the current Migrate over it. If the model no longer agrees with
// the shipped column definition, SQLite rebuilds and the count drops.
func TestSQLiteUpgradeDoesNotRebuildTheJobsTable(t *testing.T) {
	db := openTestDB(t)
	if db.Name() != "sqlite" {
		t.Skip("sqlite-specific: only its migrator rebuilds a table to alter a column")
	}
	require.NoError(t, NewGormStorage(db).Migrate(context.Background()))

	var tableDDL string
	require.NoError(t, db.Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").Scan(&tableDDL).Error)
	var idxDDL []string
	require.NoError(t, db.Raw(`SELECT sql FROM sqlite_master WHERE type='index'
		AND tbl_name='jobs' AND sql IS NOT NULL`).Scan(&idxDDL).Error)
	require.NotEmpty(t, idxDDL, "expected the migrations to have created indexes on jobs")

	require.Regexp(t, maxRetriesColumnRE, tableDDL)
	shippedDDL := maxRetriesColumnRE.ReplaceAllString(tableDDL, shippedMaxRetriesColumn)

	require.NoError(t, db.Exec("DROP TABLE jobs").Error)
	require.NoError(t, db.Exec(shippedDDL).Error)
	for _, s := range idxDDL {
		require.NoError(t, db.Exec(s).Error)
	}

	var before int
	require.NoError(t, db.Raw(`SELECT count(*) FROM sqlite_master WHERE type='index'
		AND tbl_name='jobs' AND sql IS NOT NULL`).Scan(&before).Error)

	require.NoError(t, NewGormStorage(db).Migrate(context.Background()))

	var after int
	require.NoError(t, db.Raw(`SELECT count(*) FROM sqlite_master WHERE type='index'
		AND tbl_name='jobs' AND sql IS NOT NULL`).Scan(&after).Error)

	require.GreaterOrEqual(t, after, before,
		"upgrading a shipped SQLite database rebuilt the jobs table and dropped %d indexes; "+
			"the current model's max_retries column no longer matches %q",
		before-after, shippedMaxRetriesColumn)
}
