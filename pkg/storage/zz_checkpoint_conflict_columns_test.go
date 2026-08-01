package storage

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// checkpointConflictColumns is hand-maintained, and a column missing from it does
// not fail loudly — the upsert just leaves that column at its old value, which for
// a checkpoint means a stale field sitting beside a fresh result. span_end was
// omitted once; result_shape was omitted again when it was added.
//
// So derive the expectation from the MODEL rather than restating the list: every
// column of core.Checkpoint that a re-save can legitimately change must appear.
// Identity and creation columns are excluded by name, and anything genuinely new
// has to be classified deliberately rather than silently forgotten.
func TestCheckpointConflictColumnsCoverEveryMutableColumn(t *testing.T) {
	db := openTestDB(t)

	stmt := db.Model(&core.Checkpoint{}).Statement
	require.NoError(t, stmt.Parse(&core.Checkpoint{}))

	// Columns an upsert must NOT overwrite: the conflict key itself and the
	// creation timestamp.
	immutable := map[string]bool{
		"id": true, "job_id": true, "call_index": true, "call_type": true,
		"created_at": true,
	}

	inList := map[string]bool{}
	for _, c := range checkpointConflictColumns {
		inList[c] = true
	}

	var missing []string
	for _, f := range stmt.Schema.Fields {
		name := f.DBName
		if name == "" || immutable[name] || inList[name] {
			continue
		}
		missing = append(missing, name)
	}
	sort.Strings(missing)
	require.Empty(t, missing,
		"these checkpoint columns can change on a re-save but are not refreshed by the upsert, "+
			"so they keep a stale value beside a fresh result: %v", missing)
}
