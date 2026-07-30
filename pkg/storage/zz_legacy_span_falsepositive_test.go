package storage

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// span_end = 0 is NOT a legacy marker on its own. Only Call() records a span, so
// every checkpoint written by the CURRENT version for a built-in durable
// operation — fan-out, signal wait, signal-wait-with-timeout — also carries
// span_end = 0. The detector's original predicate (call_index >= 0 AND
// span_end = 0) therefore flagged healthy current-version workflows as
// pre-upgrade suspects.
//
// That matters because of what the operator is told to do about a listed job.
// UPGRADE.md says "requeueing a listed job is the repair" and "Requeue anything
// you cannot rule out", and Requeue CLEARS CHECKPOINTS — so following the
// documented remediation on a false positive discards completed durable work and
// re-executes its side effects. The false positive is not cosmetic.
//
// It is also unavoidable in practice rather than a corner case: checkpoints.span_end
// is added by the v4.6 migration, so this query cannot be run before upgrading —
// by the time it CAN be run, current-version fan-out/signal checkpoints already
// exist. Any workflow using two or more built-in durable operations qualified.
func TestFindLegacyCallSpanJobs_DoesNotFlagCurrentVersionBuiltinCheckpoints(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// HEALTHY, current version: three built-in durable operations. Written by
	// this version, span_end = 0 for all of them, and not legacy.
	healthy := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, healthy, 0, core.CheckpointTypeFanOut, 0)
	seedCheckpoint(t, s, healthy, 1, core.CheckpointTypeSignalPrefix+"approval", 0)
	seedCheckpoint(t, s, healthy, 2, core.CheckpointTypeSignalTimeoutPrefix+"approval", 0)

	// GENUINELY AT RISK, and it must still be found even though a built-in
	// checkpoint sits alongside the legacy Call ones. Narrowing the predicate
	// must not buy a false negative.
	mixed := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, mixed, 0, core.CheckpointTypeFanOut, 0)
	seedCheckpoint(t, s, mixed, 1, "child", 0)
	seedCheckpoint(t, s, mixed, 2, "leaf", 0)

	// NOT at risk: exactly ONE legacy Call checkpoint. The defect needs a later
	// call to read an earlier call's slot, so one call cannot be affected. The
	// built-in rows must not pad the count past the HAVING threshold.
	single := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, single, 0, core.CheckpointTypeFanOut, 0)
	seedCheckpoint(t, s, single, 1, core.CheckpointTypeSignalPrefix+"go", 0)
	seedCheckpoint(t, s, single, 2, "child", 0)

	found, err := s.FindLegacyCallSpanJobs(ctx, 100)
	require.NoError(t, err)

	flagged := make(map[core.UUID]int, len(found))
	for _, f := range found {
		flagged[f.JobID] = f.CallCheckpoints
	}

	require.NotContains(t, flagged, healthy,
		"a healthy current-version workflow of built-in durable operations was flagged as a pre-upgrade suspect; the documented repair for a listed job is a checkpoint-clearing Requeue, which would re-execute completed work")
	require.NotContains(t, flagged, single,
		"a job with only ONE legacy Call checkpoint was flagged; built-in checkpoints must not pad the count past the HAVING threshold")
	require.Contains(t, flagged, mixed,
		"a job with two legacy Call checkpoints must still be flagged even when a built-in checkpoint sits alongside them")
	require.Equal(t, 2, flagged[mixed],
		"the reported count must be legacy CALL checkpoints only, not every checkpoint on the job")

	// The SQL in UPGRADE.md is what an operator actually pastes into a shell, so
	// verify that artifact by RUNNING it rather than trusting it to have been kept
	// in sync by hand. It must agree with FindLegacyCallSpanJobs above.
	docRows := runUpgradeDocLegacyQuery(t, s)
	require.NotContains(t, docRows, healthy,
		"the SQL published in UPGRADE.md flags a healthy current-version workflow, so an operator following the docs would Requeue it and lose completed work")
	require.NotContains(t, docRows, single, "UPGRADE.md's SQL flags a job with only one legacy Call")
	require.Contains(t, docRows, mixed, "UPGRADE.md's SQL misses a genuinely at-risk job")
}

// runUpgradeDocLegacyQuery extracts the operator-facing detection query from
// UPGRADE.md and executes it verbatim, so the published SQL cannot drift away
// from FindLegacyCallSpanJobs without a test failing.
func runUpgradeDocLegacyQuery(t *testing.T, s *GormStorage) map[core.UUID]int {
	t.Helper()
	md, err := os.ReadFile(filepath.Join("..", "..", "UPGRADE.md"))
	require.NoError(t, err, "read UPGRADE.md")

	// Pick the block by CONTENT, not position: the fences are indented inside a
	// list item, and UPGRADE.md carries more than one sql block.
	re := regexp.MustCompile("(?s)```sql\n(.*?)```")
	var query string
	for _, m := range re.FindAllSubmatch(md, -1) {
		body := string(m[1])
		if strings.Contains(body, "span_end") && strings.Contains(body, "SELECT") {
			query = strings.TrimSuffix(strings.TrimSpace(body), ";")
			break
		}
	}
	require.NotEmpty(t, query,
		"could not find the legacy-span detection SQL block in UPGRADE.md; if it moved, update this test rather than deleting it")

	var rows []struct {
		ID              core.UUID `gorm:"column:id"`
		Type            string    `gorm:"column:type"`
		CallCheckpoints int       `gorm:"column:call_checkpoints"`
	}
	require.NoError(t, s.db.Raw(query).Scan(&rows).Error, "the SQL published in UPGRADE.md failed to execute")
	out := make(map[core.UUID]int, len(rows))
	for _, r := range rows {
		out[r.ID] = r.CallCheckpoints
	}
	return out
}
