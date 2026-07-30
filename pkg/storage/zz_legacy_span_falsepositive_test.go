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
// every checkpoint the CURRENT version writes for a built-in durable operation —
// fan-out, durable sleep, and each of the signal operations — also carries
// span_end = 0. The detector's original predicate (call_index >= 0 AND
// span_end = 0) therefore flagged healthy current-version workflows as
// pre-upgrade suspects.
//
// WHAT THE IMPACT ACTUALLY IS, because my first version of this comment got it
// wrong and the claim is worth stating precisely. A listed job produces a
// spurious entry in the operator's suspect list and a spurious WARN on every
// replay, telling them a healthy workflow may be returning another call's result.
// It does NOT destroy work: I originally wrote that an operator following
// UPGRADE.md would Requeue a listed job and lose its checkpoints, and that is
// unreachable — Requeue accepts only failed/cancelled jobs and this listing
// excludes exactly those, so the two sets are disjoint. That disjointness is a
// real defect in its own right, covered by TestLegacyCallSpanRepairPath.
//
// The false positive was unavoidable rather than a corner case:
// checkpoints.span_end is added by the v4.6 migration, so this query cannot run
// before upgrading — by the time it CAN run, current-version checkpoints already
// exist, and a workflow that merely calls Sleep twice qualified.
func TestFindLegacyCallSpanJobs_DoesNotFlagCurrentVersionBuiltinCheckpoints(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// HEALTHY, current version: EVERY built-in durable operation. Written by this
	// version, span_end = 0 for all of them, and none of them legacy. My first
	// pass at this fix covered only fanout/signal/signaltimeout and silently
	// missed _sleep, signalpeek and signaldrain, so the list is enumerated once
	// and asserted exhaustive below rather than spelled out per test.
	healthy := seedJob(t, s, core.StatusRunning)
	for i, ct := range builtinCheckpointTypesUnderTest() {
		seedCheckpoint(t, s, healthy, i, ct, 0)
	}

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
		"a healthy current-version workflow of built-in durable operations was flagged as a pre-upgrade suspect, which puts it in the operator's repair list and makes every replay log a WARN saying it may be returning another call's result")
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
		"the SQL published in UPGRADE.md flags a healthy current-version workflow, so an operator following the docs would put healthy work on their repair list")
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

// builtinCheckpointTypesUnderTest is one concrete CallType per built-in durable
// operation. TestBuiltinCheckpointTypes_AreExhaustive keeps it honest.
func builtinCheckpointTypesUnderTest() []string {
	return []string{
		core.CheckpointTypeFanOut,
		core.CheckpointTypeSleep,
		core.CheckpointTypeSignalPrefix + "approval",
		core.CheckpointTypeSignalTimeoutPrefix + "approval",
		core.CheckpointTypeSignalPeekPrefix + "approval",
		core.CheckpointTypeSignalDrainPrefix + "approval",
	}
}

// TestBuiltinCheckpointTypes_AreExhaustive is the guard against the mistake this
// fix already made once: adding a built-in durable operation, giving it a
// checkpoint CallType with a real call index, and forgetting the detector.
//
// It walks the CallType values the PRODUCERS construct in pkg/signal, pkg/fanout
// and pkg/jobctx by scanning their source, and requires every one to be either a
// recognised built-in or a genuine user Call(). A new built-in type fails here
// with the file and line to fix, instead of silently rejoining the legacy
// listing and warning operators about healthy work.
func TestBuiltinCheckpointTypes_AreExhaustive(t *testing.T) {
	// Literal and prefix-built CallType values, as produced. jobctx's phase
	// checkpoints use CallIndex -1 and are excluded by the detector's
	// call_index >= 0 term, so they are not part of this contract.
	producers := map[string][]string{
		"pkg/fanout/fanout.go": {"core.CheckpointTypeFanOut"},
		"pkg/signal/signal.go": {
			"core.CheckpointTypeSleep",
			"core.CheckpointTypeSignalPrefix",
			"core.CheckpointTypeSignalTimeoutPrefix",
			"core.CheckpointTypeSignalPeekPrefix",
			"core.CheckpointTypeSignalDrainPrefix",
		},
	}
	for file, expected := range producers {
		src, err := os.ReadFile(filepath.Join("..", "..", file))
		require.NoError(t, err)
		text := string(src)

		// Every CallType/ctype the file builds must come from a core constant.
		// A bare string literal is the exact shape that was missed before.
		bare := regexp.MustCompile(`(?m)^\s*(?:const )?ctype\s*:?=\s*"([^"]+)"|^\s*CallType:\s*"([^"]+)"`)
		for _, m := range bare.FindAllStringSubmatch(text, -1) {
			lit := m[1] + m[2]
			t.Errorf("%s builds a checkpoint CallType from the bare literal %q instead of a core.CheckpointType* constant; the legacy-span detector keys on those constants, so this type would be reported as pre-upgrade work and warn operators about healthy jobs", file, lit)
		}
		for _, konst := range expected {
			require.Contains(t, text, konst,
				"%s no longer references %s; if the operation was removed, drop it from this test and from core.IsCallCheckpointType, and if it was renamed, update both", file, konst)
		}
	}

	// Every enumerated type must actually be classified as built-in.
	for _, ct := range builtinCheckpointTypesUnderTest() {
		require.False(t, core.IsCallCheckpointType(ct),
			"%q is produced by a built-in durable operation but IsCallCheckpointType calls it a user Call(), so the legacy-span detector will flag healthy work", ct)
	}
	// And a user Call() name must still classify as a Call.
	for _, ct := range []string{"child", "leaf", "chargeCard", "fanoutish", "signalish"} {
		require.True(t, core.IsCallCheckpointType(ct), "%q is a user Call() name and must not be excluded", ct)
	}
}
