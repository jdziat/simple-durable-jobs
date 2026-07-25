package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedCheckpoint writes a checkpoint with an explicit span, bypassing SaveCheckpoint
// so a legacy (span_end = 0) row can be created deliberately.
func seedCheckpoint(t *testing.T, s *GormStorage, jobID core.UUID, idx int, callType string, span int) {
	t.Helper()
	require.NoError(t, s.db.Create(&core.Checkpoint{
		ID: core.NewID(), JobID: jobID, CallIndex: idx, CallType: callType,
		Result: []byte(`1`), SpanEnd: span,
	}).Error)
}

func seedJob(t *testing.T, s *GormStorage, status core.JobStatus) core.UUID {
	t.Helper()
	j := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: status}
	require.NoError(t, s.db.Create(j).Error)
	return j.ID
}

func TestFindLegacyCallSpanJobs(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// AT RISK: two legacy Call checkpoints, still running.
	atRisk := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, atRisk, 0, "child", 0)
	seedCheckpoint(t, s, atRisk, 1, "leaf", 0)

	// SAFE: post-upgrade checkpoints carry real spans.
	migrated := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, migrated, 0, "child", 2)
	seedCheckpoint(t, s, migrated, 1, "leaf", 2)

	// SAFE: a single legacy call cannot be shifted by a preceding one.
	single := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, single, 0, "only", 0)

	// SAFE: already terminal — replay will never happen.
	done := seedJob(t, s, core.StatusCompleted)
	seedCheckpoint(t, s, done, 0, "child", 0)
	seedCheckpoint(t, s, done, 1, "leaf", 0)

	// SAFE: phase checkpoints (call_index < 0) are not Call checkpoints.
	phases := seedJob(t, s, core.StatusRunning)
	seedCheckpoint(t, s, phases, -1, "phase:a", 0)
	seedCheckpoint(t, s, phases, -1, "phase:b", 0)

	got, err := s.FindLegacyCallSpanJobs(ctx, 100)
	require.NoError(t, err)

	ids := map[core.UUID]int{}
	for _, r := range got {
		ids[r.JobID] = r.CallCheckpoints
	}

	assert.Contains(t, ids, atRisk, "a running job with 2+ legacy Call checkpoints must be reported")
	assert.Equal(t, 2, ids[atRisk])

	assert.NotContains(t, ids, migrated, "checkpoints carrying a real span are not at risk")
	assert.NotContains(t, ids, single, "one call cannot be shifted by a preceding call")
	assert.NotContains(t, ids, done, "a terminal job will never replay")
	assert.NotContains(t, ids, phases, "phase checkpoints do not participate in call-index assignment")
}

func TestFindLegacyCallSpanJobs_LimitIsApplied(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for range 5 {
		id := seedJob(t, s, core.StatusRunning)
		seedCheckpoint(t, s, id, 0, "child", 0)
		seedCheckpoint(t, s, id, 1, "leaf", 0)
	}

	got, err := s.FindLegacyCallSpanJobs(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2, "limit must bound the result set")

	// A non-positive limit must not mean "unbounded" — an operator running this
	// against a large table should not pull the whole history into memory.
	got, err = s.FindLegacyCallSpanJobs(ctx, 0)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(got), 100)
}
