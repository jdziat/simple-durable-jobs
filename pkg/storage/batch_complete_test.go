package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// binaryResult mimics secretbox ciphertext: NUL bytes, lone continuation bytes,
// and an out-of-range UTF-8 sequence. If any storage path coerces this through a
// utf8mb4-typed column it will be mangled and the round-trip assertion fails.
var binaryResult = []byte{
	0x00, 0xff, 0xfe, 0x80, 0xc0, 0x00, 'a', 0x00,
	0xde, 0xad, 0xbe, 0xef, 0x00, 0xf4, 0x90, 0x80, 0x80, 0x00,
}

func TestBatchComplete_PerRowResults_OwnershipAndGC(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Three jobs owned by "w", one owned by "other" (must be excluded).
	a := runningJob(t, ctx, s, "w")
	b := runningJob(t, ctx, s, "w")
	c := runningJob(t, ctx, s, "w")
	other := runningJob(t, ctx, s, "other")

	// Give one of w's jobs a checkpoint to prove in-tx GC (opt-in).
	s.SetDeleteCheckpointsOnComplete(true)
	require.NoError(t, s.SaveCheckpoint(ctx, &core.Checkpoint{JobID: a.ID, CallIndex: 0, CallType: "x", Result: []byte(`"r"`)}))

	items := []BatchCompleteItem{
		{JobID: a.ID, Result: []byte(`{"job":"a"}`)},
		{JobID: b.ID, Result: []byte(`{"job":"b"}`)},
		{JobID: c.ID, Result: []byte(`{"job":"c"}`)},
		{JobID: other.ID, Result: []byte(`{"job":"other"}`)}, // not owned by w
	}
	committed, err := s.BatchComplete(ctx, "w", items)
	require.NoError(t, err)

	// Only w's three committed; the other-worker row excluded.
	require.ElementsMatch(t, []core.UUID{a.ID, b.ID, c.ID}, committed)

	// Each committed job is completed with its DISTINCT result.
	for id, want := range map[core.UUID]string{a.ID: `{"job":"a"}`, b.ID: `{"job":"b"}`, c.ID: `{"job":"c"}`} {
		got, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, core.StatusCompleted, got.Status, "job %s", id)
		assert.Equal(t, want, string(got.Result), "job %s result", id)
		assert.Empty(t, got.LockedBy, "job %s lock cleared", id)
		assert.NotNil(t, got.CompletedAt, "job %s completed_at", id)
	}

	// The not-owned job is untouched (still running, no result).
	og, err := s.GetJob(ctx, other.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, og.Status)
	assert.Empty(t, og.Result)

	// Checkpoint GC happened in-tx for the committed job.
	cps, err := s.GetCheckpoints(ctx, a.ID)
	require.NoError(t, err)
	assert.Empty(t, cps, "checkpoints GC'd in the batch tx")
}

func TestBatchComplete_BinaryResultRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	j := runningJob(t, ctx, s, "w")

	committed, err := s.BatchComplete(ctx, "w", []BatchCompleteItem{
		{JobID: j.ID, Result: binaryResult},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []core.UUID{j.ID}, committed)

	got, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, got.Status)
	assert.Equal(t, binaryResult, got.Result,
		"BatchComplete must round-trip binary result byte-for-byte (got len=%d want len=%d)",
		len(got.Result), len(binaryResult))
}

func TestBatchComplete_ChunksBeyondSingleStatementLimit(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	// Seed owned running leaf jobs by DIRECT insert rather than 600 enqueue+dequeue
	// cycles: this test exercises BatchComplete's chunking, not the dequeue path,
	// and rapid enqueue/dequeue is flaky on MySQL (a pre-existing client-vs-DB-clock
	// dequeue-eligibility gap surfaces as Dequeue returning nil under a small pool).
	const n = 600
	wantIDs := seedRunningJobs(t, ctx, s, "w", n)
	items := make([]BatchCompleteItem, n)
	for i, id := range wantIDs {
		items[i] = BatchCompleteItem{JobID: id, Result: []byte(`{"ok":true}`)}
	}

	committed, err := s.BatchComplete(ctx, "w", items)
	require.NoError(t, err)
	require.ElementsMatch(t, wantIDs, committed)

	for _, id := range wantIDs {
		got, err := s.GetJob(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, core.StatusCompleted, got.Status, "job %s", id)
	}
}

func TestBatchComplete_ExcludesFanOutSubJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	parent := &core.Job{ID: core.NewID(), Type: "parent", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.db.WithContext(ctx).Create(parent).Error)

	fanOut := &core.FanOut{ID: core.NewID(), ParentJobID: parent.ID, TotalCount: 1}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	sub := &core.Job{Type: "sub", Queue: "default", FanOutID: &fanOut.ID, FanOutIndex: 0}
	require.NoError(t, s.Enqueue(ctx, sub))
	got, err := s.Dequeue(ctx, []string{"default"}, "w")
	require.NoError(t, err)
	require.NotNil(t, got)

	committed, err := s.BatchComplete(ctx, "w", []BatchCompleteItem{
		{JobID: got.ID, Result: []byte(`{"sub":true}`)},
	})
	require.NoError(t, err)
	assert.Empty(t, committed)

	after, err := s.GetJob(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusRunning, after.Status)
	assert.Equal(t, "w", after.LockedBy)
	assert.Empty(t, after.Result)
}

func TestBatchComplete_AdvancesUpdatedAt(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	j := runningJob(t, ctx, s, "w")

	before := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).Where("id = ?", j.ID).Update("updated_at", before).Error)
	committed, err := s.BatchComplete(ctx, "w", []BatchCompleteItem{
		{JobID: j.ID, Result: []byte(`{"ok":true}`)},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []core.UUID{j.ID}, committed)

	after, err := s.GetJob(ctx, j.ID)
	require.NoError(t, err)
	assert.True(t, after.UpdatedAt.After(before), "updated_at should advance: before=%s after=%s", before, after.UpdatedAt)
}

func TestBatchComplete_Empty(t *testing.T) {
	s := newTestStorage(t)
	got, err := s.BatchComplete(context.Background(), "w", nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// seedRunningJobs inserts n owned (locked_by=workerID) running leaf jobs directly,
// bypassing the enqueue+dequeue cycle so chunking tests are deterministic on every
// backend (rapid enqueue/dequeue is flaky on MySQL under a small connection pool).
func seedRunningJobs(t *testing.T, ctx context.Context, s *GormStorage, workerID string, n int) []core.UUID {
	t.Helper()
	jobs := make([]*core.Job, n)
	ids := make([]core.UUID, n)
	for i := range jobs {
		id := core.NewID()
		ids[i] = id
		jobs[i] = &core.Job{
			ID:       id,
			Type:     "chunk.task",
			Queue:    "default",
			Status:   core.StatusRunning,
			LockedBy: workerID,
		}
	}
	require.NoError(t, s.db.WithContext(ctx).CreateInBatches(jobs, 200).Error)
	return ids
}
