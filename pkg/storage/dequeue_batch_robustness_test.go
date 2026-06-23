package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDequeueBatchPerQueue_ExhaustedQueueDoesNotStarveOthers guards the teardown g3
// fix: a deep, budget-exhausted queue must not prevent other queues' jobs from
// being claimed (the locked path now excludes an exhausted queue from subsequent
// scans instead of re-scanning + SKIP-LOCK-locking its whole backlog). On
// Postgres/MySQL this exercises dequeueBatchLocked (where the fix lives); on sqlite
// it exercises the single-process path. Either way the budget + no-starvation
// contract must hold.
func TestDequeueBatchPerQueue_ExhaustedQueueDoesNotStarveOthers(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("hot", "t")))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Enqueue(ctx, newTestJob("cold", "t")))
	}

	jobs, err := s.DequeueBatchPerQueue(ctx, "w", map[string]int{"hot": 1, "cold": 5})
	require.NoError(t, err)

	counts := map[string]int{}
	for _, j := range jobs {
		counts[j.Queue]++
	}
	assert.Equal(t, 1, counts["hot"], "hot queue's budget of 1 must be respected despite its deep backlog")
	assert.Equal(t, 3, counts["cold"], "cold queue's jobs must not be starved by the deep, exhausted hot queue")
}

type poisonDecodeCodec struct{}

func (poisonDecodeCodec) Encode(plaintext []byte) ([]byte, error) { return plaintext, nil }

func (poisonDecodeCodec) Decode(stored []byte) ([]byte, error) {
	if bytes.Contains(stored, []byte("POISON")) {
		return nil, errors.New("poison decode failure")
	}
	return stored, nil
}

// TestDequeueBatch_PoisonPayloadReleasedNotStranded is the regression test for
// teardown g3: a single undecodable payload (a custom codec failing post-claim)
// used to fail the whole DequeueBatch call, stranding EVERY claimed row locked
// until the reaper (~45m). The poison row must instead be released back to pending
// and excluded, while the successfully-decoded jobs are returned.
func TestDequeueBatch_PoisonPayloadReleasedNotStranded(t *testing.T) {
	db := openTestDB(t)
	s := NewGormStorage(db, WithCodec(poisonDecodeCodec{}))
	require.NoError(t, s.Migrate(context.Background()))
	ctx := context.Background()

	good := &core.Job{Type: "t", Queue: "default", Args: []byte(`{"ok":1}`)}
	poison := &core.Job{Type: "t", Queue: "default", Args: []byte(`{"x":"POISON"}`)}
	require.NoError(t, s.Enqueue(ctx, good))
	require.NoError(t, s.Enqueue(ctx, poison))

	jobs, err := s.DequeueBatch(ctx, []string{"default"}, "w", 10)
	require.NoError(t, err, "a poison payload must not fail/strand the whole batch")

	require.Len(t, jobs, 1, "only the decodable job is returned")
	assert.Equal(t, good.ID, jobs[0].ID)

	// The poison row must be released back to pending, not left stranded
	// running/locked until the reaper.
	var poisonRow core.Job
	require.NoError(t, s.db.WithContext(ctx).First(&poisonRow, "id = ?", poison.ID).Error)
	assert.Equal(t, core.StatusPending, poisonRow.Status,
		"the undecodable row must be released to pending, not stranded as running")
	assert.Empty(t, poisonRow.LockedBy, "the released row must not retain a lock owner")
}
