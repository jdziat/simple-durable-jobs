package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// The single-row Dequeue's poison-skip budget is spent on the SKIPS, not on the
// claims: `for len(*skipped) < maxPoisonSkipsPerDequeue` gives the call exactly
// maxPoisonSkipsPerDequeue claim attempts, so a queue holding exactly that many
// poison rows ahead of the first healthy job uses all of them on poison and
// never attempts the healthy claim.
//
// The godoc used to read "more than this many ... makes no progress on this
// tick", which told an operator triaging a stalled queue that 16 is survivable
// and only 17+ stalls. The true threshold is 16. This test pins both sides of
// the boundary so the sentence and the loop can only drift together.
func TestDequeue_PoisonSkipStarvationBoundary(t *testing.T) {
	seed := func(t *testing.T, s *GormStorage, poisonRows int) core.UUID {
		t.Helper()
		// Priority 10 puts every poison row ahead of the healthy job under
		// `ORDER BY priority DESC, COALESCE(run_at, created_at)`.
		for i := 0; i < poisonRows; i++ {
			p := &core.Job{
				ID: core.NewID(), Type: "wf", Queue: "default",
				Status: core.StatusPending, Priority: 10, DQReady: true,
				Args: []byte(poisonMarker),
			}
			require.NoError(t, s.db.Create(p).Error)
		}
		healthy := &core.Job{
			ID: core.NewID(), Type: "wf", Queue: "default",
			Status: core.StatusPending, Priority: 0, DQReady: true,
			Args: []byte(`{"ok":1}`),
		}
		require.NoError(t, s.db.Create(healthy).Error)
		s.codec = poisonCodec{}
		return healthy.ID
	}

	t.Run("one below the bound still reaches the healthy job", func(t *testing.T) {
		s := newTestStorage(t)
		want := seed(t, s, maxPoisonSkipsPerDequeue-1)

		job, err := s.Dequeue(context.Background(), []string{"default"}, "w1")
		require.NoError(t, err)
		require.NotNil(t, job, "%d poison rows must still leave one claim for the healthy job", maxPoisonSkipsPerDequeue-1)
		require.Equal(t, want, job.ID)
	})

	t.Run("exactly the bound already starves", func(t *testing.T) {
		s := newTestStorage(t)
		seed(t, s, maxPoisonSkipsPerDequeue)

		job, err := s.Dequeue(context.Background(), []string{"default"}, "w1")
		require.NoError(t, err, "a fully-poisoned reachable window is not an error")
		require.Nil(t, job,
			"exactly maxPoisonSkipsPerDequeue poison rows consume every claim attempt; "+
				"if this now returns a job, the godoc's threshold sentence must change with it")
	})
}
