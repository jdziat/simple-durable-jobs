package storage

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// This file guards the "Atomic completion (CompleteWithResult)" section of
// docs/content/docs/storage-durability.md.
//
// That section shipped naming GetStalledFanOutParents as the scan that recovers
// a fan-out whose completed_count is one short. It cannot: its predicate is
// COUNT(child rows) < total_count — it finds a fan-out whose CREATION never
// finished, not one whose counter is short — so it selects exactly nothing for
// the state the page described. What actually rescues GormStorage is GetFanOut's
// live-count overlay plus the OPTIONAL GetCompletablePendingFanOuts.
//
// The tests below seed that exact state and execute all three, so the page can
// no longer name a scan that does not select it.

const storageDurabilityDocPath = "../../docs/content/docs/storage-durability.md"

// seedShortCounterFanOut builds the state storage-durability.md describes: both
// children terminal 'completed', the stored completed_count one short, the
// fan-out still 'pending', the parent 'waiting'.
func seedShortCounterFanOut(t *testing.T, s *GormStorage) (parentID, fanOutID core.UUID) {
	t.Helper()
	ctx := context.Background()
	old := time.Now().Add(-2 * time.Hour)

	parent := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusWaiting}
	require.NoError(t, s.db.Create(parent).Error)

	fanOut := &core.FanOut{ID: core.NewID(), ParentJobID: parent.ID, TotalCount: 2, Status: core.FanOutPending}
	require.NoError(t, s.CreateFanOut(ctx, fanOut))

	for i := 0; i < 2; i++ {
		child := &core.Job{
			ID: core.NewID(), Type: "child", Queue: "default",
			Status: core.StatusCompleted, FanOutID: &fanOut.ID,
		}
		require.NoError(t, s.db.Create(child).Error)
	}

	// One increment lost: the stored counter says 1, the child rows say 2.
	require.NoError(t, s.db.Model(&core.FanOut{}).Where("id = ?", fanOut.ID).
		Updates(map[string]any{"completed_count": 1, "created_at": old}).Error)

	return parent.ID, fanOut.ID
}

// TestStalledFanOutParentsCannotSelectAShortCounter pins the negative the page
// now states: the scan it used to name selects nothing for this state.
func TestStalledFanOutParentsCannotSelectAShortCounter(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	parentID, fanOutID := seedShortCounterFanOut(t, s)
	cutoff := time.Now().Add(-time.Minute)

	// Raw row really is short, so the fixture models the documented state.
	var stored core.FanOut
	require.NoError(t, s.db.Raw("SELECT * FROM fan_outs WHERE id = ?", fanOutID).Scan(&stored).Error)
	require.Equal(t, 1, stored.CompletedCount, "fixture must model a SHORT stored counter")

	stalled, err := s.GetStalledFanOutParents(ctx, cutoff)
	require.NoError(t, err)
	require.Empty(t, stalled,
		"GetStalledFanOutParents predicates on MISSING CHILD ROWS; if it starts selecting a short counter, "+
			"storage-durability.md may name it again")

	waiting, err := s.GetWaitingJobsToResume(ctx)
	require.NoError(t, err)
	require.Empty(t, waiting, "a parent with a still-pending fan-out is excluded from the other required scan")

	// The two mechanisms that DO rescue GormStorage.
	fo, err := s.GetFanOut(ctx, fanOutID)
	require.NoError(t, err)
	require.Equal(t, 2, fo.CompletedCount,
		"GetFanOut must overlay live child counts; storage-durability.md's GormStorage row depends on it")

	completable, err := s.GetCompletablePendingFanOuts(ctx, cutoff)
	require.NoError(t, err)
	require.Len(t, completable, 1, "GetCompletablePendingFanOuts is what actually finds this state")
	require.Equal(t, fanOutID, completable[0].ID)
	require.Equal(t, parentID, completable[0].ParentJobID)
}

// TestStorageDurabilityDocNamesTheRealRecovery requires the page to name the
// mechanisms the test above exercised, and to have stopped naming the one that
// cannot select the state.
func TestStorageDurabilityDocNamesTheRealRecovery(t *testing.T) {
	b, err := os.ReadFile(storageDurabilityDocPath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", storageDurabilityDocPath)
	doc := string(b)

	idx := strings.Index(doc, "## Atomic completion")
	require.GreaterOrEqual(t, idx, 0, "Atomic completion section not found")
	section := doc[idx:]

	require.NotContains(t, section,
		"stays `waiting` until the stalled-fan-out recovery scan\n(`GetStalledFanOutParents`, `FanOutRecoveryStaleAge`) resumes it",
		"GetStalledFanOutParents does not select a short counter")
	require.Contains(t, section, "GetCompletablePendingFanOuts",
		"the page must name the scan that really recovers this state")
	require.Contains(t, section, "GetStalledFanOutParents",
		"the page should still explain WHY the obvious-looking scan does not apply")
	require.NotContains(t, section, "neither loses the completion",
		"on a backend with a stored counter and no GetCompletablePendingFanOuts, the completion IS lost")
}

// TestEveryFanOutReaderOverlaysLiveCounts pins the claim GetFanOut's godoc makes:
// that EVERY fan-out reader on this type overlays live child counts, so the stored
// columns are visible only to a raw row read.
//
// The godoc originally shipped with this paragraph inverted — it named
// GetFanOutsByParent and the dashboard as readers that do NOT overlay, and told
// implementors the two views "must not be assumed interchangeable". Both skeptics
// reviewing that patch caught it independently. The claim sits inside the comment
// that storage-durability.md cites as the authority for why a lost fan-out
// increment is recoverable, so getting it backwards would lead a custom-backend
// author to overlay in exactly one place and inherit the bug the page warns about.
//
// Mutation-tested: dropping the overlay from any reader fails this test.
func TestEveryFanOutReaderOverlaysLiveCounts(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	parentID, fanOutID := seedShortCounterFanOut(t, s)

	const live = 2 // two children really completed; the stored column says 1

	var stored core.FanOut
	require.NoError(t, s.db.Where("id = ?", fanOutID).First(&stored).Error)
	require.Equalf(t, 1, stored.CompletedCount,
		"fixture precondition: the STORED column must be short, or this test cannot "+
			"distinguish an overlaying reader from a non-overlaying one")

	got, err := s.GetFanOut(ctx, fanOutID)
	require.NoError(t, err)
	require.Equal(t, live, got.CompletedCount, "GetFanOut must overlay")

	byParent, err := s.GetFanOutsByParent(ctx, parentID)
	require.NoError(t, err)
	require.Len(t, byParent, 1)
	require.Equalf(t, live, byParent[0].CompletedCount,
		"GetFanOutsByParent must overlay too — the godoc once claimed it does not")

	completable, err := s.GetCompletablePendingFanOuts(ctx, time.Now())
	require.NoError(t, err)
	require.NotEmpty(t, completable, "the short-counter fan-out must be completable")
	for _, fo := range completable {
		if fo.ID == fanOutID {
			require.Equal(t, live, fo.CompletedCount, "GetCompletablePendingFanOuts must overlay")
		}
	}
}
