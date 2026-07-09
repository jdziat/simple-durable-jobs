package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// scope64 returns a deterministic 64-char scope hash (unique_locks.ScopeHash is
// size:64) built from a short seed.
func scope64(seed string) string {
	return (seed + strings.Repeat("0", 64))[:64]
}

// Compile-time proof that *GormStorage satisfies the metrics package's
// (unexported) cardinality source interfaces — the exact method shapes
// registerConcurrencySlotCardinalityGauge / registerRateLimitWindowCardinalityGauge
// type-assert at runtime.
var _ interface {
	ConcurrencySlotCardinality(context.Context) (int64, error)
	RateLimitWindowCardinality(context.Context) (int64, error)
} = (*GormStorage)(nil)

// TestPurgeJobs_DeletesDanglingUniqueLocks (B-03) proves PurgeJobs deletes the
// unique_locks of purged leaf jobs (previously stranded) while SKIPPING a fan-out
// parent's lock (the parent itself is skipped from the purge).
func TestPurgeJobs_DeletesDanglingUniqueLocks(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// A leaf job holding a live unique lock, driven to completed so PurgeJobs
	// selects it.
	leafID, err := s.EnqueueWithUniqueLock(ctx, &core.Job{
		ID: core.NewID(), Type: "work", Queue: "q", Args: []byte(`{}`),
	}, scope64("leaf"), time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", leafID).
		Update("status", core.StatusCompleted).Error)

	// A fan-out parent (also completed, same queue) holding its own unique lock.
	// PurgeJobs must SKIP it (parent-skip guard) and therefore keep its lock.
	parentID := core.NewID()
	require.NoError(t, s.db.Create(&core.Job{
		ID: parentID, Type: "work", Queue: "q", Status: core.StatusCompleted,
	}).Error)
	require.NoError(t, s.db.Create(&core.FanOut{
		ID: core.NewID(), ParentJobID: parentID, TotalCount: 1,
	}).Error)
	require.NoError(t, s.db.Create(&core.UniqueLock{
		ScopeHash: scope64("parent"), JobID: parentID, ExpiresAt: time.Now().Add(time.Hour),
	}).Error)

	// Preconditions (guard the test is meaningful).
	require.Equal(t, int64(1), countLocks(t, s, leafID))
	require.Equal(t, int64(1), countLocks(t, s, parentID))

	deleted, err := s.PurgeJobs(ctx, "q", core.StatusCompleted)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "only the leaf is purged; the fan-out parent is skipped")

	// Fail-first anchor: the leaf's lock must be gone (old code stranded it).
	require.Equal(t, int64(0), countLocks(t, s, leafID), "PurgeJobs must delete the purged leaf's unique lock")
	// The skipped parent's lock (and job) must survive — the delete is not over-broad.
	require.Equal(t, int64(1), countLocks(t, s, parentID), "a skipped fan-out parent's unique lock must survive")
	var parentJobs int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("id = ?", parentID).Count(&parentJobs).Error)
	require.Equal(t, int64(1), parentJobs, "the fan-out parent job must survive the purge")
}

func countLocks(t *testing.T, s *GormStorage, jobID core.UUID) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(&core.UniqueLock{}).Where("job_id = ?", jobID).Count(&n).Error)
	return n
}

// TestConcurrencySlotCardinality (B-04) proves the cardinality counts the
// permanent per-slot-name sentinels, independent of live/released slots.
func TestConcurrencySlotCardinality(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	names := []string{"tenant:a", "tenant:b", "tenant:c"}
	jobIDs := make([]core.UUID, len(names))
	for i, name := range names {
		jobIDs[i] = core.NewID()
		ok, err := s.TryAcquireConcurrencySlot(ctx, name, jobIDs[i], "w1", 5, time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
	}

	n, err := s.ConcurrencySlotCardinality(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), n, "one permanent sentinel per distinct slot name")

	// Releasing a live slot must NOT change cardinality (sentinel is permanent).
	require.NoError(t, s.ReleaseConcurrencySlot(ctx, names[0], jobIDs[0]))
	n, err = s.ConcurrencySlotCardinality(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), n, "releasing a slot leaves its admission sentinel; cardinality is unchanged")
}

// TestRateLimitWindowCardinality (B-04) proves the cardinality tracks the total
// rate-limit window rows (one per distinct active limit name here).
func TestRateLimitWindowCardinality(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	now := time.Now()
	for _, name := range []string{"api:tenantA", "api:tenantB"} {
		ok, err := s.TryConsumeRate(ctx, name, 10, time.Second, now)
		require.NoError(t, err)
		require.True(t, ok)
	}

	n, err := s.RateLimitWindowCardinality(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), n, "one window row per distinct active limit name")
}
