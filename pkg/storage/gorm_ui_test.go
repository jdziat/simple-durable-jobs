package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
)

func newUITestStorage(t *testing.T) *GormStorage {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return store
}

func TestMigrate_IdempotentRecordsAllMigrations(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)

	require.NoError(t, store.Migrate(ctx))
	assert.True(t, store.db.Migrator().HasColumn(&core.ScheduledFire{}, "last_fired_at"))

	var versions []int
	require.NoError(t, store.db.Model(&core.SchemaMigration{}).Order("version").Pluck("version", &versions).Error)
	assert.Equal(t, len(schemaMigrations), len(versions))
	assert.Equal(t, schemaMigrations[len(schemaMigrations)-1].Version, versions[len(versions)-1])
}

func TestSearchJobs_EscapesLikeMetacharacters(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()

	jobs := []*core.Job{
		{ID: testUUID("literal-percent-%"), Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"literal percent %"}`), CreatedAt: now},
		{ID: testUUID("literal-underscore-_"), Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"literal underscore _"}`), CreatedAt: now.Add(time.Second)},
		{ID: testUUID("plain"), Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"plain"}`), CreatedAt: now.Add(2 * time.Second)},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	percentMatches, percentTotal, err := store.SearchJobs(ctx, core.JobFilter{Search: "%", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), percentTotal)
	require.Len(t, percentMatches, 1)
	assert.Equal(t, testUUID("literal-percent-%"), percentMatches[0].ID)

	underscoreMatches, underscoreTotal, err := store.SearchJobs(ctx, core.JobFilter{Search: "_", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), underscoreTotal)
	require.Len(t, underscoreMatches, 1)
	assert.Equal(t, testUUID("literal-underscore-_"), underscoreMatches[0].ID)
}

func TestSearchJobs_FiltersTenantAndMetadata(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()

	jobs := []*core.Job{
		{
			ID:        testUUID("tenant-a-match"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Tenant:    "tenant-a",
			Metadata:  map[string]string{"env": "prod", "region": "us"},
			Args:      []byte(`{}`),
			CreatedAt: now,
		},
		{
			ID:        testUUID("tenant-a-dev"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Tenant:    "tenant-a",
			Metadata:  map[string]string{"env": "dev", "region": "us"},
			Args:      []byte(`{}`),
			CreatedAt: now.Add(time.Second),
		},
		{
			ID:        testUUID("tenant-b-prod"),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Tenant:    "tenant-b",
			Metadata:  map[string]string{"env": "prod", "region": "eu"},
			Args:      []byte(`{}`),
			CreatedAt: now.Add(2 * time.Second),
		},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	tenantMatches, tenantTotal, err := store.SearchJobs(ctx, core.JobFilter{Tenant: "tenant-a", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(2), tenantTotal)
	assert.ElementsMatch(t, []string{string(testUUID("tenant-a-match")), string(testUUID("tenant-a-dev"))}, jobIDs(tenantMatches))

	metaMatches, metaTotal, err := store.SearchJobs(ctx, core.JobFilter{
		MetaContains: &core.MetadataMap{"env": "prod", "region": "us"},
		Limit:        10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), metaTotal)
	require.Len(t, metaMatches, 1)
	assert.Equal(t, testUUID("tenant-a-match"), metaMatches[0].ID)

	combinedMatches, combinedTotal, err := store.SearchJobs(ctx, core.JobFilter{
		Tenant:       "tenant-b",
		MetaContains: &core.MetadataMap{"env": "prod"},
		Limit:        10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), combinedTotal)
	require.Len(t, combinedMatches, 1)
	assert.Equal(t, testUUID("tenant-b-prod"), combinedMatches[0].ID)
}

func TestCountActiveWorkers_CountsDistinctRunningLockHolders(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)

	count, err := store.CountActiveWorkers(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	now := time.Now()
	lockedUntil := now.Add(time.Minute)
	jobs := []*core.Job{
		{ID: testUUID("running-worker-1-a"), Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-1", LockedUntil: &lockedUntil, CreatedAt: now},
		{ID: testUUID("running-worker-1-b"), Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "worker-1", LockedUntil: &lockedUntil, CreatedAt: now},
		{ID: testUUID("running-worker-2"), Type: "work", Queue: "emails", Status: core.StatusRunning, LockedBy: "worker-2", LockedUntil: &lockedUntil, CreatedAt: now},
		{ID: testUUID("running-empty-worker"), Type: "work", Queue: "default", Status: core.StatusRunning, LockedBy: "", LockedUntil: &lockedUntil, CreatedAt: now},
		{ID: testUUID("pending-worker-3"), Type: "work", Queue: "default", Status: core.StatusPending, LockedBy: "worker-3", LockedUntil: &lockedUntil, CreatedAt: now},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	count, err = store.CountActiveWorkers(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestGetQueueDepthStats_OldestPendingAt(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	oldestDefault := now.Add(-3 * time.Hour)
	newerDefault := now.Add(-time.Hour)
	oldestEmails := now.Add(-2 * time.Hour)

	jobs := []*core.Job{
		{ID: testUUID("default-oldest"), Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{}`), CreatedAt: oldestDefault},
		{ID: testUUID("default-newer"), Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{}`), CreatedAt: newerDefault},
		{ID: testUUID("default-running"), Type: "work", Queue: "default", Status: core.StatusRunning, Args: []byte(`{}`), CreatedAt: now.Add(-4 * time.Hour)},
		{ID: testUUID("emails-oldest"), Type: "work", Queue: "emails", Status: core.StatusPending, Args: []byte(`{}`), CreatedAt: oldestEmails},
		{ID: testUUID("completed-only"), Type: "work", Queue: "archive", Status: core.StatusCompleted, Args: []byte(`{}`), CreatedAt: now.Add(-5 * time.Hour)},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	stats, err := store.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	byQueue := queueStatsByName(stats)

	require.NotNil(t, byQueue["default"].OldestPendingAt)
	assert.Equal(t, oldestDefault.Unix(), byQueue["default"].OldestPendingAt.AsTime().Unix())
	require.NotNil(t, byQueue["emails"].OldestPendingAt)
	assert.Equal(t, oldestEmails.Unix(), byQueue["emails"].OldestPendingAt.AsTime().Unix())
	require.NotNil(t, byQueue["archive"])
	assert.Nil(t, byQueue["archive"].OldestPendingAt)
}

func TestGetQueueDepthStats_IncludesRetryingWaitingCancelled(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()

	jobs := []*core.Job{
		{ID: testUUID("default-retrying-1"), Type: "work", Queue: "default", Status: core.StatusRetrying, Args: []byte(`{}`), CreatedAt: now},
		{ID: testUUID("default-retrying-2"), Type: "work", Queue: "default", Status: core.StatusRetrying, Args: []byte(`{}`), CreatedAt: now.Add(time.Second)},
		{ID: testUUID("default-waiting"), Type: "work", Queue: "default", Status: core.StatusWaiting, Args: []byte(`{}`), CreatedAt: now.Add(2 * time.Second)},
		{ID: testUUID("emails-waiting"), Type: "work", Queue: "emails", Status: core.StatusWaiting, Args: []byte(`{}`), CreatedAt: now.Add(3 * time.Second)},
		{ID: testUUID("emails-cancelled"), Type: "work", Queue: "emails", Status: core.StatusCancelled, Args: []byte(`{}`), CreatedAt: now.Add(4 * time.Second)},
		{ID: testUUID("archive-cancelled"), Type: "work", Queue: "archive", Status: core.StatusCancelled, Args: []byte(`{}`), CreatedAt: now.Add(5 * time.Second)},
		{ID: testUUID("archive-pending"), Type: "work", Queue: "archive", Status: core.StatusPending, Args: []byte(`{}`), CreatedAt: now.Add(6 * time.Second)},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	stats, err := store.GetQueueDepthStats(ctx)
	require.NoError(t, err)
	byQueue := queueStatsByName(stats)

	require.Contains(t, byQueue, "default")
	assert.Equal(t, int64(2), byQueue["default"].Retrying)
	assert.Equal(t, int64(1), byQueue["default"].Waiting)
	assert.Equal(t, int64(0), byQueue["default"].Cancelled)

	require.Contains(t, byQueue, "emails")
	assert.Equal(t, int64(0), byQueue["emails"].Retrying)
	assert.Equal(t, int64(1), byQueue["emails"].Waiting)
	assert.Equal(t, int64(1), byQueue["emails"].Cancelled)

	require.Contains(t, byQueue, "archive")
	assert.Equal(t, int64(0), byQueue["archive"].Retrying)
	assert.Equal(t, int64(0), byQueue["archive"].Waiting)
	assert.Equal(t, int64(1), byQueue["archive"].Cancelled)
}

func TestParseDBTimestamp_AcceptsRFC3339NanoAndSQLiteLiterals(t *testing.T) {
	rfc3339Nano := "2026-06-07T12:34:56.123456789Z"
	sqliteLiteral := "2026-06-07 12:34:56.123456789+00:00"

	rfcTime, ok := parseDBTimestamp(rfc3339Nano)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 6, 7, 12, 34, 56, 123456789, time.UTC).UnixNano(), rfcTime.UnixNano())

	sqliteTime, ok := parseDBTimestamp(sqliteLiteral)
	require.True(t, ok)
	assert.Equal(t, time.Date(2026, 6, 7, 12, 34, 56, 123456789, time.UTC).UnixNano(), sqliteTime.UnixNano())
}

func TestGetScheduledFireTimes(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	anchor := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	fireTime := time.Date(2026, 6, 7, 12, 15, 0, 0, time.UTC)

	_, err := store.SeedScheduledFire(ctx, "seeded-only", anchor)
	require.NoError(t, err)
	claimed, err := store.ClaimScheduledFire(ctx, "daily-report", fireTime)
	require.NoError(t, err)
	require.True(t, claimed)

	times, err := store.GetScheduledFireTimes(ctx)
	require.NoError(t, err)
	require.NotContains(t, times, "seeded-only")
	require.Contains(t, times, "daily-report")
	assert.Equal(t, fireTime.Unix(), times["daily-report"].Unix())
}

func queueStatsByName(stats []*jobsv1.QueueStats) map[string]*jobsv1.QueueStats {
	byName := make(map[string]*jobsv1.QueueStats, len(stats))
	for _, stat := range stats {
		byName[stat.Name] = stat
	}
	return byName
}

func TestSearchJobs_OverlongSearchIsBounded(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)

	bounded := strings.Repeat("a", maxUISearchLength)
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		Type:      "work",
		Queue:     "default",
		Status:    core.StatusPending,
		Args:      []byte(`{"search":"` + bounded + `"}`),
		CreatedAt: time.Now(),
	}))

	matches, total, err := store.SearchJobs(ctx, core.JobFilter{Search: bounded + "extra", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, matches, 1)
	assert.Contains(t, string(matches[0].Args), bounded)
}

func TestPurgeJobs_DeletesCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	job := &core.Job{
		ID:        core.NewID(),
		Type:      "work",
		Queue:     "q",
		Status:    core.StatusFailed,
		Args:      []byte(`{}`),
		CreatedAt: time.Now(),
	}
	require.NoError(t, store.Enqueue(ctx, job))
	require.NoError(t, store.SaveCheckpoint(ctx, &core.Checkpoint{
		ID:        core.NewID(),
		JobID:     job.ID,
		CallIndex: 0,
		CallType:  "test",
		Result:    []byte(`"ok"`),
		CreatedAt: time.Now(),
	}))

	deleted, err := store.PurgeJobs(ctx, "q", core.StatusFailed)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	var checkpointCount int64
	require.NoError(t, store.db.WithContext(ctx).Model(&core.Checkpoint{}).Count(&checkpointCount).Error)
	assert.Equal(t, int64(0), checkpointCount)
}

// seedFanOutTree creates a fan-out parent with one FanOut row and the given
// number of sub-jobs (each with parent_job_id/root_job_id/fan_out_id set), plus
// a checkpoint and a signal on the parent. Returns the parent and sub-job ids.
func seedFanOutTree(t *testing.T, store *GormStorage, queue string, status core.JobStatus, subCount int) (core.UUID, []core.UUID) {
	t.Helper()
	ctx := context.Background()
	parentID := core.NewID()
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID: parentID, Type: "workflow", Queue: queue, Status: status,
		Args: []byte(`{}`), CreatedAt: time.Now(),
	}))
	require.NoError(t, store.SaveCheckpoint(ctx, &core.Checkpoint{
		ID: core.NewID(), JobID: parentID, CallIndex: 0, CallType: "fanout",
		Result: []byte(`"ok"`), CreatedAt: time.Now(),
	}))
	require.NoError(t, store.db.Create(&core.Signal{
		ID: core.NewID(), JobID: parentID, Name: "go", CreatedAt: time.Now(),
	}).Error)
	fanOutID := core.NewID()
	require.NoError(t, store.CreateFanOut(ctx, &core.FanOut{
		ID: fanOutID, ParentJobID: parentID, TotalCount: subCount, Status: core.FanOutPending,
	}))
	subIDs := make([]core.UUID, 0, subCount)
	for i := 0; i < subCount; i++ {
		subID := core.NewID()
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID: subID, Type: "step", Queue: queue, Status: core.StatusPending,
			Args: []byte(`{}`), ParentJobID: &parentID, RootJobID: &parentID,
			FanOutID: &fanOutID, FanOutIndex: i, CreatedAt: time.Now(),
		}))
		subIDs = append(subIDs, subID)
	}
	return parentID, subIDs
}

func jobExists(t *testing.T, store *GormStorage, id core.UUID) bool {
	t.Helper()
	var n int64
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", id).Count(&n).Error)
	return n > 0
}

// F-001: DeleteJob must refuse to delete a fan-out parent (which would strand its
// children with dangling parent/root/fan_out refs), and DeleteWorkflowSubtree is
// the explicit workflow-aware path that removes the whole tree.
func TestDeleteJob_RejectsFanOutParent(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)
	parentID, subIDs := seedFanOutTree(t, store, "q", core.StatusCompleted, 3)

	err := store.DeleteJob(ctx, parentID)
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrJobHasChildren)

	// Nothing was deleted — parent and all children survive.
	assert.True(t, jobExists(t, store, parentID), "parent must survive a rejected delete")
	for _, id := range subIDs {
		assert.True(t, jobExists(t, store, id), "child must survive a rejected parent delete")
	}
}

func TestDeleteJob_AllowsLeaf(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)
	leaf := &core.Job{
		ID: core.NewID(), Type: "work", Queue: "q", Status: core.StatusCompleted,
		Args: []byte(`{}`), CreatedAt: time.Now(),
	}
	require.NoError(t, store.Enqueue(ctx, leaf))
	require.NoError(t, store.DeleteJob(ctx, leaf.ID))
	assert.False(t, jobExists(t, store, leaf.ID))
}

func TestDeleteWorkflowSubtree_RemovesWholeTree(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)
	parentID, subIDs := seedFanOutTree(t, store, "q", core.StatusCompleted, 3)

	require.NoError(t, store.DeleteWorkflowSubtree(ctx, parentID))

	assert.False(t, jobExists(t, store, parentID), "root removed")
	for _, id := range subIDs {
		assert.False(t, jobExists(t, store, id), "sub-job removed")
	}
	var fanOuts, checkpoints, signals int64
	require.NoError(t, store.db.Model(&core.FanOut{}).Count(&fanOuts).Error)
	require.NoError(t, store.db.Model(&core.Checkpoint{}).Count(&checkpoints).Error)
	require.NoError(t, store.db.Model(&core.Signal{}).Count(&signals).Error)
	assert.Equal(t, int64(0), fanOuts, "fan_outs removed")
	assert.Equal(t, int64(0), checkpoints, "root + sub checkpoints removed")
	assert.Equal(t, int64(0), signals, "root + sub signals removed")
}

// F-001: PurgeJobs must skip fan-out parents (so it can't strand children) while
// still deleting matching leaf jobs along with their checkpoints AND signals.
func TestPurgeJobs_SkipsFanOutParentsAndDeletesSignals(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)

	// A fan-out parent in the purged status+queue — must be skipped.
	parentID, _ := seedFanOutTree(t, store, "q", core.StatusCompleted, 2)
	// A standalone leaf in the same status+queue with a checkpoint + signal.
	leaf := &core.Job{
		ID: core.NewID(), Type: "work", Queue: "q", Status: core.StatusCompleted,
		Args: []byte(`{}`), CreatedAt: time.Now(),
	}
	require.NoError(t, store.Enqueue(ctx, leaf))
	require.NoError(t, store.SaveCheckpoint(ctx, &core.Checkpoint{
		ID: core.NewID(), JobID: leaf.ID, CallIndex: 0, CallType: "t", Result: []byte(`"x"`), CreatedAt: time.Now(),
	}))
	require.NoError(t, store.db.Create(&core.Signal{
		ID: core.NewID(), JobID: leaf.ID, Name: "s", CreatedAt: time.Now(),
	}).Error)

	deleted, err := store.PurgeJobs(ctx, "q", core.StatusCompleted)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "only the standalone leaf is purged; the fan-out parent is skipped")

	assert.True(t, jobExists(t, store, parentID), "fan-out parent must NOT be purged")
	assert.False(t, jobExists(t, store, leaf.ID), "standalone leaf is purged")

	// The leaf's signal must be gone (regression: purge previously left signals).
	var leafSignals int64
	require.NoError(t, store.db.Model(&core.Signal{}).Where("job_id = ?", leaf.ID).Count(&leafSignals).Error)
	assert.Equal(t, int64(0), leafSignals, "purge must delete the leaf's signals")
}

// F1: PurgeJobs must NOT delete a completed leaf sub-job while its fan-out PARENT
// is still non-terminal. The parent rebuilds its result slice from surviving
// sub-jobs when it resumes (pkg/fanout CollectResults), so purging a succeeded
// child first silently turns it into ErrSubJobIncomplete with no top-level error.
// This mirrors the automatic retention fanOutParentGuard, which PurgeJobs
// previously omitted despite its docstring claiming it "can never strand its
// children."
func TestPurgeJobs_SkipsCompletedSubJobOfNonTerminalParent(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)

	// Parent still WAITING (non-terminal); fan_out pending; 2 sub-jobs.
	parentID, subIDs := seedFanOutTree(t, store, "q", core.StatusWaiting, 2)
	// One sub-job finishes while the fan-out is still in flight.
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", subIDs[0]).
		Update("status", core.StatusCompleted).Error)

	// A routine "purge completed jobs" must SKIP the completed sub-job.
	deleted, err := store.PurgeJobs(ctx, "q", core.StatusCompleted)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted,
		"the only completed job is a sub-job of a non-terminal parent; it must be skipped")
	assert.True(t, jobExists(t, store, subIDs[0]),
		"completed sub-job of a still-waiting parent must survive the purge")

	// Once the parent reaches a terminal status, the completed sub-jobs ARE
	// purgeable — the guard must not over-block.
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", parentID).
		Update("status", core.StatusCompleted).Error)
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", subIDs[1]).
		Update("status", core.StatusCompleted).Error)
	deleted2, err := store.PurgeJobs(ctx, "q", core.StatusCompleted)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted2,
		"with a terminal parent both completed sub-jobs are purgeable (the parent itself is skipped as a fan-out parent)")
	assert.False(t, jobExists(t, store, subIDs[0]))
	assert.False(t, jobExists(t, store, subIDs[1]))
	assert.True(t, jobExists(t, store, parentID), "PurgeJobs always skips fan-out parents")
}

// F1: DeleteJob (dashboard-exposed) must refuse to delete a fan-out sub-job while
// its parent is not terminal — the same silent-corruption hazard PurgeJobs guards
// against — and allow the delete once the parent is terminal.
func TestDeleteJob_RefusesSubJobOfNonTerminalParent(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)

	parentID, subIDs := seedFanOutTree(t, store, "q", core.StatusWaiting, 2)
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", subIDs[0]).
		Update("status", core.StatusCompleted).Error)

	err := store.DeleteJob(ctx, subIDs[0])
	require.ErrorIs(t, err, core.ErrJobHasPendingParent)
	assert.True(t, jobExists(t, store, subIDs[0]),
		"a completed sub-job of a non-terminal parent must not be deletable")

	// Once the parent reaches a terminal status, the sub-job is deletable.
	require.NoError(t, store.db.Model(&core.Job{}).Where("id = ?", parentID).
		Update("status", core.StatusCompleted).Error)
	require.NoError(t, store.DeleteJob(ctx, subIDs[0]))
	assert.False(t, jobExists(t, store, subIDs[0]))
}

// F1 (gate): PurgeJobs must handle a backlog larger than one internal batch — the
// id list is bounded per batch (<= purgeBatchSize) so it can never exceed the
// driver's per-statement bind-parameter ceiling, and the batched loop still purges
// every matching row.
func TestPurgeJobs_LargeBacklogPurgedInBatches(t *testing.T) {
	ctx := context.Background()
	store := newTestStorage(t)

	const n = 1000 + 25 // just over the internal 1000-row batch
	for i := 0; i < n; i++ {
		j := newTestJob("q", "t")
		j.Status = core.StatusCompleted
		require.NoError(t, store.Enqueue(ctx, j))
	}

	deleted, err := store.PurgeJobs(ctx, "q", core.StatusCompleted)
	require.NoError(t, err, "a backlog larger than one internal batch must purge without a bind-parameter-ceiling error")
	assert.Equal(t, int64(n), deleted, "every matching job is purged across batches")

	var remaining int64
	require.NoError(t, store.db.WithContext(ctx).Model(&core.Job{}).
		Where("queue = ? AND status = ?", "q", core.StatusCompleted).Count(&remaining).Error)
	assert.Equal(t, int64(0), remaining, "no matching job remains after the batched purge")
}

func TestSearchJobs_ClampsOffsetAndLimit(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()
	for i := 0; i < maxUIQueryLimit+25; i++ {
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID:        core.NewID(),
			Type:      "work",
			Queue:     "default",
			Status:    core.StatusPending,
			Args:      []byte(`{}`),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}))
	}

	jobs, total, err := store.SearchJobs(ctx, core.JobFilter{Limit: maxUIQueryLimit + 1000, Offset: -100})
	require.NoError(t, err)
	assert.Equal(t, int64(maxUIQueryLimit+25), total)
	assert.Len(t, jobs, maxUIQueryLimit)

	for i := 0; i < maxUIQueryLimit+25; i++ {
		rootID := core.NewID()
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID:        rootID,
			Type:      "workflow",
			Queue:     "default",
			Status:    core.StatusRunning,
			Args:      []byte(`{}`),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}))
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID:          core.NewID(),
			Type:        "step",
			Queue:       "default",
			Status:      core.StatusPending,
			Args:        []byte(`{}`),
			ParentJobID: &rootID,
			CreatedAt:   now.Add(time.Duration(i) * time.Second),
		}))
	}

	roots, workflowTotal, err := store.GetWorkflowRoots(ctx, "", maxUIQueryLimit+1000, -100)
	require.NoError(t, err)
	assert.Equal(t, int64(maxUIQueryLimit+25), workflowTotal)
	assert.Len(t, roots, maxUIQueryLimit)
}

// UI-01: SearchJobs sorts server-side over the full result set by a whitelisted
// column, and an unknown/hostile sort key falls back to the default (no SQL
// injection, no error).
func TestSearchJobs_ServerSideSortWhitelist(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()
	// Three jobs: priorities 1,3,2 enqueued oldest→newest.
	for i, p := range []int{1, 3, 2} {
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID: core.NewID(), Type: "work", Queue: "default", Status: core.StatusPending,
			Priority: p, Args: []byte(`{}`), CreatedAt: now.Add(time.Duration(i) * time.Second),
		}))
	}

	// priority ASC → 1,2,3
	jobs, total, err := store.SearchJobs(ctx, core.JobFilter{SortKey: "priority", SortDir: "asc", Limit: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 3, total)
	require.Len(t, jobs, 3)
	assert.Equal(t, []int{1, 2, 3}, []int{jobs[0].Priority, jobs[1].Priority, jobs[2].Priority})

	// priority DESC → 3,2,1
	jobs, _, err = store.SearchJobs(ctx, core.JobFilter{SortKey: "priority", SortDir: "desc", Limit: 10})
	require.NoError(t, err)
	require.Len(t, jobs, 3)
	assert.Equal(t, []int{3, 2, 1}, []int{jobs[0].Priority, jobs[1].Priority, jobs[2].Priority})

	// Hostile/unknown sort key → falls back to the default order, no error, no
	// injection (table intact, all rows returned, same order as the default).
	hostile, total, err := store.SearchJobs(ctx, core.JobFilter{SortKey: "priority; DROP TABLE jobs;--", SortDir: "asc", Limit: 10})
	require.NoError(t, err, "unknown sort key must fall back, never error/inject")
	assert.EqualValues(t, 3, total, "table intact, all rows returned")
	require.Len(t, hostile, 3)
	// Fallback maps the unknown key to created_at but honors the requested dir,
	// so it must match an explicit created_at+asc sort (not error, not inject).
	baseline, _, err := store.SearchJobs(ctx, core.JobFilter{SortKey: "created_at", SortDir: "asc", Limit: 10})
	require.NoError(t, err)
	require.Len(t, baseline, 3)
	assert.Equal(t, jobIDs(baseline), jobIDs(hostile), "hostile sort key falls back to created_at, dir honored")
}
