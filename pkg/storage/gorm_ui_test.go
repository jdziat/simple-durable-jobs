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

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v3/ui/gen/jobs/v1"
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
