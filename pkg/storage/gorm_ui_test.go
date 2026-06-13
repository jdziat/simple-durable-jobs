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
