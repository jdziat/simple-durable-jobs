package storage

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
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

func TestSearchJobs_EscapesLikeMetacharacters(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	now := time.Now()

	jobs := []*core.Job{
		{ID: "literal-percent-%", Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"literal percent %"}`), CreatedAt: now},
		{ID: "literal-underscore-_", Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"literal underscore _"}`), CreatedAt: now.Add(time.Second)},
		{ID: "plain", Type: "work", Queue: "default", Status: core.StatusPending, Args: []byte(`{"value":"plain"}`), CreatedAt: now.Add(2 * time.Second)},
	}
	for _, job := range jobs {
		require.NoError(t, store.Enqueue(ctx, job))
	}

	percentMatches, percentTotal, err := store.SearchJobs(ctx, core.JobFilter{Search: "%", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), percentTotal)
	require.Len(t, percentMatches, 1)
	assert.Equal(t, "literal-percent-%", percentMatches[0].ID)

	underscoreMatches, underscoreTotal, err := store.SearchJobs(ctx, core.JobFilter{Search: "_", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), underscoreTotal)
	require.Len(t, underscoreMatches, 1)
	assert.Equal(t, "literal-underscore-_", underscoreMatches[0].ID)
}

func TestSearchJobs_OverlongSearchIsBounded(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)

	bounded := strings.Repeat("a", maxUISearchLength)
	require.NoError(t, store.Enqueue(ctx, &core.Job{
		ID:        "bounded-" + bounded,
		Type:      "work",
		Queue:     "default",
		Status:    core.StatusPending,
		Args:      []byte(`{}`),
		CreatedAt: time.Now(),
	}))

	matches, total, err := store.SearchJobs(ctx, core.JobFilter{Search: bounded + "extra", Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, matches, 1)
	assert.Equal(t, "bounded-"+bounded, matches[0].ID)
}

func TestPurgeJobs_DeletesCheckpoints(t *testing.T) {
	ctx := context.Background()
	store := newUITestStorage(t)
	job := &core.Job{
		ID:        "failed-with-checkpoint",
		Type:      "work",
		Queue:     "q",
		Status:    core.StatusFailed,
		Args:      []byte(`{}`),
		CreatedAt: time.Now(),
	}
	require.NoError(t, store.Enqueue(ctx, job))
	require.NoError(t, store.SaveCheckpoint(ctx, &core.Checkpoint{
		ID:        "checkpoint-1",
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
			ID:        "job-" + strconv.Itoa(i),
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
		rootID := "root-" + strconv.Itoa(i)
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID:        rootID,
			Type:      "workflow",
			Queue:     "default",
			Status:    core.StatusRunning,
			Args:      []byte(`{}`),
			CreatedAt: now.Add(time.Duration(i) * time.Second),
		}))
		require.NoError(t, store.Enqueue(ctx, &core.Job{
			ID:          "child-" + strconv.Itoa(i),
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
