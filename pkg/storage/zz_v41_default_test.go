package storage

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// core.Job.MaxRetries carries no gorm `default:` tag, because GORM substitutes a
// declared default for any zero value and that silently turned Retries(0) into 3.
// The column consequently has no default either — this pins that, so a future
// re-added tag fails here rather than in production.
func TestMaxRetriesHasNoColumnDefault(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, NewGormStorage(db).Migrate(context.Background()))

	var def string
	switch db.Name() {
	case "postgres":
		require.NoError(t, db.Raw(`SELECT COALESCE(column_default,'<none>') FROM information_schema.columns
			WHERE table_name='jobs' AND column_name='max_retries'`).Scan(&def).Error)
	case "mysql":
		require.NoError(t, db.Raw(`SELECT COALESCE(COLUMN_DEFAULT,'<none>') FROM information_schema.COLUMNS
			WHERE TABLE_NAME='jobs' AND COLUMN_NAME='max_retries' AND TABLE_SCHEMA=DATABASE()`).Scan(&def).Error)
	default:
		t.Skip("sqlite reports defaults differently; the sqlite path is covered by TestEnqueuePreservesAnExplicitZeroRetries")
	}
	// No column default on any dialect, by design: AutoMigrate derives the DDL
	// from the struct tag, and the tag cannot declare a default without GORM
	// substituting it for an explicit Retries(0). A raw INSERT must list the
	// column, as both documented sql-interop examples do.
	require.Equal(t, "<none>", def,
		"jobs.max_retries must have no column default; a default here is what swallowed Retries(0)")

	// And the point of the whole change: an explicit zero survives.
	var raw int
	id := mustEnqueueZeroRetries(t, db)
	require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", id).Scan(&raw).Error)
	require.Equal(t, 0, raw, "the column default must not swallow an explicit Retries(0)")
}

func mustEnqueueZeroRetries(t *testing.T, db *gorm.DB) core.UUID {
	t.Helper()
	s := NewGormStorage(db)
	job := &core.Job{ID: core.NewID(), Type: "charge", Queue: "default", Status: core.StatusPending, MaxRetries: 0}
	require.NoError(t, s.Enqueue(context.Background(), job))
	return job.ID
}
