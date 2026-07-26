package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Exercises the SELECT ... FOR UPDATE on the real dialects. The package's other
// scheduled-fire tests are SQLite-only, where the lock clause is deliberately
// skipped — so without this, an invalid FOR UPDATE would ship untested.
func TestEnqueueScheduledFire_DedupRestoreOnRealDialects(t *testing.T) {
	var db *gorm.DB
	var err error
	switch {
	case os.Getenv("TEST_DATABASE_URL") != "":
		db, err = gorm.Open(postgres.Open(os.Getenv("TEST_DATABASE_URL")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	case os.Getenv("TEST_MYSQL_URL") != "":
		db, err = gorm.Open(mysql.Open(os.Getenv("TEST_MYSQL_URL")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	default:
		t.Skip("needs a real dialect")
	}
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := New(store)
	q.Register("probejob", func(context.Context, struct{}) error { return nil })
	ctx := context.Background()

	name := "probe-sched-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduled_fires WHERE name = ?`, name) })

	// Real fire, then a dedup-skip on the next boundary.
	b1 := time.Now().Truncate(time.Minute)
	claimed, _, err := q.EnqueueScheduledFire(ctx, name, b1, "probejob", nil, Unique(name+"-key"))
	require.NoError(t, err, "FOR UPDATE must be valid SQL on %s", db.Name())
	require.True(t, claimed)

	claimed2, id2, err2 := q.EnqueueScheduledFire(ctx, name, b1.Add(time.Minute), "probejob", nil, Unique(name+"-key"))
	require.ErrorIs(t, err2, core.ErrDuplicateJob)
	assert.True(t, claimed2)
	assert.Equal(t, core.NilUUID, id2)

	var row core.ScheduledFire
	require.NoError(t, db.Where("name = ?", name).First(&row).Error)
	require.NotNil(t, row.LastFiredAt)
	assert.True(t, row.LastFiredAt.UTC().Truncate(time.Second).Equal(b1.UTC().Truncate(time.Second)),
		"the real-fire marker must stay at the last ACTUAL fire on %s", db.Name())
	assert.True(t, row.LastFireAt.After(*row.LastFiredAt), "the cursor must have advanced past it")
}
