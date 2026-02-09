package jobs_test

import (
	"context"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestQueue(t *testing.T) (*jobs.Queue, *gorm.DB) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)
	return queue, db
}

func TestQueue_Register(t *testing.T) {
	queue, _ := setupTestQueue(t)

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	assert.True(t, queue.HasHandler("send-email"))
	assert.False(t, queue.HasHandler("unknown"))
}
