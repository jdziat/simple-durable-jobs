package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestEvents_Stream(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := jobs.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var received []jobs.Event
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case e := <-events:
			received = append(received, e)
			if len(received) >= 2 { // start + complete
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	assert.GreaterOrEqual(t, len(received), 2)
}
