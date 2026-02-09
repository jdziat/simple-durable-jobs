package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var eventsTestCounter int

func setupEventsTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	eventsTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_events_test_%d_%d.db", os.Getpid(), eventsTestCounter)
	t.Cleanup(func() {
		os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)
	return queue, store
}

func TestEvents_Stream(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "test", struct{}{})
	require.NoError(t, err)

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

func TestEvents_JobStarted(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	queue.Register("started-test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "started-test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var startedEvent *jobs.JobStarted
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case e := <-events:
			if started, ok := e.(*jobs.JobStarted); ok {
				startedEvent = started
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	require.NotNil(t, startedEvent)
	assert.Equal(t, "started-test", startedEvent.Job.Type)
	assert.False(t, startedEvent.Timestamp.IsZero())
}

func TestEvents_JobCompleted(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	queue.Register("completed-test", func(ctx context.Context, _ struct{}) error {
		time.Sleep(50 * time.Millisecond) // Add some duration
		return nil
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "completed-test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var completedEvent *jobs.JobCompleted
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case e := <-events:
			if completed, ok := e.(*jobs.JobCompleted); ok {
				completedEvent = completed
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	require.NotNil(t, completedEvent)
	assert.Equal(t, "completed-test", completedEvent.Job.Type)
	assert.False(t, completedEvent.Timestamp.IsZero())
	assert.Greater(t, completedEvent.Duration, time.Duration(0))
}

func TestEvents_JobFailed(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	queue.Register("failed-test", func(ctx context.Context, _ struct{}) error {
		return jobs.NoRetry(errors.New("intentional failure"))
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "failed-test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var failedEvent *jobs.JobFailed
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case e := <-events:
			if failed, ok := e.(*jobs.JobFailed); ok {
				failedEvent = failed
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	require.NotNil(t, failedEvent)
	assert.Equal(t, "failed-test", failedEvent.Job.Type)
	assert.False(t, failedEvent.Timestamp.IsZero())
	assert.NotNil(t, failedEvent.Error)
}

func TestEvents_JobRetrying(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	attempts := 0
	queue.Register("retrying-test", func(ctx context.Context, _ struct{}) error {
		attempts++
		if attempts < 2 {
			return errors.New("retry")
		}
		return nil
	})

	events := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "retrying-test", struct{}{}, jobs.Retries(3))
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var retryingEvent *jobs.JobRetrying
	timeout := time.After(3 * time.Second)

loop:
	for {
		select {
		case e := <-events:
			if retrying, ok := e.(*jobs.JobRetrying); ok {
				retryingEvent = retrying
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	require.NotNil(t, retryingEvent)
	assert.Equal(t, "retrying-test", retryingEvent.Job.Type)
	assert.Equal(t, 1, retryingEvent.Attempt)
	assert.NotNil(t, retryingEvent.Error)
	assert.False(t, retryingEvent.NextRunAt.IsZero())
}

func TestEvents_MultipleSubscribers(t *testing.T) {
	queue, _ := setupEventsTestQueue(t)

	queue.Register("multi-sub-test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	// Multiple subscribers
	events1 := queue.Events()
	events2 := queue.Events()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "multi-sub-test", struct{}{})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	var received1, received2 int
	timeout := time.After(1 * time.Second)

loop:
	for {
		select {
		case <-events1:
			received1++
		case <-events2:
			received2++
		case <-timeout:
			break loop
		}
		if received1 >= 2 && received2 >= 2 {
			break loop
		}
	}

	assert.GreaterOrEqual(t, received1, 2)
	assert.GreaterOrEqual(t, received2, 2)
}

func TestEvents_EventMarker(t *testing.T) {
	// Verify all event types implement the Event interface marker
	var _ jobs.Event = &jobs.JobStarted{}
	var _ jobs.Event = &jobs.JobCompleted{}
	var _ jobs.Event = &jobs.JobFailed{}
	var _ jobs.Event = &jobs.JobRetrying{}
}
