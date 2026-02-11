package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
)

func TestWorkerConfig_Defaults(t *testing.T) {
	config := WorkerConfig{
		Queues:       nil,
		PollInterval: 100 * time.Millisecond,
		WorkerID:     "test-worker",
	}

	assert.Nil(t, config.Queues)
	assert.Equal(t, 100*time.Millisecond, config.PollInterval)
	assert.Equal(t, "test-worker", config.WorkerID)
	assert.False(t, config.EnableScheduler)
}

func TestConcurrency_AppliesCorrectly(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 1,
			"high":    1,
		},
	}

	Concurrency(5).ApplyWorker(&config)

	assert.Equal(t, 5, config.Queues["default"])
	assert.Equal(t, 5, config.Queues["high"])
}

func TestConcurrency_ClampedToMax(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 1,
		},
	}

	// MaxConcurrency is 1000
	Concurrency(5000).ApplyWorker(&config)

	assert.Equal(t, 1000, config.Queues["default"])
}

func TestConcurrency_ClampedToMin(t *testing.T) {
	config := WorkerConfig{
		Queues: map[string]int{
			"default": 10,
		},
	}

	Concurrency(0).ApplyWorker(&config)

	assert.Equal(t, 1, config.Queues["default"])
}

func TestWithScheduler_Enables(t *testing.T) {
	config := WorkerConfig{}

	WithScheduler(true).ApplyWorker(&config)

	assert.True(t, config.EnableScheduler)
}

func TestWithScheduler_Disables(t *testing.T) {
	config := WorkerConfig{EnableScheduler: true}

	WithScheduler(false).ApplyWorker(&config)

	assert.False(t, config.EnableScheduler)
}

func TestWorkerQueue_AddsQueue(t *testing.T) {
	config := WorkerConfig{}

	WorkerQueue("emails").ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	assert.Contains(t, config.Queues, "emails")
	assert.Equal(t, 10, config.Queues["emails"]) // default concurrency
}

func TestWorkerQueue_WithConcurrency(t *testing.T) {
	config := WorkerConfig{}

	WorkerQueue("high-priority", Concurrency(20)).ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	assert.Equal(t, 20, config.Queues["high-priority"])
}

func TestWorkerQueue_MultipleQueues(t *testing.T) {
	config := WorkerConfig{}

	// Note: Concurrency option applies to ALL existing queues in the config
	// So add queues first, then set concurrency once at the end
	WorkerQueue("default").ApplyWorker(&config)
	WorkerQueue("critical").ApplyWorker(&config)
	WorkerQueue("low").ApplyWorker(&config)

	require.NotNil(t, config.Queues)
	// All queues have default concurrency of 10
	assert.Equal(t, 10, config.Queues["default"])
	assert.Equal(t, 10, config.Queues["critical"])
	assert.Equal(t, 10, config.Queues["low"])
}

func TestWorkerQueue_ConcurrencyAffectsAllQueues(t *testing.T) {
	config := WorkerConfig{}

	// Add multiple queues
	WorkerQueue("default").ApplyWorker(&config)
	WorkerQueue("critical").ApplyWorker(&config)

	// Concurrency(5) will set ALL existing queues to 5
	Concurrency(5).ApplyWorker(&config)

	assert.Equal(t, 5, config.Queues["default"])
	assert.Equal(t, 5, config.Queues["critical"])
}

func TestWorkerOptionFunc_ImplementsInterface(t *testing.T) {
	var opt WorkerOption = workerOptionFunc(func(c *WorkerConfig) {
		c.WorkerID = "custom-id"
	})

	config := WorkerConfig{}
	opt.ApplyWorker(&config)

	assert.Equal(t, "custom-id", config.WorkerID)
}

func TestWorker_Pause(t *testing.T) {
	// Create a minimal queue for testing
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())
	q := queue.New(store)

	w := NewWorker(q)

	// Not paused initially
	assert.False(t, w.IsPaused())

	// Pause
	w.Pause(core.PauseModeGraceful)
	assert.True(t, w.IsPaused())

	// Resume
	w.Resume()
	assert.False(t, w.IsPaused())
}

func TestWorker_PauseMode(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())
	q := queue.New(store)

	w := NewWorker(q)

	// Default mode
	assert.Equal(t, core.PauseModeGraceful, w.PauseMode())

	// Set aggressive mode
	w.Pause(core.PauseModeAggressive)
	assert.True(t, w.IsPaused())
	assert.Equal(t, core.PauseModeAggressive, w.PauseMode())
}
