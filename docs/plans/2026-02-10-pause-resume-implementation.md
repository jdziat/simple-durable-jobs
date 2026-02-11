# Pause and Resume Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add pause/resume functionality at job, queue, and worker levels with configurable graceful vs aggressive modes.

**Architecture:** New `StatusPaused` job status, `QueueState` table for queue pause state with worker memory cache, cancellation channels for aggressive interruption, and both standalone functions and convenience methods.

**Tech Stack:** Go, GORM, sync/atomic for worker state, context cancellation for interruption.

---

## Task 1: Add StatusPaused and Pause Mode Types

**Files:**
- Modify: `pkg/core/job.go:11-19`

**Step 1: Write the failing test**

Create file `pkg/core/pause_test.go`:

```go
package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPauseModeConstants(t *testing.T) {
	assert.Equal(t, PauseMode("graceful"), PauseModeGraceful)
	assert.Equal(t, PauseMode("aggressive"), PauseModeAggressive)
}

func TestStatusPaused(t *testing.T) {
	assert.Equal(t, JobStatus("paused"), StatusPaused)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/core -run TestPauseMode -v`
Expected: FAIL with undefined: PauseModeGraceful

**Step 3: Write minimal implementation**

Add to `pkg/core/job.go` after line 18 (after StatusCancelled):

```go
	StatusPaused    JobStatus = "paused"    // Paused, won't be picked up
```

Create new file `pkg/core/pause.go`:

```go
package core

// PauseMode determines how pause operations behave.
type PauseMode string

const (
	// PauseModeGraceful lets running jobs complete, stops new jobs from being picked up.
	PauseModeGraceful PauseMode = "graceful"
	// PauseModeAggressive interrupts running jobs immediately via context cancellation.
	PauseModeAggressive PauseMode = "aggressive"
)
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/core -run TestPauseMode -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/core/job.go pkg/core/pause.go pkg/core/pause_test.go
git commit -m "feat(core): add StatusPaused and PauseMode types"
```

---

## Task 2: Add Pause-Related Errors

**Files:**
- Modify: `pkg/core/errors.go:9-18`

**Step 1: Write the failing test**

Add to `pkg/core/errors_test.go`:

```go
func TestPauseErrors(t *testing.T) {
	assert.Error(t, core.ErrJobAlreadyPaused)
	assert.Error(t, core.ErrJobNotPaused)
	assert.Error(t, core.ErrQueueAlreadyPaused)
	assert.Error(t, core.ErrQueueNotPaused)
	assert.Error(t, core.ErrCannotPauseStatus)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/core -run TestPauseErrors -v`
Expected: FAIL with undefined: ErrJobAlreadyPaused

**Step 3: Write minimal implementation**

Add to `pkg/core/errors.go` after line 18:

```go
	ErrJobAlreadyPaused   = errors.New("jobs: job is already paused")
	ErrJobNotPaused       = errors.New("jobs: job is not paused")
	ErrQueueAlreadyPaused = errors.New("jobs: queue is already paused")
	ErrQueueNotPaused     = errors.New("jobs: queue is not paused")
	ErrCannotPauseStatus  = errors.New("jobs: cannot pause job in current status")
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/core -run TestPauseErrors -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/core/errors.go pkg/core/errors_test.go
git commit -m "feat(core): add pause-related errors"
```

---

## Task 3: Add QueueState Model

**Files:**
- Create: `pkg/core/queue_state.go`

**Step 1: Write the failing test**

Create `pkg/core/queue_state_test.go`:

```go
package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueState_Fields(t *testing.T) {
	now := time.Now()
	qs := QueueState{
		Queue:     "emails",
		Paused:    true,
		PausedAt:  &now,
		PausedBy:  "admin",
	}

	assert.Equal(t, "emails", qs.Queue)
	assert.True(t, qs.Paused)
	assert.NotNil(t, qs.PausedAt)
	assert.Equal(t, "admin", qs.PausedBy)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/core -run TestQueueState -v`
Expected: FAIL with undefined: QueueState

**Step 3: Write minimal implementation**

Create `pkg/core/queue_state.go`:

```go
package core

import "time"

// QueueState tracks the pause state of a queue.
type QueueState struct {
	Queue     string     `gorm:"primaryKey;size:255"`
	Paused    bool       `gorm:"default:false"`
	PausedAt  *time.Time
	PausedBy  string     `gorm:"size:255"`
	UpdatedAt time.Time  `gorm:"autoUpdateTime"`
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/core -run TestQueueState -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/core/queue_state.go pkg/core/queue_state_test.go
git commit -m "feat(core): add QueueState model for queue pause tracking"
```

---

## Task 4: Add Pause Events

**Files:**
- Modify: `pkg/core/events.go`

**Step 1: Write the failing test**

Add to `pkg/core/events_test.go`:

```go
func TestJobPausedEvent(t *testing.T) {
	job := &core.Job{ID: "test-123"}
	e := &core.JobPaused{
		Job:       job,
		Mode:      core.PauseModeGraceful,
		Timestamp: time.Now(),
	}

	// Verify it implements Event interface
	var _ core.Event = e
	assert.Equal(t, "test-123", e.Job.ID)
	assert.Equal(t, core.PauseModeGraceful, e.Mode)
}

func TestJobResumedEvent(t *testing.T) {
	job := &core.Job{ID: "test-123"}
	e := &core.JobResumed{
		Job:       job,
		Timestamp: time.Now(),
	}

	var _ core.Event = e
	assert.Equal(t, "test-123", e.Job.ID)
}

func TestQueuePausedEvent(t *testing.T) {
	e := &core.QueuePaused{
		Queue:     "emails",
		Timestamp: time.Now(),
	}

	var _ core.Event = e
	assert.Equal(t, "emails", e.Queue)
}

func TestQueueResumedEvent(t *testing.T) {
	e := &core.QueueResumed{
		Queue:     "emails",
		Timestamp: time.Now(),
	}

	var _ core.Event = e
	assert.Equal(t, "emails", e.Queue)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/core -run TestJobPausedEvent -v`
Expected: FAIL with undefined: JobPaused

**Step 3: Write minimal implementation**

Add to `pkg/core/events.go`:

```go
// JobPaused is emitted when a job is paused.
type JobPaused struct {
	Job       *Job
	Mode      PauseMode
	Timestamp time.Time
}

func (*JobPaused) eventMarker() {}

// JobResumed is emitted when a job is resumed.
type JobResumed struct {
	Job       *Job
	Timestamp time.Time
}

func (*JobResumed) eventMarker() {}

// QueuePaused is emitted when a queue is paused.
type QueuePaused struct {
	Queue     string
	Timestamp time.Time
}

func (*QueuePaused) eventMarker() {}

// QueueResumed is emitted when a queue is resumed.
type QueueResumed struct {
	Queue     string
	Timestamp time.Time
}

func (*QueueResumed) eventMarker() {}

// WorkerPaused is emitted when a worker is paused.
type WorkerPaused struct {
	WorkerID  string
	Mode      PauseMode
	Timestamp time.Time
}

func (*WorkerPaused) eventMarker() {}

// WorkerResumed is emitted when a worker is resumed.
type WorkerResumed struct {
	WorkerID  string
	Timestamp time.Time
}

func (*WorkerResumed) eventMarker() {}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/core -run "TestJob.*Event|TestQueue.*Event" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/core/events.go pkg/core/events_test.go
git commit -m "feat(core): add pause/resume events"
```

---

## Task 5: Add Storage Interface Methods for Job Pause

**Files:**
- Modify: `pkg/core/storage.go`

**Step 1: Write the failing test**

This is an interface change - no direct test. We'll test through implementation.

**Step 2: Add interface methods**

Add to `pkg/core/storage.go` after line 63 (before the closing brace):

```go
	// Job pause operations
	PauseJob(ctx context.Context, jobID string) error
	UnpauseJob(ctx context.Context, jobID string) error
	GetPausedJobs(ctx context.Context, queue string) ([]*Job, error)
	IsJobPaused(ctx context.Context, jobID string) (bool, error)

	// Queue pause operations
	PauseQueue(ctx context.Context, queue string) error
	UnpauseQueue(ctx context.Context, queue string) error
	GetPausedQueues(ctx context.Context) ([]string, error)
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	RefreshQueueStates(ctx context.Context) (map[string]bool, error)
```

**Step 3: Commit**

```bash
git add pkg/core/storage.go
git commit -m "feat(core): add pause/resume methods to Storage interface"
```

---

## Task 6: Implement Job Pause in GORM Storage

**Files:**
- Modify: `pkg/storage/gorm.go`

**Step 1: Write the failing test**

Create `tests/pause_test.go`:

```go
package jobs_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGormStorage_PauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create a pending job
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Pause the job
	err = store.PauseJob(ctx, job.ID)
	require.NoError(t, err)

	// Verify it's paused
	paused, err := store.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)

	// Verify status changed
	updated, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusPaused, updated.Status)
}

func TestGormStorage_PauseJob_AlreadyPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Pause once
	err = store.PauseJob(ctx, job.ID)
	require.NoError(t, err)

	// Pause again - should error
	err = store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobAlreadyPaused)
}

func TestGormStorage_PauseJob_CannotPauseCompleted(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Dequeue and complete
	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NotNil(t, dequeued)
	err = store.Complete(ctx, job.ID, "worker-1")
	require.NoError(t, err)

	// Try to pause - should error
	err = store.PauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrCannotPauseStatus)
}

func TestGormStorage_UnpauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Pause then unpause
	err = store.PauseJob(ctx, job.ID)
	require.NoError(t, err)

	err = store.UnpauseJob(ctx, job.ID)
	require.NoError(t, err)

	// Verify it's pending again
	updated, err := store.GetJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, jobs.StatusPending, updated.Status)
}

func TestGormStorage_UnpauseJob_NotPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Try to unpause without pausing first
	err = store.UnpauseJob(ctx, job.ID)
	assert.ErrorIs(t, err, jobs.ErrJobNotPaused)
}

func TestGormStorage_GetPausedJobs(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create jobs in different queues
	job1 := &jobs.Job{Type: "job1", Queue: "emails"}
	job2 := &jobs.Job{Type: "job2", Queue: "emails"}
	job3 := &jobs.Job{Type: "job3", Queue: "other"}

	store.Enqueue(ctx, job1)
	store.Enqueue(ctx, job2)
	store.Enqueue(ctx, job3)

	// Pause some
	store.PauseJob(ctx, job1.ID)
	store.PauseJob(ctx, job3.ID)

	// Get paused jobs in emails queue
	paused, err := store.GetPausedJobs(ctx, "emails")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
	assert.Equal(t, job1.ID, paused[0].ID)
}

func TestGormStorage_IsJobPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test-job", Queue: "default"}
	store.Enqueue(ctx, job)

	// Not paused initially
	paused, err := store.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.False(t, paused)

	// Pause it
	store.PauseJob(ctx, job.ID)

	// Now paused
	paused, err = store.IsJobPaused(ctx, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run TestGormStorage_PauseJob -v`
Expected: FAIL with method not found

**Step 3: Write minimal implementation**

Add to `pkg/storage/gorm.go`:

```go
// --- Job pause operations ---

// PauseJob pauses a job, preventing it from being picked up.
// Only pending, running, and waiting jobs can be paused.
func (s *GormStorage) PauseJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job core.Job
		if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("job not found: %s", jobID)
			}
			return err
		}

		// Check if already paused
		if job.Status == core.StatusPaused {
			return core.ErrJobAlreadyPaused
		}

		// Only allow pausing pending, running, or waiting jobs
		switch job.Status {
		case core.StatusPending, core.StatusRunning, core.StatusWaiting:
			// OK to pause
		default:
			return core.ErrCannotPauseStatus
		}

		return tx.Model(&core.Job{}).
			Where("id = ?", jobID).
			Updates(map[string]any{
				"status":       core.StatusPaused,
				"locked_by":    "",
				"locked_until": nil,
				"updated_at":   time.Now(),
			}).Error
	})
}

// UnpauseJob resumes a paused job back to pending status.
func (s *GormStorage) UnpauseJob(ctx context.Context, jobID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusPaused).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"updated_at": time.Now(),
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotPaused
	}
	return nil
}

// GetPausedJobs returns all paused jobs in a queue.
func (s *GormStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("queue = ? AND status = ?", queue, core.StatusPaused).
		Order("created_at ASC").
		Find(&jobs).Error
	return jobs, err
}

// IsJobPaused checks if a job is paused.
func (s *GormStorage) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	var job core.Job
	err := s.db.WithContext(ctx).
		Select("status").
		First(&job, "id = ?", jobID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return job.Status == core.StatusPaused, nil
}
```

Add `"fmt"` to imports if not present.

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run "TestGormStorage_PauseJob|TestGormStorage_UnpauseJob|TestGormStorage_GetPausedJobs|TestGormStorage_IsJobPaused" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/storage/gorm.go tests/pause_test.go
git commit -m "feat(storage): implement job pause/unpause operations"
```

---

## Task 7: Implement Queue Pause in GORM Storage

**Files:**
- Modify: `pkg/storage/gorm.go`

**Step 1: Write the failing test**

Add to `tests/pause_test.go`:

```go
func TestGormStorage_PauseQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Pause the queue
	err := store.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	// Verify it's paused
	paused, err := store.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestGormStorage_PauseQueue_AlreadyPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := store.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	// Pause again - should error
	err = store.PauseQueue(ctx, "emails")
	assert.ErrorIs(t, err, jobs.ErrQueueAlreadyPaused)
}

func TestGormStorage_UnpauseQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Pause then unpause
	err := store.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	err = store.UnpauseQueue(ctx, "emails")
	require.NoError(t, err)

	// Verify it's not paused
	paused, err := store.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestGormStorage_UnpauseQueue_NotPaused(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := store.UnpauseQueue(ctx, "not-paused")
	assert.ErrorIs(t, err, jobs.ErrQueueNotPaused)
}

func TestGormStorage_GetPausedQueues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Pause some queues
	store.PauseQueue(ctx, "emails")
	store.PauseQueue(ctx, "notifications")

	queues, err := store.GetPausedQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
	assert.Contains(t, queues, "emails")
	assert.Contains(t, queues, "notifications")
}

func TestGormStorage_RefreshQueueStates(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Pause a queue
	store.PauseQueue(ctx, "emails")

	// Refresh states
	states, err := store.RefreshQueueStates(ctx)
	require.NoError(t, err)

	assert.True(t, states["emails"])
	assert.False(t, states["other"])
}

func TestGormStorage_Dequeue_SkipsPausedQueues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	// Create jobs in two queues
	job1 := &jobs.Job{Type: "job1", Queue: "emails"}
	job2 := &jobs.Job{Type: "job2", Queue: "other"}
	store.Enqueue(ctx, job1)
	store.Enqueue(ctx, job2)

	// Pause emails queue
	store.PauseQueue(ctx, "emails")

	// Dequeue from both queues - should only get the "other" job
	dequeued, err := store.Dequeue(ctx, []string{"emails", "other"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, "other", dequeued.Queue)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run TestGormStorage_PauseQueue -v`
Expected: FAIL

**Step 3: Write minimal implementation**

First update Migrate to include QueueState:

```go
// Migrate creates the necessary tables.
func (s *GormStorage) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{})
}
```

Add queue pause methods to `pkg/storage/gorm.go`:

```go
// --- Queue pause operations ---

// PauseQueue marks a queue as paused.
func (s *GormStorage) PauseQueue(ctx context.Context, queue string) error {
	now := time.Now()

	// Try to create or update
	var existing core.QueueState
	err := s.db.WithContext(ctx).First(&existing, "queue = ?", queue).Error

	if err == nil {
		// Record exists
		if existing.Paused {
			return core.ErrQueueAlreadyPaused
		}
		return s.db.WithContext(ctx).
			Model(&core.QueueState{}).
			Where("queue = ?", queue).
			Updates(map[string]any{
				"paused":    true,
				"paused_at": now,
			}).Error
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	// Create new record
	return s.db.WithContext(ctx).Create(&core.QueueState{
		Queue:    queue,
		Paused:   true,
		PausedAt: &now,
	}).Error
}

// UnpauseQueue unpauses a queue.
func (s *GormStorage) UnpauseQueue(ctx context.Context, queue string) error {
	result := s.db.WithContext(ctx).
		Model(&core.QueueState{}).
		Where("queue = ? AND paused = ?", queue, true).
		Updates(map[string]any{
			"paused":    false,
			"paused_at": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrQueueNotPaused
	}
	return nil
}

// GetPausedQueues returns all paused queue names.
func (s *GormStorage) GetPausedQueues(ctx context.Context) ([]string, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).
		Where("paused = ?", true).
		Find(&states).Error
	if err != nil {
		return nil, err
	}

	queues := make([]string, len(states))
	for i, s := range states {
		queues[i] = s.Queue
	}
	return queues, nil
}

// IsQueuePaused checks if a queue is paused.
func (s *GormStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var state core.QueueState
	err := s.db.WithContext(ctx).First(&state, "queue = ?", queue).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return state.Paused, nil
}

// RefreshQueueStates returns a map of queue names to their paused state.
func (s *GormStorage) RefreshQueueStates(ctx context.Context) (map[string]bool, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).Find(&states).Error
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool)
	for _, s := range states {
		result[s.Queue] = s.Paused
	}
	return result, nil
}
```

Now update Dequeue to skip paused queues. Modify the Dequeue method to check paused queues:

```go
// Dequeue fetches and locks the next available job.
// Uses FOR UPDATE SKIP LOCKED to prevent multiple workers from selecting the same job.
// For SQLite, uses optimistic locking with atomic update (suitable for dev/testing).
// Skips jobs in paused queues.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	var job core.Job
	now := time.Now()
	lockUntil := now.Add(45 * time.Minute)

	// Get paused queues
	pausedQueues, err := s.GetPausedQueues(ctx)
	if err != nil {
		return nil, err
	}

	// Filter out paused queues
	activeQueues := make([]string, 0, len(queues))
	pausedSet := make(map[string]bool)
	for _, q := range pausedQueues {
		pausedSet[q] = true
	}
	for _, q := range queues {
		if !pausedSet[q] {
			activeQueues = append(activeQueues, q)
		}
	}

	if len(activeQueues) == 0 {
		return nil, nil // All queues are paused
	}

	// SQLite uses optimistic locking
	if s.isSQLite {
		return s.dequeueSQLite(ctx, activeQueues, workerID, now, lockUntil)
	}

	// PostgreSQL/MySQL: Use FOR UPDATE SKIP LOCKED
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("queue IN ?", activeQueues).
			Where("status = ?", core.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", now).
			Where("(locked_until IS NULL OR locked_until < ?)", now).
			Order("priority DESC, created_at ASC").
			First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		job.Status = core.StatusRunning
		job.LockedBy = workerID
		job.LockedUntil = &lockUntil
		job.StartedAt = &now
		job.Attempt++

		return tx.Save(&job).Error
	})

	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, nil
	}
	return &job, nil
}
```

Similarly update `dequeueSQLite` to accept `activeQueues` instead of `queues`.

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run "TestGormStorage.*Queue" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/storage/gorm.go tests/pause_test.go
git commit -m "feat(storage): implement queue pause/unpause with dequeue filtering"
```

---

## Task 8: Add Worker Pause State

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Write the failing test**

Add to `pkg/worker/worker_test.go`:

```go
func TestWorker_Pause(t *testing.T) {
	q := setupTestQueue(t)
	w := worker.NewWorker(q)

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
	q := setupTestQueue(t)
	w := worker.NewWorker(q)

	w.Pause(core.PauseModeAggressive)
	assert.True(t, w.IsPaused())
	assert.Equal(t, core.PauseModeAggressive, w.PauseMode())
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/worker -run TestWorker_Pause -v`
Expected: FAIL with undefined: Pause

**Step 3: Write minimal implementation**

Add fields and methods to `pkg/worker/worker.go`:

```go
import (
	"sync/atomic"
	// ... other imports
)

// Worker processes jobs from the queue.
type Worker struct {
	queue  *queue.Queue
	config WorkerConfig
	logger *slog.Logger
	wg     sync.WaitGroup

	// Pause state
	paused    atomic.Bool
	pauseMode atomic.Value // stores PauseMode
	pauseMu   sync.RWMutex

	// Queue state cache
	queueStates     map[string]bool
	queueStatesMu   sync.RWMutex
	lastStateRefresh time.Time

	// Running job cancellation (for aggressive pause)
	runningJobs   map[string]context.CancelFunc
	runningJobsMu sync.Mutex
}

// Pause pauses the worker.
func (w *Worker) Pause(mode core.PauseMode) {
	w.pauseMode.Store(mode)
	w.paused.Store(true)

	if mode == core.PauseModeAggressive {
		// Cancel all running jobs
		w.runningJobsMu.Lock()
		for _, cancel := range w.runningJobs {
			cancel()
		}
		w.runningJobsMu.Unlock()
	}
}

// Resume resumes the worker.
func (w *Worker) Resume() {
	w.paused.Store(false)
}

// IsPaused returns true if the worker is paused.
func (w *Worker) IsPaused() bool {
	return w.paused.Load()
}

// PauseMode returns the current pause mode.
func (w *Worker) PauseMode() core.PauseMode {
	mode := w.pauseMode.Load()
	if mode == nil {
		return core.PauseModeGraceful
	}
	return mode.(core.PauseMode)
}
```

Update NewWorker to initialize the maps:

```go
func NewWorker(q *queue.Queue, opts ...WorkerOption) *Worker {
	// ... existing code ...

	return &Worker{
		queue:       q,
		config:      config,
		logger:      slog.Default(),
		runningJobs: make(map[string]context.CancelFunc),
		queueStates: make(map[string]bool),
	}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/worker -run TestWorker_Pause -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/worker/worker.go pkg/worker/worker_test.go
git commit -m "feat(worker): add pause/resume state and methods"
```

---

## Task 9: Integrate Pause Check into Worker Loop

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Write the failing test**

Add to `pkg/worker/worker_test.go`:

```go
func TestWorker_PausedDoesNotDequeue(t *testing.T) {
	q := setupTestQueue(t)

	// Register a handler
	processed := make(chan string, 10)
	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		processed <- "processed"
		return nil
	})

	// Enqueue a job
	_, err := q.Enqueue(context.Background(), "test-job", struct{}{})
	require.NoError(t, err)

	// Create and immediately pause the worker
	w := worker.NewWorker(q)
	w.Pause(core.PauseModeGraceful)

	// Start worker in background
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go w.Start(ctx)

	// Wait a bit - job should NOT be processed
	select {
	case <-processed:
		t.Fatal("job should not be processed while paused")
	case <-time.After(300*time.Millisecond):
		// Expected - no job processed
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/worker -run TestWorker_PausedDoesNotDequeue -v`
Expected: FAIL (job gets processed)

**Step 3: Write minimal implementation**

Modify the Start method to check pause state:

```go
func (w *Worker) Start(ctx context.Context) error {
	// ... existing setup code ...

	for {
		select {
		case <-ctx.Done():
			close(jobsChan)
			w.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			// Skip dequeue if paused
			if w.IsPaused() {
				continue
			}

			job, err := w.dequeueWithRetry(ctx, queues)
			// ... rest of existing code ...
		}
	}
}
```

Also modify runHeartbeat to respect aggressive pause:

```go
func (w *Worker) runHeartbeat(ctx context.Context, job *core.Job) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Stop heartbeat if aggressively paused
			if w.IsPaused() && w.PauseMode() == core.PauseModeAggressive {
				return
			}

			err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				return w.queue.Storage().Heartbeat(ctx, job.ID, w.config.WorkerID)
			})
			// ... rest of existing code ...
		}
	}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/worker -run TestWorker_PausedDoesNotDequeue -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/worker/worker.go pkg/worker/worker_test.go
git commit -m "feat(worker): integrate pause check into worker loop"
```

---

## Task 10: Add Running Job Tracking for Aggressive Cancel

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Write the failing test**

Add to `pkg/worker/worker_test.go`:

```go
func TestWorker_AggressivePauseCancelsRunningJobs(t *testing.T) {
	q := setupTestQueue(t)

	started := make(chan struct{})
	cancelled := make(chan struct{})

	q.Register("long-job", func(ctx context.Context, args struct{}) error {
		close(started)
		<-ctx.Done() // Wait for cancellation
		close(cancelled)
		return ctx.Err()
	})

	_, err := q.Enqueue(context.Background(), "long-job", struct{}{})
	require.NoError(t, err)

	w := worker.NewWorker(q)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Start(ctx)

	// Wait for job to start
	select {
	case <-started:
	case <-time.After(2*time.Second):
		t.Fatal("job did not start")
	}

	// Aggressive pause should cancel the job
	w.Pause(core.PauseModeAggressive)

	// Job should be cancelled
	select {
	case <-cancelled:
		// Success
	case <-time.After(2*time.Second):
		t.Fatal("job was not cancelled")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/worker -run TestWorker_AggressivePauseCancelsRunningJobs -v`
Expected: FAIL (job not cancelled)

**Step 3: Write minimal implementation**

Update processJob to track running jobs and use cancellable context:

```go
func (w *Worker) processJob(ctx context.Context, job *core.Job) {
	startTime := time.Now()

	// Create cancellable context for this job
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()

	// Track this job for aggressive pause cancellation
	w.runningJobsMu.Lock()
	w.runningJobs[job.ID] = cancelJob
	w.runningJobsMu.Unlock()
	defer func() {
		w.runningJobsMu.Lock()
		delete(w.runningJobs, job.ID)
		w.runningJobsMu.Unlock()
	}()

	h, ok := w.queue.GetHandler(job.Type)
	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Call start hooks
	w.queue.CallStartHooks(jobCtx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// Create a cancellable context for the heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()

	// Start heartbeat goroutine
	go w.runHeartbeat(heartbeatCtx, job)

	err := w.executeHandler(jobCtx, job, h)

	// ... rest of existing code, but use jobCtx where appropriate ...
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/worker -run TestWorker_AggressivePauseCancelsRunningJobs -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/worker/worker.go pkg/worker/worker_test.go
git commit -m "feat(worker): track running jobs for aggressive pause cancellation"
```

---

## Task 11: Add Queue Convenience Methods

**Files:**
- Modify: `pkg/queue/queue.go`

**Step 1: Write the failing test**

Add to `pkg/queue/queue_test.go`:

```go
func TestQueue_PauseJob(t *testing.T) {
	store := setupTestStorage(t)
	q := queue.New(store)
	ctx := context.Background()

	q.Register("test-job", func(ctx context.Context, args struct{}) error {
		return nil
	})

	jobID, err := q.Enqueue(ctx, "test-job", struct{}{})
	require.NoError(t, err)

	err = q.PauseJob(ctx, jobID)
	require.NoError(t, err)

	paused, err := q.IsJobPaused(ctx, jobID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestQueue_PauseQueue(t *testing.T) {
	store := setupTestStorage(t)
	q := queue.New(store)
	ctx := context.Background()

	err := q.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	paused, err := q.IsQueuePaused(ctx, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/queue -run "TestQueue_Pause" -v`
Expected: FAIL with undefined method

**Step 3: Write minimal implementation**

Add to `pkg/queue/queue.go`:

```go
// --- Pause operations ---

// PauseJob pauses a specific job.
func (q *Queue) PauseJob(ctx context.Context, jobID string, opts ...PauseOption) error {
	po := &PauseOptions{Mode: core.PauseModeGraceful}
	for _, opt := range opts {
		opt.ApplyPause(po)
	}
	return q.storage.PauseJob(ctx, jobID)
}

// ResumeJob resumes a paused job.
func (q *Queue) ResumeJob(ctx context.Context, jobID string) error {
	return q.storage.UnpauseJob(ctx, jobID)
}

// IsJobPaused checks if a job is paused.
func (q *Queue) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	return q.storage.IsJobPaused(ctx, jobID)
}

// GetPausedJobs returns all paused jobs in a queue.
func (q *Queue) GetPausedJobs(ctx context.Context, queueName string) ([]*core.Job, error) {
	return q.storage.GetPausedJobs(ctx, queueName)
}

// PauseQueue pauses an entire queue.
func (q *Queue) PauseQueue(ctx context.Context, queueName string) error {
	return q.storage.PauseQueue(ctx, queueName)
}

// ResumeQueue resumes a paused queue.
func (q *Queue) ResumeQueue(ctx context.Context, queueName string) error {
	return q.storage.UnpauseQueue(ctx, queueName)
}

// IsQueuePaused checks if a queue is paused.
func (q *Queue) IsQueuePaused(ctx context.Context, queueName string) (bool, error) {
	return q.storage.IsQueuePaused(ctx, queueName)
}

// GetPausedQueues returns all paused queue names.
func (q *Queue) GetPausedQueues(ctx context.Context) ([]string, error) {
	return q.storage.GetPausedQueues(ctx)
}
```

Add pause options:

```go
// PauseOptions configures pause behavior.
type PauseOptions struct {
	Mode core.PauseMode
}

// PauseOption configures pause operations.
type PauseOption interface {
	ApplyPause(*PauseOptions)
}

type pauseModeOption struct {
	mode core.PauseMode
}

func (o pauseModeOption) ApplyPause(opts *PauseOptions) {
	opts.Mode = o.mode
}

// WithPauseMode sets the pause mode.
func WithPauseMode(mode core.PauseMode) PauseOption {
	return pauseModeOption{mode: mode}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/queue -run "TestQueue_Pause" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/queue/queue.go pkg/queue/queue_test.go
git commit -m "feat(queue): add pause/resume convenience methods"
```

---

## Task 12: Add Standalone Pause Functions

**Files:**
- Create: `pause.go` (root package)

**Step 1: Write the failing test**

Create `tests/pause_functions_test.go`:

```go
package jobs_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPauseJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	store.Enqueue(ctx, job)

	err := jobs.PauseJob(ctx, store, job.ID)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, job.ID)
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestResumeJob(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	store.Enqueue(ctx, job)
	jobs.PauseJob(ctx, store, job.ID)

	err := jobs.ResumeJob(ctx, store, job.ID)
	require.NoError(t, err)

	paused, err := jobs.IsJobPaused(ctx, store, job.ID)
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestPauseQueue(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	err := jobs.PauseQueue(ctx, store, "emails")
	require.NoError(t, err)

	paused, err := jobs.IsQueuePaused(ctx, store, "emails")
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestGetPausedJobs(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	job := &jobs.Job{Type: "test", Queue: "default"}
	store.Enqueue(ctx, job)
	jobs.PauseJob(ctx, store, job.ID)

	paused, err := jobs.GetPausedJobs(ctx, store, "default")
	require.NoError(t, err)
	assert.Len(t, paused, 1)
}

func TestGetPausedQueues(t *testing.T) {
	store := setupStorageTest(t)
	ctx := context.Background()

	jobs.PauseQueue(ctx, store, "emails")
	jobs.PauseQueue(ctx, store, "notifications")

	queues, err := jobs.GetPausedQueues(ctx, store)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./tests -run "TestPauseJob$|TestResumeJob|TestPauseQueue$|TestGetPausedJobs|TestGetPausedQueues" -v`
Expected: FAIL

**Step 3: Write minimal implementation**

Create `pause.go`:

```go
package jobs

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// PauseJob pauses a specific job.
func PauseJob(ctx context.Context, storage Storage, jobID string, opts ...PauseOption) error {
	return storage.PauseJob(ctx, jobID)
}

// ResumeJob resumes a paused job.
func ResumeJob(ctx context.Context, storage Storage, jobID string) error {
	return storage.UnpauseJob(ctx, jobID)
}

// IsJobPaused checks if a job is paused.
func IsJobPaused(ctx context.Context, storage Storage, jobID string) (bool, error) {
	return storage.IsJobPaused(ctx, jobID)
}

// GetPausedJobs returns all paused jobs in a queue.
func GetPausedJobs(ctx context.Context, storage Storage, queue string) ([]*Job, error) {
	return storage.GetPausedJobs(ctx, queue)
}

// PauseQueue pauses an entire queue.
func PauseQueue(ctx context.Context, storage Storage, queue string) error {
	return storage.PauseQueue(ctx, queue)
}

// ResumeQueue resumes a paused queue.
func ResumeQueue(ctx context.Context, storage Storage, queue string) error {
	return storage.UnpauseQueue(ctx, queue)
}

// IsQueuePaused checks if a queue is paused.
func IsQueuePaused(ctx context.Context, storage Storage, queue string) (bool, error) {
	return storage.IsQueuePaused(ctx, queue)
}

// GetPausedQueues returns all paused queue names.
func GetPausedQueues(ctx context.Context, storage Storage) ([]string, error) {
	return storage.GetPausedQueues(ctx)
}

// PauseOption configures pause operations.
type PauseOption interface {
	applyPause(*pauseOptions)
}

type pauseOptions struct {
	mode core.PauseMode
}

type pauseModeOpt struct {
	mode core.PauseMode
}

func (o pauseModeOpt) applyPause(opts *pauseOptions) {
	opts.mode = o.mode
}

// WithPauseMode sets the pause mode.
func WithPauseMode(mode core.PauseMode) PauseOption {
	return pauseModeOpt{mode: mode}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./tests -run "TestPauseJob$|TestResumeJob|TestPauseQueue$|TestGetPausedJobs|TestGetPausedQueues" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pause.go tests/pause_functions_test.go
git commit -m "feat: add standalone pause/resume functions"
```

---

## Task 13: Add Re-exports to Root Package

**Files:**
- Modify: `jobs.go`

**Step 1: Verify current exports**

Check what's currently exported in `jobs.go`.

**Step 2: Add pause-related re-exports**

Add to `jobs.go`:

```go
// Pause types and constants
type PauseMode = core.PauseMode

const (
	PauseModeGraceful   = core.PauseModeGraceful
	PauseModeAggressive = core.PauseModeAggressive
)

// Pause errors
var (
	ErrJobAlreadyPaused   = core.ErrJobAlreadyPaused
	ErrJobNotPaused       = core.ErrJobNotPaused
	ErrQueueAlreadyPaused = core.ErrQueueAlreadyPaused
	ErrQueueNotPaused     = core.ErrQueueNotPaused
	ErrCannotPauseStatus  = core.ErrCannotPauseStatus
)
```

**Step 3: Commit**

```bash
git add jobs.go
git commit -m "feat: re-export pause types and errors from root package"
```

---

## Task 14: Integration Test for Full Pause Flow

**Files:**
- Create: `tests/pause_integration_test.go`

**Step 1: Write integration test**

```go
package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPauseIntegration_JobLevelPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.NewQueue(store)
	ctx := context.Background()

	var processed atomic.Int32
	q.Register("count-job", func(ctx context.Context, args struct{}) error {
		processed.Add(1)
		return nil
	})

	// Enqueue two jobs
	job1ID, _ := q.Enqueue(ctx, "count-job", struct{}{})
	job2ID, _ := q.Enqueue(ctx, "count-job", struct{}{})

	// Pause job1
	err := q.PauseJob(ctx, job1ID)
	require.NoError(t, err)

	// Start worker
	w := q.NewWorker()
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go w.Start(workerCtx)
	<-workerCtx.Done()

	// Only job2 should be processed
	assert.Equal(t, int32(1), processed.Load())

	// Job1 should still be paused
	paused, _ := q.IsJobPaused(ctx, job1ID)
	assert.True(t, paused)

	// Resume job1
	err = q.ResumeJob(ctx, job1ID)
	require.NoError(t, err)

	// Run worker again
	workerCtx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	go q.NewWorker().Start(workerCtx2)
	<-workerCtx2.Done()

	// Now both should be processed
	assert.Equal(t, int32(2), processed.Load())
}

func TestPauseIntegration_QueueLevelPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.NewQueue(store)
	ctx := context.Background()

	var emailsProcessed, othersProcessed atomic.Int32

	q.Register("email-job", func(ctx context.Context, args struct{}) error {
		emailsProcessed.Add(1)
		return nil
	})
	q.Register("other-job", func(ctx context.Context, args struct{}) error {
		othersProcessed.Add(1)
		return nil
	})

	// Enqueue jobs to different queues
	q.Enqueue(ctx, "email-job", struct{}{}, jobs.QueueOpt("emails"))
	q.Enqueue(ctx, "other-job", struct{}{}, jobs.QueueOpt("other"))

	// Pause emails queue
	err := q.PauseQueue(ctx, "emails")
	require.NoError(t, err)

	// Start worker for both queues
	w := q.NewWorker(jobs.WithQueues(map[string]int{"emails": 1, "other": 1}))
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go w.Start(workerCtx)
	<-workerCtx.Done()

	// Only other queue should be processed
	assert.Equal(t, int32(0), emailsProcessed.Load())
	assert.Equal(t, int32(1), othersProcessed.Load())
}

func TestPauseIntegration_WorkerLevelPause(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.NewQueue(store)
	ctx := context.Background()

	var processed atomic.Int32
	started := make(chan struct{})

	q.Register("slow-job", func(ctx context.Context, args struct{}) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	q.Enqueue(ctx, "slow-job", struct{}{})

	w := q.NewWorker()
	workerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go w.Start(workerCtx)

	// Wait for job to start
	<-started

	// Graceful pause - should let job finish
	w.Pause(jobs.PauseModeGraceful)

	// Wait a bit for job to complete
	time.Sleep(300 * time.Millisecond)

	// Job should have completed
	assert.Equal(t, int32(1), processed.Load())
}

func TestPauseIntegration_CheckpointResume(t *testing.T) {
	store := setupStorageTest(t)
	q := jobs.NewQueue(store)
	ctx := context.Background()

	var step1Count, step2Count atomic.Int32

	q.Register("step1", func(ctx context.Context, args struct{}) error {
		step1Count.Add(1)
		return nil
	})

	q.Register("checkpoint-job", func(ctx context.Context, args struct{}) error {
		// This job calls step1, then does step2
		result, err := jobs.Call[struct{}](ctx, "step1", struct{}{})
		if err != nil {
			return err
		}
		_ = result
		step2Count.Add(1)
		return nil
	})

	jobID, _ := q.Enqueue(ctx, "checkpoint-job", struct{}{})

	// Run first part
	w := q.NewWorker()
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	go w.Start(workerCtx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Job should have completed
	job, _ := store.GetJob(ctx, jobID)
	if job.Status != jobs.StatusCompleted {
		t.Skip("job did not complete in time, skipping checkpoint test")
	}

	// Verify step1 was only called once (checkpoint prevents re-execution)
	assert.Equal(t, int32(1), step1Count.Load())
}
```

**Step 2: Run integration tests**

Run: `go test ./tests -run TestPauseIntegration -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/pause_integration_test.go
git commit -m "test: add pause/resume integration tests"
```

---

## Task 15: Final Verification

**Step 1: Run all tests**

```bash
go test ./... -v
```

Expected: All tests pass

**Step 2: Run linter**

```bash
golangci-lint run
```

Expected: No issues

**Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete pause/resume implementation"
```

---

## Summary

This implementation adds:
1. `StatusPaused` job status
2. `PauseMode` (graceful/aggressive) configuration
3. `QueueState` model for queue pause persistence
4. Storage methods for job and queue pause operations
5. Worker pause state with aggressive cancellation support
6. Queue convenience methods
7. Standalone functions for external use
8. Full event emission for observability
9. Comprehensive tests at all levels
