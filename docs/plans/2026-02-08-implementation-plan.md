# Durable Jobs Library Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Go library for durable job queues with nested checkpointed calls, GORM storage, retries, scheduling, and observability.

**Architecture:** Function-based job registration with `jobs.Call()` for durable nested calls. GORM storage backend for broad database support. Two-layer retries (call + job level). Worker pools with named queues and priorities.

**Tech Stack:** Go 1.21+, GORM, robfig/cron, testify, SQLite (tests)

---

## Phase 1: Project Setup

### Task 1.1: Initialize Go Module

**Files:**
- Create: `go.mod`
- Create: `go.sum`

**Step 1: Initialize module**

Run:
```bash
go mod init github.com/jdziat/simple-durable-jobs
```

**Step 2: Add core dependencies**

Run:
```bash
go get gorm.io/gorm
go get gorm.io/driver/sqlite
go get github.com/stretchr/testify
go get github.com/robfig/cron/v3
go get github.com/google/uuid
```

**Step 3: Verify go.mod**

Run: `cat go.mod`
Expected: Module with dependencies listed

**Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: initialize Go module with dependencies"
```

---

### Task 1.2: Create Directory Structure

**Files:**
- Create: `jobs.go` (package declaration only)
- Create: `jobs_test.go` (package declaration only)
- Create: `storage/storage.go`
- Create: `storage/gorm.go`
- Create: `internal/context.go`

**Step 1: Create package files**

Create `jobs.go`:
```go
// Package jobs provides a durable job queue with checkpointed nested calls.
package jobs
```

Create `jobs_test.go`:
```go
package jobs_test
```

Create `storage/storage.go`:
```go
// Package storage defines the storage interface for the jobs library.
package storage
```

Create `storage/gorm.go`:
```go
package storage
```

Create `internal/context.go`:
```go
// Package internal contains internal utilities.
package internal
```

**Step 2: Verify structure**

Run: `go build ./...`
Expected: No errors

**Step 3: Commit**

```bash
git add .
git commit -m "chore: create package directory structure"
```

---

## Phase 2: Core Types

### Task 2.1: Define Job Struct

**Files:**
- Modify: `jobs.go`
- Create: `job_test.go`

**Step 1: Write test for Job struct**

Create `job_test.go`:
```go
package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestJob_HasRequiredFields(t *testing.T) {
	job := jobs.Job{
		ID:        "test-123",
		Type:      "send-email",
		Args:      []byte(`{"to":"user@example.com"}`),
		Queue:     "default",
		Priority:  0,
		Status:    jobs.StatusPending,
		Attempt:   0,
		MaxRetries: 3,
		CreatedAt: time.Now(),
	}

	assert.Equal(t, "test-123", job.ID)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, jobs.StatusPending, job.Status)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestJob_HasRequiredFields`
Expected: FAIL - undefined: jobs.Job

**Step 3: Implement Job struct**

Update `jobs.go`:
```go
// Package jobs provides a durable job queue with checkpointed nested calls.
package jobs

import (
	"time"
)

// JobStatus represents the current state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusRetrying  JobStatus = "retrying"
)

// Job represents a unit of work to be processed.
type Job struct {
	ID          string          `gorm:"primaryKey;size:36"`
	Type        string          `gorm:"index;size:255;not null"`
	Args        []byte          `gorm:"type:bytes"`
	Queue       string          `gorm:"index;size:255;default:'default'"`
	Priority    int             `gorm:"index;default:0"`
	Status      JobStatus       `gorm:"index;size:20;default:'pending'"`
	Attempt     int             `gorm:"default:0"`
	MaxRetries  int             `gorm:"default:3"`
	LastError   string          `gorm:"type:text"`
	RunAt       *time.Time      `gorm:"index"`
	StartedAt   *time.Time
	CompletedAt *time.Time
	CreatedAt   time.Time       `gorm:"autoCreateTime"`
	UpdatedAt   time.Time       `gorm:"autoUpdateTime"`
	LockedBy    string          `gorm:"size:255"`
	LockedUntil *time.Time      `gorm:"index"`
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run TestJob_HasRequiredFields`
Expected: PASS

**Step 5: Commit**

```bash
git add jobs.go job_test.go
git commit -m "feat: add Job struct with status constants"
```

---

### Task 2.2: Define Checkpoint Struct

**Files:**
- Modify: `jobs.go`
- Modify: `job_test.go`

**Step 1: Write test for Checkpoint**

Add to `job_test.go`:
```go
func TestCheckpoint_HasRequiredFields(t *testing.T) {
	cp := jobs.Checkpoint{
		ID:        "cp-123",
		JobID:     "job-456",
		CallIndex: 0,
		CallType:  "charge-card",
		Result:    []byte(`{"receipt":"abc"}`),
	}

	assert.Equal(t, "cp-123", cp.ID)
	assert.Equal(t, "job-456", cp.JobID)
	assert.Equal(t, 0, cp.CallIndex)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestCheckpoint_HasRequiredFields`
Expected: FAIL - undefined: jobs.Checkpoint

**Step 3: Implement Checkpoint struct**

Add to `jobs.go`:
```go
// Checkpoint stores the result of a durable Call() for replay.
type Checkpoint struct {
	ID        string    `gorm:"primaryKey;size:36"`
	JobID     string    `gorm:"index;size:36;not null"`
	CallIndex int       `gorm:"not null"`
	CallType  string    `gorm:"size:255;not null"`
	Result    []byte    `gorm:"type:bytes"`
	Error     string    `gorm:"type:text"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run TestCheckpoint_HasRequiredFields`
Expected: PASS

**Step 5: Commit**

```bash
git add jobs.go job_test.go
git commit -m "feat: add Checkpoint struct for durable calls"
```

---

### Task 2.3: Define Options

**Files:**
- Create: `options.go`
- Create: `options_test.go`

**Step 1: Write test for options**

Create `options_test.go`:
```go
package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestOptions_Queue(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.QueueOpt("emails").Apply(opts)
	assert.Equal(t, "emails", opts.Queue)
}

func TestOptions_Priority(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Priority(100).Apply(opts)
	assert.Equal(t, 100, opts.Priority)
}

func TestOptions_Retries(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Retries(5).Apply(opts)
	assert.Equal(t, 5, opts.MaxRetries)
}

func TestOptions_Delay(t *testing.T) {
	opts := jobs.NewOptions()
	jobs.Delay(time.Hour).Apply(opts)
	assert.Equal(t, time.Hour, opts.Delay)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestOptions`
Expected: FAIL - undefined: jobs.NewOptions

**Step 3: Implement options**

Create `options.go`:
```go
package jobs

import "time"

// Options holds configuration for job enqueueing and registration.
type Options struct {
	Queue       string
	Priority    int
	MaxRetries  int
	Delay       time.Duration
	RunAt       *time.Time
	UniqueKey   string
	Determinism DeterminismMode
}

// NewOptions creates Options with defaults.
func NewOptions() *Options {
	return &Options{
		Queue:      "default",
		Priority:   0,
		MaxRetries: DefaultJobRetries,
	}
}

// Option modifies Options.
type Option interface {
	Apply(*Options)
}

type optionFunc func(*Options)

func (f optionFunc) Apply(o *Options) { f(o) }

// QueueOpt sets the queue name.
func QueueOpt(name string) Option {
	return optionFunc(func(o *Options) {
		o.Queue = name
	})
}

// Priority sets the job priority (higher = runs first).
func Priority(p int) Option {
	return optionFunc(func(o *Options) {
		o.Priority = p
	})
}

// Retries sets the maximum retry count.
func Retries(n int) Option {
	return optionFunc(func(o *Options) {
		o.MaxRetries = n
	})
}

// Delay schedules the job to run after a duration.
func Delay(d time.Duration) Option {
	return optionFunc(func(o *Options) {
		o.Delay = d
	})
}

// At schedules the job to run at a specific time.
func At(t time.Time) Option {
	return optionFunc(func(o *Options) {
		o.RunAt = &t
	})
}

// Unique ensures only one job with this key exists.
func Unique(key string) Option {
	return optionFunc(func(o *Options) {
		o.UniqueKey = key
	})
}

// DeterminismMode controls replay strictness.
type DeterminismMode int

const (
	ExplicitCheckpoints DeterminismMode = iota
	Strict
	BestEffort
)

// Determinism sets the replay mode.
func Determinism(mode DeterminismMode) Option {
	return optionFunc(func(o *Options) {
		o.Determinism = mode
	})
}

// Default values.
var (
	DefaultJobRetries  = 2
	DefaultCallRetries = 3
)
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run TestOptions`
Expected: PASS

**Step 5: Commit**

```bash
git add options.go options_test.go
git commit -m "feat: add job options (queue, priority, retries, delay)"
```

---

### Task 2.4: Define Error Types

**Files:**
- Create: `errors.go`
- Create: `errors_test.go`

**Step 1: Write test for error types**

Create `errors_test.go`:
```go
package jobs_test

import (
	"errors"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestNoRetry_WrapsError(t *testing.T) {
	original := errors.New("invalid input")
	wrapped := jobs.NoRetry(original)

	var noRetryErr *jobs.NoRetryError
	assert.True(t, errors.As(wrapped, &noRetryErr))
	assert.Equal(t, original, noRetryErr.Unwrap())
}

func TestRetryAfter_HasDelay(t *testing.T) {
	original := errors.New("rate limited")
	wrapped := jobs.RetryAfter(30*time.Second, original)

	var retryErr *jobs.RetryAfterError
	assert.True(t, errors.As(wrapped, &retryErr))
	assert.Equal(t, 30*time.Second, retryErr.Delay)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestNoRetry`
Expected: FAIL - undefined: jobs.NoRetry

**Step 3: Implement error types**

Create `errors.go`:
```go
package jobs

import (
	"fmt"
	"time"
)

// NoRetryError indicates an error that should not be retried.
type NoRetryError struct {
	Err error
}

func (e *NoRetryError) Error() string {
	return fmt.Sprintf("no retry: %v", e.Err)
}

func (e *NoRetryError) Unwrap() error {
	return e.Err
}

// NoRetry wraps an error to indicate it should not be retried.
func NoRetry(err error) error {
	return &NoRetryError{Err: err}
}

// RetryAfterError indicates an error that should be retried after a delay.
type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("retry after %v: %v", e.Delay, e.Err)
}

func (e *RetryAfterError) Unwrap() error {
	return e.Err
}

// RetryAfter wraps an error to indicate it should be retried after a delay.
func RetryAfter(d time.Duration, err error) error {
	return &RetryAfterError{Err: err, Delay: d}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run "TestNoRetry|TestRetryAfter"`
Expected: PASS

**Step 5: Commit**

```bash
git add errors.go errors_test.go
git commit -m "feat: add NoRetry and RetryAfter error types"
```

---

## Phase 3: Storage Interface & GORM Implementation

### Task 3.1: Define Storage Interface

**Files:**
- Modify: `storage/storage.go`

**Step 1: Define interface**

Update `storage/storage.go`:
```go
// Package storage defines the storage interface for the jobs library.
package storage

import (
	"context"
	"time"

	"github.com/jdziat/simple-durable-jobs"
)

// Storage defines the persistence layer for jobs.
type Storage interface {
	// Migrate creates the necessary database tables.
	Migrate(ctx context.Context) error

	// Job lifecycle
	Enqueue(ctx context.Context, job *jobs.Job) error
	Dequeue(ctx context.Context, queues []string, workerID string) (*jobs.Job, error)
	Complete(ctx context.Context, jobID string, result []byte) error
	Fail(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) error

	// Checkpointing
	SaveCheckpoint(ctx context.Context, cp *jobs.Checkpoint) error
	GetCheckpoints(ctx context.Context, jobID string) ([]jobs.Checkpoint, error)
	DeleteCheckpoints(ctx context.Context, jobID string) error

	// Scheduling
	GetDueJobs(ctx context.Context, queues []string, limit int) ([]*jobs.Job, error)

	// Locking
	Heartbeat(ctx context.Context, jobID string, workerID string) error
	ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error)

	// Queries
	GetJob(ctx context.Context, jobID string) (*jobs.Job, error)
	GetJobsByStatus(ctx context.Context, status jobs.JobStatus, limit int) ([]*jobs.Job, error)
}
```

**Step 2: Verify it compiles**

Run: `go build ./...`
Expected: No errors

**Step 3: Commit**

```bash
git add storage/storage.go
git commit -m "feat: define Storage interface"
```

---

### Task 3.2: Implement GORM Storage

**Files:**
- Modify: `storage/gorm.go`
- Create: `storage/gorm_test.go`

**Step 1: Write test for Enqueue/Dequeue**

Create `storage/gorm_test.go`:
```go
package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	return db
}

func TestGormStorage_EnqueueAndDequeue(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	job := &jobs.Job{
		ID:     "job-123",
		Type:   "send-email",
		Args:   []byte(`{"to":"test@example.com"}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}

	err = store.Enqueue(ctx, job)
	require.NoError(t, err)

	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	assert.Equal(t, "job-123", dequeued.ID)
	assert.Equal(t, jobs.StatusRunning, dequeued.Status)
	assert.Equal(t, "worker-1", dequeued.LockedBy)
}

func TestGormStorage_DequeueRespectsQueue(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	job := &jobs.Job{
		ID:    "job-123",
		Type:  "report",
		Queue: "reports",
	}
	err = store.Enqueue(ctx, job)
	require.NoError(t, err)

	// Try to dequeue from different queue
	dequeued, err := store.Dequeue(ctx, []string{"emails"}, "worker-1")
	require.NoError(t, err)
	assert.Nil(t, dequeued)

	// Dequeue from correct queue
	dequeued, err = store.Dequeue(ctx, []string{"reports"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, "job-123", dequeued.ID)
}

func TestGormStorage_DequeueRespectsPriority(t *testing.T) {
	db := setupTestDB(t)
	store := storage.NewGormStorage(db)
	ctx := context.Background()

	err := store.Migrate(ctx)
	require.NoError(t, err)

	// Enqueue low priority first
	lowPri := &jobs.Job{ID: "low", Type: "task", Queue: "default", Priority: 1}
	highPri := &jobs.Job{ID: "high", Type: "task", Queue: "default", Priority: 100}

	store.Enqueue(ctx, lowPri)
	store.Enqueue(ctx, highPri)

	// High priority should come first
	dequeued, _ := store.Dequeue(ctx, []string{"default"}, "worker-1")
	assert.Equal(t, "high", dequeued.ID)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./storage/... -v -run TestGormStorage`
Expected: FAIL - undefined: storage.NewGormStorage

**Step 3: Implement GORM storage**

Update `storage/gorm.go`:
```go
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GormStorage implements Storage using GORM.
type GormStorage struct {
	db *gorm.DB
}

// NewGormStorage creates a new GORM-backed storage.
func NewGormStorage(db *gorm.DB) *GormStorage {
	return &GormStorage{db: db}
}

// Migrate creates the necessary tables.
func (s *GormStorage) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&jobs.Job{}, &jobs.Checkpoint{})
}

// Enqueue adds a job to the queue.
func (s *GormStorage) Enqueue(ctx context.Context, job *jobs.Job) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.Status == "" {
		job.Status = jobs.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	return s.db.WithContext(ctx).Create(job).Error
}

// Dequeue fetches and locks the next available job.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*jobs.Job, error) {
	var job jobs.Job
	now := time.Now()
	lockUntil := now.Add(5 * time.Minute)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find next available job
		result := tx.
			Where("queue IN ?", queues).
			Where("status = ?", jobs.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", now).
			Where("(locked_until IS NULL OR locked_until < ?)", now).
			Order("priority DESC, created_at ASC").
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		// Lock the job
		job.Status = jobs.StatusRunning
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

// Complete marks a job as successfully completed.
func (s *GormStorage) Complete(ctx context.Context, jobID string, result []byte) error {
	now := time.Now()
	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ?", jobID).
		Updates(map[string]interface{}{
			"status":       jobs.StatusCompleted,
			"completed_at": now,
			"locked_by":    nil,
			"locked_until": nil,
		}).Error
}

// Fail marks a job as failed, optionally scheduling a retry.
func (s *GormStorage) Fail(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) error {
	updates := map[string]interface{}{
		"last_error":   errMsg,
		"locked_by":    nil,
		"locked_until": nil,
	}

	if retryAt != nil {
		updates["status"] = jobs.StatusPending
		updates["run_at"] = retryAt
	} else {
		updates["status"] = jobs.StatusFailed
		now := time.Now()
		updates["completed_at"] = now
	}

	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ?", jobID).
		Updates(updates).Error
}

// SaveCheckpoint stores a checkpoint for a durable call.
func (s *GormStorage) SaveCheckpoint(ctx context.Context, cp *jobs.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(cp).Error
}

// GetCheckpoints retrieves all checkpoints for a job.
func (s *GormStorage) GetCheckpoints(ctx context.Context, jobID string) ([]jobs.Checkpoint, error) {
	var checkpoints []jobs.Checkpoint
	err := s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Order("call_index ASC").
		Find(&checkpoints).Error
	return checkpoints, err
}

// DeleteCheckpoints removes all checkpoints for a job.
func (s *GormStorage) DeleteCheckpoints(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Delete(&jobs.Checkpoint{}).Error
}

// GetDueJobs returns jobs ready to run.
func (s *GormStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*jobs.Job, error) {
	var jobList []*jobs.Job
	now := time.Now()

	err := s.db.WithContext(ctx).
		Where("queue IN ?", queues).
		Where("status = ?", jobs.StatusPending).
		Where("(run_at IS NULL OR run_at <= ?)", now).
		Where("(locked_until IS NULL OR locked_until < ?)", now).
		Order("priority DESC, created_at ASC").
		Limit(limit).
		Find(&jobList).Error

	return jobList, err
}

// Heartbeat extends the lock on a running job.
func (s *GormStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	lockUntil := time.Now().Add(5 * time.Minute)
	return s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Update("locked_until", lockUntil).Error
}

// ReleaseStaleLocks releases locks on jobs that haven't had a heartbeat.
func (s *GormStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) (int64, error) {
	cutoff := time.Now().Add(-staleDuration)
	result := s.db.WithContext(ctx).
		Model(&jobs.Job{}).
		Where("status = ?", jobs.StatusRunning).
		Where("locked_until < ?", cutoff).
		Updates(map[string]interface{}{
			"status":       jobs.StatusPending,
			"locked_by":    nil,
			"locked_until": nil,
		})
	return result.RowsAffected, result.Error
}

// GetJob retrieves a job by ID.
func (s *GormStorage) GetJob(ctx context.Context, jobID string) (*jobs.Job, error) {
	var job jobs.Job
	err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &job, err
}

// GetJobsByStatus retrieves jobs by status.
func (s *GormStorage) GetJobsByStatus(ctx context.Context, status jobs.JobStatus, limit int) ([]*jobs.Job, error) {
	var jobList []*jobs.Job
	err := s.db.WithContext(ctx).
		Where("status = ?", status).
		Limit(limit).
		Find(&jobList).Error
	return jobList, err
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./storage/... -v -run TestGormStorage`
Expected: PASS (some may fail due to SQLite not supporting SKIP LOCKED - we'll handle this)

**Step 5: Fix SQLite compatibility**

The SKIP LOCKED clause doesn't work with SQLite. Update `Dequeue` to handle this:

Update the Dequeue method in `storage/gorm.go` to use a simpler locking approach for SQLite:
```go
// Dequeue fetches and locks the next available job.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*jobs.Job, error) {
	var job jobs.Job
	now := time.Now()
	lockUntil := now.Add(5 * time.Minute)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find next available job
		result := tx.
			Where("queue IN ?", queues).
			Where("status = ?", jobs.StatusPending).
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

		// Lock the job
		job.Status = jobs.StatusRunning
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

**Step 6: Run tests again**

Run: `go test ./storage/... -v`
Expected: PASS

**Step 7: Commit**

```bash
git add storage/gorm.go storage/gorm_test.go
git commit -m "feat: implement GORM storage backend"
```

---

## Phase 4: Queue Core

### Task 4.1: Implement Queue and Register

**Files:**
- Create: `queue.go`
- Create: `queue_test.go`

**Step 1: Write test for Register**

Create `queue_test.go`:
```go
package jobs_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
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
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestQueue_Register`
Expected: FAIL - undefined: jobs.New

**Step 3: Implement Queue**

Create `queue.go`:
```go
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/storage"
)

// Queue manages job registration, enqueueing, and processing.
type Queue struct {
	storage  storage.Storage
	handlers map[string]*jobHandler
	mu       sync.RWMutex

	// Hooks
	onStart    []func(context.Context, *Job)
	onComplete []func(context.Context, *Job)
	onFail     []func(context.Context, *Job, error)
	onRetry    []func(context.Context, *Job, int, error)

	// Event stream
	events     chan Event
	eventSubs  []chan Event

	// Config
	determinism DeterminismMode
}

type jobHandler struct {
	fn         reflect.Value
	argsType   reflect.Type
	hasContext bool
}

// New creates a new Queue with the given storage backend.
func New(s storage.Storage) *Queue {
	return &Queue{
		storage:     s,
		handlers:    make(map[string]*jobHandler),
		determinism: ExplicitCheckpoints,
		events:      make(chan Event, 1000),
	}
}

// Register registers a job handler function.
// The function must have signature: func(ctx context.Context, args T) error
func (q *Queue) Register(name string, fn interface{}, opts ...Option) {
	q.mu.Lock()
	defer q.mu.Unlock()

	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		panic(fmt.Sprintf("jobs: handler for %q must be a function", name))
	}

	handler := &jobHandler{fn: fnVal}

	// Parse function signature
	numIn := fnType.NumIn()
	if numIn < 1 || numIn > 2 {
		panic(fmt.Sprintf("jobs: handler for %q must have 1-2 arguments", name))
	}

	argIdx := 0
	if fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		handler.hasContext = true
		argIdx = 1
	}

	if argIdx < numIn {
		handler.argsType = fnType.In(argIdx)
	}

	// Validate return type
	if fnType.NumOut() != 1 || !fnType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("jobs: handler for %q must return error", name))
	}

	q.handlers[name] = handler
}

// HasHandler checks if a handler is registered.
func (q *Queue) HasHandler(name string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.handlers[name]
	return ok
}

// Enqueue adds a job to the queue.
func (q *Queue) Enqueue(ctx context.Context, name string, args interface{}, opts ...Option) (string, error) {
	q.mu.RLock()
	_, ok := q.handlers[name]
	q.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("jobs: no handler registered for %q", name)
	}

	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("jobs: failed to marshal args: %w", err)
	}

	job := &Job{
		ID:         uuid.New().String(),
		Type:       name,
		Args:       argsBytes,
		Queue:      options.Queue,
		Priority:   options.Priority,
		MaxRetries: options.MaxRetries,
		Status:     StatusPending,
	}

	if options.Delay > 0 {
		runAt := now().Add(options.Delay)
		job.RunAt = &runAt
	}
	if options.RunAt != nil {
		job.RunAt = options.RunAt
	}

	if err := q.storage.Enqueue(ctx, job); err != nil {
		return "", fmt.Errorf("jobs: failed to enqueue: %w", err)
	}

	return job.ID, nil
}

// Storage returns the underlying storage.
func (q *Queue) Storage() storage.Storage {
	return q.storage
}

// SetDeterminism sets the default determinism mode.
func (q *Queue) SetDeterminism(mode DeterminismMode) {
	q.determinism = mode
}

// now is a variable so it can be mocked in tests
var now = func() time.Time { return time.Now() }
```

Add the missing import at the top:
```go
import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/storage"
)
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run TestQueue_Register`
Expected: PASS

**Step 5: Commit**

```bash
git add queue.go queue_test.go
git commit -m "feat: add Queue with Register and Enqueue"
```

---

### Task 4.2: Add Enqueue Test

**Files:**
- Modify: `queue_test.go`

**Step 1: Write test for Enqueue**

Add to `queue_test.go`:
```go
func TestQueue_Enqueue(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	type EmailParams struct {
		To      string
		Subject string
	}

	queue.Register("send-email", func(ctx context.Context, p EmailParams) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "send-email", EmailParams{
		To:      "test@example.com",
		Subject: "Hello",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, jobID)

	// Verify job in database
	var job jobs.Job
	err = db.First(&job, "id = ?", jobID).Error
	require.NoError(t, err)
	assert.Equal(t, "send-email", job.Type)
	assert.Equal(t, "default", job.Queue)
}

func TestQueue_EnqueueWithOptions(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	queue.Register("task", func(ctx context.Context, args string) error {
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "task", "data",
		jobs.QueueOpt("critical"),
		jobs.Priority(100),
		jobs.Retries(5),
	)

	require.NoError(t, err)

	var job jobs.Job
	db.First(&job, "id = ?", jobID)

	assert.Equal(t, "critical", job.Queue)
	assert.Equal(t, 100, job.Priority)
	assert.Equal(t, 5, job.MaxRetries)
}
```

**Step 2: Run test**

Run: `go test ./... -v -run TestQueue_Enqueue`
Expected: PASS

**Step 3: Commit**

```bash
git add queue_test.go
git commit -m "test: add Queue.Enqueue tests"
```

---

## Phase 5: Worker

### Task 5.1: Implement Basic Worker

**Files:**
- Create: `worker.go`
- Create: `worker_test.go`

**Step 1: Write test for basic worker**

Create `worker_test.go`:
```go
package jobs_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestWorker_ProcessesJob(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var processed atomic.Bool
	queue.Register("test-job", func(ctx context.Context, msg string) error {
		processed.Store(true)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test-job", "hello")

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	cancel()

	assert.True(t, processed.Load())
}

func TestWorker_MarksJobComplete(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	queue.Register("test-job", func(ctx context.Context, msg string) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, _ := queue.Enqueue(ctx, "test-job", "hello")

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	job, _ := store.GetJob(context.Background(), jobID)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestWorker`
Expected: FAIL - undefined: queue.NewWorker

**Step 3: Implement Worker**

Create `worker.go`:
```go
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

// WorkerOption configures a Worker.
type WorkerOption interface {
	ApplyWorker(*WorkerConfig)
}

type workerOptionFunc func(*WorkerConfig)

func (f workerOptionFunc) ApplyWorker(c *WorkerConfig) { f(c) }

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	Queues       map[string]int // queue name -> concurrency
	PollInterval time.Duration
	WorkerID     string
}

// Concurrency sets the concurrency for a queue.
func Concurrency(n int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if len(c.Queues) == 0 {
			c.Queues["default"] = n
		} else {
			// Apply to last added queue
			for k := range c.Queues {
				c.Queues[k] = n
			}
		}
	})
}

// Queue adds a queue to process.
func Queue(name string, opts ...WorkerOption) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.Queues[name] = 10 // default concurrency
		for _, opt := range opts {
			opt.ApplyWorker(c)
		}
	})
}

// Worker processes jobs from the queue.
type Worker struct {
	queue    *Queue
	config   WorkerConfig
	logger   *slog.Logger
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewWorker creates a new worker.
func (q *Queue) NewWorker(opts ...WorkerOption) *Worker {
	config := WorkerConfig{
		Queues:       map[string]int{"default": 10},
		PollInterval: 100 * time.Millisecond,
		WorkerID:     uuid.New().String(),
	}

	for _, opt := range opts {
		opt.ApplyWorker(&config)
	}

	return &Worker{
		queue:  q,
		config: config,
		logger: slog.Default(),
	}
}

// Start begins processing jobs. Blocks until context is cancelled.
func (w *Worker) Start(ctx context.Context) error {
	queues := make([]string, 0, len(w.config.Queues))
	for q := range w.config.Queues {
		queues = append(queues, q)
	}

	// Calculate total concurrency
	totalConcurrency := 0
	for _, c := range w.config.Queues {
		totalConcurrency += c
	}

	// Job channel
	jobsChan := make(chan *Job, totalConcurrency)

	// Start workers
	for i := 0; i < totalConcurrency; i++ {
		w.wg.Add(1)
		go w.processLoop(ctx, jobsChan)
	}

	// Poll for jobs
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobsChan)
			w.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			job, err := w.queue.storage.Dequeue(ctx, queues, w.config.WorkerID)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					w.logger.Error("failed to dequeue", "error", err)
				}
				continue
			}
			if job != nil {
				select {
				case jobsChan <- job:
				case <-ctx.Done():
				}
			}
		}
	}
}

func (w *Worker) processLoop(ctx context.Context, jobs <-chan *Job) {
	defer w.wg.Done()

	for job := range jobs {
		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *Job) {
	w.queue.mu.RLock()
	handler, ok := w.queue.handlers[job.Type]
	w.queue.mu.RUnlock()

	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		w.queue.storage.Fail(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Call hooks
	for _, fn := range w.queue.onStart {
		fn(ctx, job)
	}

	// Execute handler
	err := w.executeHandler(ctx, job, handler)

	if err != nil {
		w.handleError(ctx, job, err)
	} else {
		w.queue.storage.Complete(ctx, job.ID, nil)
		for _, fn := range w.queue.onComplete {
			fn(ctx, job)
		}
	}
}

func (w *Worker) executeHandler(ctx context.Context, job *Job, handler *jobHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Build arguments
	var args []reflect.Value

	if handler.hasContext {
		// Create job context with metadata
		jobCtx := withJobContext(ctx, job, w.queue)
		args = append(args, reflect.ValueOf(jobCtx))
	}

	if handler.argsType != nil {
		argVal := reflect.New(handler.argsType)
		if err := json.Unmarshal(job.Args, argVal.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal args: %w", err)
		}
		args = append(args, argVal.Elem())
	}

	// Call handler
	results := handler.fn.Call(args)

	if !results[0].IsNil() {
		return results[0].Interface().(error)
	}
	return nil
}

func (w *Worker) handleError(ctx context.Context, job *Job, err error) {
	// Check for NoRetry
	var noRetry *NoRetryError
	if errors.As(err, &noRetry) {
		w.queue.storage.Fail(ctx, job.ID, err.Error(), nil)
		for _, fn := range w.queue.onFail {
			fn(ctx, job, err)
		}
		return
	}

	// Check for RetryAfter
	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		if job.Attempt < job.MaxRetries {
			retryAt := time.Now().Add(retryAfter.Delay)
			w.queue.storage.Fail(ctx, job.ID, err.Error(), &retryAt)
			for _, fn := range w.queue.onRetry {
				fn(ctx, job, job.Attempt, err)
			}
			return
		}
	}

	// Normal retry logic
	if job.Attempt < job.MaxRetries {
		backoff := w.calculateBackoff(job.Attempt)
		retryAt := time.Now().Add(backoff)
		w.queue.storage.Fail(ctx, job.ID, err.Error(), &retryAt)
		for _, fn := range w.queue.onRetry {
			fn(ctx, job, job.Attempt, err)
		}
	} else {
		w.queue.storage.Fail(ctx, job.ID, err.Error(), nil)
		for _, fn := range w.queue.onFail {
			fn(ctx, job, err)
		}
	}
}

func (w *Worker) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: 1s, 2s, 4s, 8s, ...
	base := time.Second
	backoff := base * (1 << attempt)
	if backoff > time.Minute {
		backoff = time.Minute
	}
	return backoff
}
```

**Step 4: Add job context helper**

Add to `queue.go` or create `internal/context.go`:

Add to `queue.go`:
```go
type jobContextKey struct{}

type jobContext struct {
	job   *Job
	queue *Queue
}

func withJobContext(ctx context.Context, job *Job, q *Queue) context.Context {
	return context.WithValue(ctx, jobContextKey{}, &jobContext{job: job, queue: q})
}

func getJobContext(ctx context.Context) *jobContext {
	if jc, ok := ctx.Value(jobContextKey{}).(*jobContext); ok {
		return jc
	}
	return nil
}
```

**Step 5: Run test to verify it passes**

Run: `go test ./... -v -run TestWorker`
Expected: PASS

**Step 6: Commit**

```bash
git add worker.go queue.go
git commit -m "feat: add Worker for job processing"
```

---

## Phase 6: Checkpointing (jobs.Call)

### Task 6.1: Implement jobs.Call

**Files:**
- Create: `call.go`
- Create: `call_test.go`

**Step 1: Write test for Call**

Create `call_test.go`:
```go
package jobs_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestCall_ExecutesNestedJob(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var callCount atomic.Int32
	queue.Register("nested", func(ctx context.Context, n int) (int, error) {
		callCount.Add(1)
		return n * 2, nil
	})

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[int](ctx, "nested", 5)
		if err != nil {
			return err
		}
		assert.Equal(t, 10, result)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "parent", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(1), callCount.Load())
}

func TestCall_ReturnsCheckpointedResult(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var callCount atomic.Int32
	queue.Register("nested", func(ctx context.Context, n int) (int, error) {
		callCount.Add(1)
		return n * 2, nil
	})

	// Simulate pre-existing checkpoint
	jobID := "test-job"
	checkpoint := &jobs.Checkpoint{
		ID:        "cp-1",
		JobID:     jobID,
		CallIndex: 0,
		CallType:  "nested",
		Result:    []byte(`100`), // Pre-computed result
	}
	store.SaveCheckpoint(context.Background(), checkpoint)

	queue.Register("parent", func(ctx context.Context, _ struct{}) error {
		result, err := jobs.Call[int](ctx, "nested", 5)
		require.NoError(t, err)
		assert.Equal(t, 100, result) // Should get checkpointed result
		return nil
	})

	// Create job with pre-existing checkpoints
	job := &jobs.Job{
		ID:     jobID,
		Type:   "parent",
		Args:   []byte(`{}`),
		Queue:  "default",
		Status: jobs.StatusPending,
	}
	store.Enqueue(context.Background(), job)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	cancel()

	// Nested should not have been called (checkpoint existed)
	assert.Equal(t, int32(0), callCount.Load())
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestCall`
Expected: FAIL - undefined: jobs.Call

**Step 3: Implement Call**

Create `call.go`:
```go
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/google/uuid"
)

// callState tracks the current call index for replay.
type callState struct {
	mu          sync.Mutex
	callIndex   int
	checkpoints map[int]*Checkpoint
}

type callStateKey struct{}

func getCallState(ctx context.Context) *callState {
	if cs, ok := ctx.Value(callStateKey{}).(*callState); ok {
		return cs
	}
	return nil
}

func withCallState(ctx context.Context, checkpoints []Checkpoint) context.Context {
	cs := &callState{
		checkpoints: make(map[int]*Checkpoint),
	}
	for i := range checkpoints {
		cs.checkpoints[checkpoints[i].CallIndex] = &checkpoints[i]
	}
	return context.WithValue(ctx, callStateKey{}, cs)
}

// Call executes a durable nested job call.
// Results are checkpointed; on replay, cached results are returned without re-execution.
func Call[T any](ctx context.Context, name string, args interface{}, opts ...Option) (T, error) {
	var zero T

	jc := getJobContext(ctx)
	if jc == nil {
		return zero, fmt.Errorf("jobs.Call must be used within a job handler")
	}

	cs := getCallState(ctx)
	if cs == nil {
		return zero, fmt.Errorf("jobs.Call: call state not initialized")
	}

	cs.mu.Lock()
	callIndex := cs.callIndex
	cs.callIndex++
	checkpoint, hasCheckpoint := cs.checkpoints[callIndex]
	cs.mu.Unlock()

	// If we have a checkpoint, return the cached result
	if hasCheckpoint {
		if checkpoint.Error != "" {
			return zero, fmt.Errorf("%s", checkpoint.Error)
		}
		var result T
		if err := json.Unmarshal(checkpoint.Result, &result); err != nil {
			return zero, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}
		return result, nil
	}

	// Execute the nested call
	jc.queue.mu.RLock()
	handler, ok := jc.queue.handlers[name]
	jc.queue.mu.RUnlock()

	if !ok {
		return zero, fmt.Errorf("no handler registered for %q", name)
	}

	result, err := executeCall[T](ctx, handler, args)

	// Save checkpoint
	cp := &Checkpoint{
		ID:        uuid.New().String(),
		JobID:     jc.job.ID,
		CallIndex: callIndex,
		CallType:  name,
	}

	if err != nil {
		cp.Error = err.Error()
	} else {
		resultBytes, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return zero, fmt.Errorf("failed to marshal result: %w", marshalErr)
		}
		cp.Result = resultBytes
	}

	if saveErr := jc.queue.storage.SaveCheckpoint(ctx, cp); saveErr != nil {
		return zero, fmt.Errorf("failed to save checkpoint: %w", saveErr)
	}

	return result, err
}

func executeCall[T any](ctx context.Context, handler *jobHandler, args interface{}) (T, error) {
	var zero T

	// Build arguments
	var callArgs []reflect.Value

	if handler.hasContext {
		callArgs = append(callArgs, reflect.ValueOf(ctx))
	}

	if handler.argsType != nil {
		argsVal := reflect.ValueOf(args)
		if argsVal.Type() != handler.argsType {
			// Try to convert via JSON
			argsBytes, err := json.Marshal(args)
			if err != nil {
				return zero, fmt.Errorf("failed to marshal args: %w", err)
			}
			argPtr := reflect.New(handler.argsType)
			if err := json.Unmarshal(argsBytes, argPtr.Interface()); err != nil {
				return zero, fmt.Errorf("failed to unmarshal args: %w", err)
			}
			argsVal = argPtr.Elem()
		}
		callArgs = append(callArgs, argsVal)
	}

	// Call handler
	results := handler.fn.Call(callArgs)

	// Parse results - handler can return (T, error) or just error
	numOut := handler.fn.Type().NumOut()

	if numOut == 1 {
		// Just error return
		if !results[0].IsNil() {
			return zero, results[0].Interface().(error)
		}
		return zero, nil
	}

	// (T, error) return
	if numOut == 2 {
		if !results[1].IsNil() {
			return zero, results[1].Interface().(error)
		}
		if results[0].CanInterface() {
			if result, ok := results[0].Interface().(T); ok {
				return result, nil
			}
			// Try JSON conversion
			resultBytes, _ := json.Marshal(results[0].Interface())
			var result T
			json.Unmarshal(resultBytes, &result)
			return result, nil
		}
	}

	return zero, nil
}
```

**Step 4: Update worker to initialize call state**

Update `worker.go` in `executeHandler`:
```go
func (w *Worker) executeHandler(ctx context.Context, job *Job, handler *jobHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Load checkpoints for replay
	checkpoints, err := w.queue.storage.GetCheckpoints(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("failed to load checkpoints: %w", err)
	}

	// Create job context with call state
	jobCtx := withJobContext(ctx, job, w.queue)
	jobCtx = withCallState(jobCtx, checkpoints)

	// Build arguments
	var args []reflect.Value

	if handler.hasContext {
		args = append(args, reflect.ValueOf(jobCtx))
	}

	if handler.argsType != nil {
		argVal := reflect.New(handler.argsType)
		if err := json.Unmarshal(job.Args, argVal.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal args: %w", err)
		}
		args = append(args, argVal.Elem())
	}

	// Call handler
	results := handler.fn.Call(args)

	if !results[0].IsNil() {
		return results[0].Interface().(error)
	}
	return nil
}
```

**Step 5: Run test to verify it passes**

Run: `go test ./... -v -run TestCall`
Expected: PASS

**Step 6: Commit**

```bash
git add call.go call_test.go worker.go
git commit -m "feat: implement jobs.Call with checkpointing and replay"
```

---

## Phase 7: Scheduling

### Task 7.1: Implement Schedule Types

**Files:**
- Create: `schedule.go`
- Create: `schedule_test.go`

**Step 1: Write test for schedules**

Create `schedule_test.go`:
```go
package jobs_test

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestEvery_CalculatesNextRun(t *testing.T) {
	schedule := jobs.Every(time.Hour)
	now := time.Date(2026, 2, 8, 10, 30, 0, 0, time.UTC)

	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 11, 30, 0, 0, time.UTC), next)
}

func TestDaily_CalculatesNextRun(t *testing.T) {
	schedule := jobs.Daily(9, 0)

	// Before 9am
	now := time.Date(2026, 2, 8, 8, 0, 0, 0, time.UTC)
	next := schedule.Next(now)
	assert.Equal(t, time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC), next)

	// After 9am
	now = time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	next = schedule.Next(now)
	assert.Equal(t, time.Date(2026, 2, 9, 9, 0, 0, 0, time.UTC), next)
}

func TestCron_ParsesExpression(t *testing.T) {
	schedule := jobs.Cron("0 9 * * *") // 9am daily

	now := time.Date(2026, 2, 8, 8, 0, 0, 0, time.UTC)
	next := schedule.Next(now)

	assert.Equal(t, time.Date(2026, 2, 8, 9, 0, 0, 0, time.UTC), next)
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./... -v -run TestEvery`
Expected: FAIL - undefined: jobs.Every

**Step 3: Implement schedules**

Create `schedule.go`:
```go
package jobs

import (
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule determines when a recurring job should run.
type Schedule interface {
	Next(from time.Time) time.Time
}

// everySchedule runs at fixed intervals.
type everySchedule struct {
	interval time.Duration
}

// Every creates a schedule that runs at fixed intervals.
func Every(d time.Duration) Schedule {
	return &everySchedule{interval: d}
}

func (s *everySchedule) Next(from time.Time) time.Time {
	return from.Add(s.interval)
}

// dailySchedule runs at a specific time each day.
type dailySchedule struct {
	hour   int
	minute int
	loc    *time.Location
}

// Daily creates a schedule that runs at a specific time each day.
func Daily(hour, minute int) Schedule {
	return &dailySchedule{hour: hour, minute: minute, loc: time.UTC}
}

func (s *dailySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)
	next := time.Date(from.Year(), from.Month(), from.Day(), s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// weeklySchedule runs at a specific day and time each week.
type weeklySchedule struct {
	day    time.Weekday
	hour   int
	minute int
	loc    *time.Location
}

// Weekly creates a schedule that runs at a specific day and time each week.
func Weekly(day time.Weekday, hour, minute int) Schedule {
	return &weeklySchedule{day: day, hour: hour, minute: minute, loc: time.UTC}
}

func (s *weeklySchedule) Next(from time.Time) time.Time {
	from = from.In(s.loc)

	daysUntil := int(s.day - from.Weekday())
	if daysUntil < 0 {
		daysUntil += 7
	}

	next := time.Date(from.Year(), from.Month(), from.Day()+daysUntil, s.hour, s.minute, 0, 0, s.loc)
	if !next.After(from) {
		next = next.AddDate(0, 0, 7)
	}
	return next
}

// cronSchedule wraps a cron expression.
type cronSchedule struct {
	schedule cron.Schedule
}

// Cron creates a schedule from a cron expression.
func Cron(expr string) Schedule {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(expr)
	if err != nil {
		panic("invalid cron expression: " + err.Error())
	}
	return &cronSchedule{schedule: schedule}
}

func (s *cronSchedule) Next(from time.Time) time.Time {
	return s.schedule.Next(from)
}

// Timezone sets the timezone for a schedule.
func Timezone(name string) Option {
	return optionFunc(func(o *Options) {
		loc, err := time.LoadLocation(name)
		if err != nil {
			panic("invalid timezone: " + err.Error())
		}
		o.Timezone = loc
	})
}
```

Add `Timezone` field to Options in `options.go`:
```go
type Options struct {
	Queue       string
	Priority    int
	MaxRetries  int
	Delay       time.Duration
	RunAt       *time.Time
	UniqueKey   string
	Determinism DeterminismMode
	Timezone    *time.Location
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./... -v -run "TestEvery|TestDaily|TestCron"`
Expected: PASS

**Step 5: Commit**

```bash
git add schedule.go schedule_test.go options.go
git commit -m "feat: add scheduling (Every, Daily, Weekly, Cron)"
```

---

### Task 7.2: Add Queue.Schedule Method

**Files:**
- Modify: `queue.go`
- Create: `scheduler.go`
- Modify: `queue_test.go`

**Step 1: Write test**

Add to `queue_test.go`:
```go
func TestQueue_Schedule(t *testing.T) {
	queue, db := setupTestQueue(t)
	ctx := context.Background()

	var runCount atomic.Int32
	queue.Register("scheduled-task", func(ctx context.Context, _ struct{}) error {
		runCount.Add(1)
		return nil
	})

	queue.Schedule("scheduled-task", jobs.Every(100*time.Millisecond))

	worker := queue.NewWorker(jobs.WithScheduler(true))
	workerCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	go worker.Start(workerCtx)

	time.Sleep(400 * time.Millisecond)

	// Should have run multiple times
	assert.GreaterOrEqual(t, runCount.Load(), int32(2))
}
```

**Step 2: Implement Schedule method and scheduler**

Add to `queue.go`:
```go
// ScheduledJob represents a recurring job.
type ScheduledJob struct {
	Name     string
	Schedule Schedule
	Args     interface{}
	Options  *Options
}

// Schedule registers a recurring job.
func (q *Queue) Schedule(name string, schedule Schedule, opts ...Option) {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	q.mu.Lock()
	if q.scheduledJobs == nil {
		q.scheduledJobs = make(map[string]*ScheduledJob)
	}
	q.scheduledJobs[name] = &ScheduledJob{
		Name:     name,
		Schedule: schedule,
		Options:  options,
	}
	q.mu.Unlock()
}
```

Add to Queue struct in `queue.go`:
```go
type Queue struct {
	storage       storage.Storage
	handlers      map[string]*jobHandler
	scheduledJobs map[string]*ScheduledJob
	mu            sync.RWMutex
	// ... rest of fields
}
```

Create `scheduler.go`:
```go
package jobs

import (
	"context"
	"time"
)

// WithScheduler enables the scheduler in the worker.
func WithScheduler(enabled bool) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		c.EnableScheduler = enabled
	})
}

// Add to WorkerConfig:
// EnableScheduler bool

func (w *Worker) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Track last run times
	lastRun := make(map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.queue.mu.RLock()
			scheduled := w.queue.scheduledJobs
			w.queue.mu.RUnlock()

			now := time.Now()
			for name, sj := range scheduled {
				nextRun := sj.Schedule.Next(lastRun[name])
				if now.After(nextRun) || now.Equal(nextRun) {
					// Time to run
					_, err := w.queue.Enqueue(ctx, sj.Name, sj.Args,
						QueueOpt(sj.Options.Queue),
						Priority(sj.Options.Priority),
					)
					if err != nil {
						w.logger.Error("failed to enqueue scheduled job", "name", name, "error", err)
					} else {
						lastRun[name] = now
					}
				}
			}
		}
	}
}
```

Update `worker.go` Start method to run scheduler:
```go
func (w *Worker) Start(ctx context.Context) error {
	// Start scheduler if enabled
	if w.config.EnableScheduler {
		go w.runScheduler(ctx)
	}

	// ... rest of Start method
}
```

Add to WorkerConfig in `worker.go`:
```go
type WorkerConfig struct {
	Queues          map[string]int
	PollInterval    time.Duration
	WorkerID        string
	EnableScheduler bool
}
```

**Step 3: Run test**

Run: `go test ./... -v -run TestQueue_Schedule`
Expected: PASS

**Step 4: Commit**

```bash
git add queue.go scheduler.go worker.go queue_test.go
git commit -m "feat: add Queue.Schedule and scheduler worker"
```

---

## Phase 8: Observability

### Task 8.1: Add Hooks

**Files:**
- Modify: `queue.go`
- Create: `hooks_test.go`

**Step 1: Write test**

Create `hooks_test.go`:
```go
package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestHooks_OnJobStart(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var started atomic.Bool
	queue.OnJobStart(func(ctx context.Context, j *jobs.Job) {
		started.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, started.Load())
}

func TestHooks_OnJobComplete(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var completed atomic.Bool
	queue.OnJobComplete(func(ctx context.Context, j *jobs.Job) {
		completed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, completed.Load())
}

func TestHooks_OnJobFail(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var failed atomic.Bool
	queue.OnJobFail(func(ctx context.Context, j *jobs.Job, err error) {
		failed.Store(true)
	})

	queue.Register("test", func(ctx context.Context, _ struct{}) error {
		return jobs.NoRetry(errors.New("fail"))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	queue.Enqueue(ctx, "test", struct{}{})

	worker := queue.NewWorker()
	go worker.Start(ctx)

	time.Sleep(300 * time.Millisecond)

	assert.True(t, failed.Load())
}
```

**Step 2: Implement hooks**

Add to `queue.go`:
```go
// OnJobStart registers a callback for when a job starts.
func (q *Queue) OnJobStart(fn func(context.Context, *Job)) {
	q.mu.Lock()
	q.onStart = append(q.onStart, fn)
	q.mu.Unlock()
}

// OnJobComplete registers a callback for when a job completes successfully.
func (q *Queue) OnJobComplete(fn func(context.Context, *Job)) {
	q.mu.Lock()
	q.onComplete = append(q.onComplete, fn)
	q.mu.Unlock()
}

// OnJobFail registers a callback for when a job fails permanently.
func (q *Queue) OnJobFail(fn func(context.Context, *Job, error)) {
	q.mu.Lock()
	q.onFail = append(q.onFail, fn)
	q.mu.Unlock()
}

// OnRetry registers a callback for when a job is retried.
func (q *Queue) OnRetry(fn func(context.Context, *Job, int, error)) {
	q.mu.Lock()
	q.onRetry = append(q.onRetry, fn)
	q.mu.Unlock()
}
```

**Step 3: Run test**

Run: `go test ./... -v -run TestHooks`
Expected: PASS

**Step 4: Commit**

```bash
git add queue.go hooks_test.go
git commit -m "feat: add observability hooks (OnJobStart, OnJobComplete, OnJobFail, OnRetry)"
```

---

### Task 8.2: Add Event Stream

**Files:**
- Create: `events.go`
- Create: `events_test.go`

**Step 1: Write test**

Create `events_test.go`:
```go
package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestEvents_Stream(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
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
```

**Step 2: Implement events**

Create `events.go`:
```go
package jobs

import "time"

// Event represents something that happened in the queue.
type Event interface {
	eventMarker()
}

// JobStarted is emitted when a job starts processing.
type JobStarted struct {
	Job       *Job
	Timestamp time.Time
}

func (*JobStarted) eventMarker() {}

// JobCompleted is emitted when a job completes successfully.
type JobCompleted struct {
	Job       *Job
	Duration  time.Duration
	Timestamp time.Time
}

func (*JobCompleted) eventMarker() {}

// JobFailed is emitted when a job fails permanently.
type JobFailed struct {
	Job       *Job
	Error     error
	Timestamp time.Time
}

func (*JobFailed) eventMarker() {}

// JobRetrying is emitted when a job is retried.
type JobRetrying struct {
	Job       *Job
	Attempt   int
	Error     error
	NextRunAt time.Time
	Timestamp time.Time
}

func (*JobRetrying) eventMarker() {}

// CheckpointSaved is emitted when a checkpoint is saved.
type CheckpointSaved struct {
	JobID     string
	CallIndex int
	CallType  string
	Timestamp time.Time
}

func (*CheckpointSaved) eventMarker() {}
```

Add to `queue.go`:
```go
// Events returns a channel for receiving queue events.
func (q *Queue) Events() <-chan Event {
	ch := make(chan Event, 100)
	q.mu.Lock()
	q.eventSubs = append(q.eventSubs, ch)
	q.mu.Unlock()
	return ch
}

func (q *Queue) emit(e Event) {
	q.mu.RLock()
	subs := q.eventSubs
	q.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
			// Drop if full
		}
	}
}
```

Update worker to emit events in `worker.go`:
```go
func (w *Worker) processJob(ctx context.Context, job *Job) {
	startTime := time.Now()

	// ... existing code for calling hooks ...

	// Emit start event
	w.queue.emit(&JobStarted{Job: job, Timestamp: startTime})

	// Execute handler
	err := w.executeHandler(ctx, job, handler)

	if err != nil {
		w.handleError(ctx, job, err, startTime)
	} else {
		w.queue.storage.Complete(ctx, job.ID, nil)
		duration := time.Since(startTime)

		for _, fn := range w.queue.onComplete {
			fn(ctx, job)
		}
		w.queue.emit(&JobCompleted{Job: job, Duration: duration, Timestamp: time.Now()})
	}
}

func (w *Worker) handleError(ctx context.Context, job *Job, err error, startTime time.Time) {
	// ... in the retry branch:
	w.queue.emit(&JobRetrying{
		Job:       job,
		Attempt:   job.Attempt,
		Error:     err,
		NextRunAt: retryAt,
		Timestamp: time.Now(),
	})

	// ... in the fail branch:
	w.queue.emit(&JobFailed{Job: job, Error: err, Timestamp: time.Now()})
}
```

**Step 3: Run test**

Run: `go test ./... -v -run TestEvents`
Expected: PASS

**Step 4: Commit**

```bash
git add events.go events_test.go queue.go worker.go
git commit -m "feat: add event stream for observability"
```

---

## Phase 9: Final Polish

### Task 9.1: Add Integration Test

**Files:**
- Create: `integration_test.go`

**Step 1: Write comprehensive integration test**

Create `integration_test.go`:
```go
package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/jdziat/simple-durable-jobs/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestIntegration_WorkflowWithNestedCalls(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	// Track execution
	var chargeCount, shipCount, notifyCount atomic.Int32

	queue.Register("charge", func(ctx context.Context, amount int) (string, error) {
		chargeCount.Add(1)
		return "receipt-123", nil
	})

	queue.Register("ship", func(ctx context.Context, items []string) error {
		shipCount.Add(1)
		return nil
	})

	queue.Register("notify", func(ctx context.Context, email string) error {
		notifyCount.Add(1)
		return nil
	})

	type Order struct {
		Amount int
		Items  []string
		Email  string
	}

	queue.Register("process-order", func(ctx context.Context, order Order) error {
		receipt, err := jobs.Call[string](ctx, "charge", order.Amount)
		if err != nil {
			return err
		}
		_ = receipt

		if err := jobs.Call[any](ctx, "ship", order.Items); err != nil {
			return err
		}

		return jobs.Call[any](ctx, "notify", order.Email)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	order := Order{
		Amount: 100,
		Items:  []string{"item1", "item2"},
		Email:  "test@example.com",
	}

	jobID, err := queue.Enqueue(ctx, "process-order", order)
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(1), chargeCount.Load())
	assert.Equal(t, int32(1), shipCount.Load())
	assert.Equal(t, int32(1), notifyCount.Load())

	// Verify job completed
	job, _ := store.GetJob(context.Background(), jobID)
	assert.Equal(t, jobs.StatusCompleted, job.Status)

	// Verify checkpoints saved
	checkpoints, _ := store.GetCheckpoints(context.Background(), jobID)
	assert.Len(t, checkpoints, 3)
}

func TestIntegration_RetryOnFailure(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var attempts atomic.Int32
	queue.Register("flaky", func(ctx context.Context, _ struct{}) error {
		count := attempts.Add(1)
		if count < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobID, _ := queue.Enqueue(ctx, "flaky", struct{}{}, jobs.Retries(5))

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for retries
	time.Sleep(5 * time.Second)

	assert.GreaterOrEqual(t, attempts.Load(), int32(3))

	job, _ := store.GetJob(context.Background(), jobID)
	assert.Equal(t, jobs.StatusCompleted, job.Status)
}

func TestIntegration_Priorities(t *testing.T) {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	store := storage.NewGormStorage(db)
	store.Migrate(context.Background())

	queue := jobs.New(store)

	var order []string
	var mu sync.Mutex

	queue.Register("task", func(ctx context.Context, name string) error {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
		return nil
	})

	ctx := context.Background()

	// Enqueue in reverse priority order
	queue.Enqueue(ctx, "task", "low", jobs.Priority(1))
	queue.Enqueue(ctx, "task", "medium", jobs.Priority(50))
	queue.Enqueue(ctx, "task", "high", jobs.Priority(100))

	workerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Single worker to ensure sequential processing
	worker := queue.NewWorker(jobs.Queue("default", jobs.Concurrency(1)))
	go worker.Start(workerCtx)

	time.Sleep(1 * time.Second)

	// High priority should run first
	assert.Equal(t, "high", order[0])
}
```

Add missing import:
```go
import "sync"
```

**Step 2: Run integration tests**

Run: `go test ./... -v -run TestIntegration`
Expected: PASS

**Step 3: Commit**

```bash
git add integration_test.go
git commit -m "test: add integration tests for workflows, retries, and priorities"
```

---

### Task 9.2: Run All Tests and Verify

**Step 1: Run full test suite**

Run: `go test ./... -v -race`
Expected: All tests pass

**Step 2: Check for any linting issues**

Run: `go vet ./...`
Expected: No issues

**Step 3: Format code**

Run: `go fmt ./...`

**Step 4: Final commit**

```bash
git add .
git commit -m "chore: format and finalize v0.1.0"
```

---

## Summary

The implementation is complete with:

1. **Core types**: Job, Checkpoint, Options, Errors
2. **Storage**: GORM-based storage supporting all GORM databases
3. **Queue**: Register, Enqueue with options
4. **Worker**: Job processing with retries and backoff
5. **Checkpointing**: jobs.Call() with replay on resume
6. **Scheduling**: Every, Daily, Weekly, Cron
7. **Observability**: Hooks and event stream

Total: ~20 tasks, ~1500 lines of code, ~500 lines of tests.
