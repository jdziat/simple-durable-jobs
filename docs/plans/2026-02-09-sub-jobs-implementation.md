# Sub-Jobs and Fan-Out Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add sub-job support with fan-out/fan-in patterns for parallel data processing and workflow orchestration.

**Architecture:** Sub-jobs are independent database records picked up by any worker. Parent jobs suspend to `waiting` status while sub-jobs execute. Event-driven resume with polling fallback. Configurable failure strategies (FailFast, CollectAll, Threshold).

**Tech Stack:** Go 1.21+, GORM, SQLite/PostgreSQL, generics for type-safe results.

---

## Task 1: Add New Job Statuses

**Files:**
- Modify: `pkg/core/job.go`
- Test: `pkg/core/job_test.go`

**Step 1: Add new status constants**

In `pkg/core/job.go`, add after existing statuses:

```go
const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusRetrying  JobStatus = "retrying"
	StatusWaiting   JobStatus = "waiting"    // NEW: suspended waiting for sub-jobs
	StatusCancelled JobStatus = "cancelled"  // NEW: terminated before completion
)
```

**Step 2: Run existing tests to verify no breakage**

```bash
go test ./pkg/core/... -v
```

Expected: All existing tests pass.

**Step 3: Commit**

```bash
git add pkg/core/job.go
git commit -m "feat(core): add waiting and cancelled job statuses"
```

---

## Task 2: Add Parent-Child Fields to Job Struct

**Files:**
- Modify: `pkg/core/job.go`

**Step 1: Add new fields to Job struct**

In `pkg/core/job.go`, add to Job struct after `UniqueKey`:

```go
type Job struct {
	// ... existing fields ...
	UniqueKey       string     `gorm:"index;size:255"` // For job deduplication

	// Parent-child relationship
	ParentJobID *string `gorm:"index;size:36"`
	RootJobID   *string `gorm:"index;size:36"` // Top-level workflow job

	// Fan-out tracking
	FanOutID    *string `gorm:"index;size:36"` // Groups sibling sub-jobs
	FanOutIndex int     `gorm:"default:0"`     // Position in fan-out batch

	// Result storage for parent retrieval
	Result []byte `gorm:"type:bytes"` // Serialized return value
}
```

**Step 2: Run tests**

```bash
go test ./pkg/core/... -v
```

Expected: Pass (GORM handles new fields gracefully).

**Step 3: Commit**

```bash
git add pkg/core/job.go
git commit -m "feat(core): add parent-child and fan-out fields to Job"
```

---

## Task 3: Create FanOut Struct

**Files:**
- Create: `pkg/core/fanout.go`
- Modify: `pkg/storage/gorm.go` (add to Migrate)

**Step 1: Create fanout.go**

Create `pkg/core/fanout.go`:

```go
package core

import (
	"time"
)

// FanOutStrategy defines how sub-job failures affect the parent.
type FanOutStrategy string

const (
	StrategyFailFast   FanOutStrategy = "fail_fast"   // Fail parent on first sub-job failure
	StrategyCollectAll FanOutStrategy = "collect_all" // Wait for all, return partial results
	StrategyThreshold  FanOutStrategy = "threshold"   // Ok if threshold% succeed
)

// FanOutStatus represents the state of a fan-out batch.
type FanOutStatus string

const (
	FanOutPending   FanOutStatus = "pending"
	FanOutCompleted FanOutStatus = "completed"
	FanOutFailed    FanOutStatus = "failed"
)

// FanOut tracks a batch of sub-jobs spawned by a parent job.
type FanOut struct {
	ID             string         `gorm:"primaryKey;size:36"`
	ParentJobID    string         `gorm:"index;size:36;not null"`
	TotalCount     int            `gorm:"not null"`
	CompletedCount int            `gorm:"default:0"`
	FailedCount    int            `gorm:"default:0"`
	Strategy       FanOutStrategy `gorm:"size:20;default:'fail_fast'"`
	Threshold      float64        `gorm:"default:1.0"`
	Status         FanOutStatus   `gorm:"size:20;default:'pending'"`
	TimeoutAt      *time.Time     `gorm:"index"`
	CancelOnFail   bool           `gorm:"default:false"`
	CreatedAt      time.Time      `gorm:"autoCreateTime"`
	UpdatedAt      time.Time      `gorm:"autoUpdateTime"`
}
```

**Step 2: Add FanOut to Migrate**

In `pkg/storage/gorm.go`, update Migrate:

```go
func (s *GormStorage) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{})
}
```

**Step 3: Run tests**

```bash
go test ./pkg/storage/... -v
```

Expected: Pass.

**Step 4: Commit**

```bash
git add pkg/core/fanout.go pkg/storage/gorm.go
git commit -m "feat(core): add FanOut tracking struct"
```

---

## Task 4: Add FanOut Storage Interface Methods

**Files:**
- Modify: `pkg/core/storage.go`

**Step 1: Add FanOut methods to Storage interface**

In `pkg/core/storage.go`, add new methods:

```go
type Storage interface {
	// ... existing methods ...

	// Fan-out operations
	CreateFanOut(ctx context.Context, fanOut *FanOut) error
	GetFanOut(ctx context.Context, fanOutID string) (*FanOut, error)
	IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*FanOut, error)
	IncrementFanOutFailed(ctx context.Context, fanOutID string) (*FanOut, error)
	UpdateFanOutStatus(ctx context.Context, fanOutID string, status FanOutStatus) error
	GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*FanOut, error)

	// Sub-job operations
	EnqueueBatch(ctx context.Context, jobs []*Job) error
	GetSubJobs(ctx context.Context, fanOutID string) ([]*Job, error)
	GetSubJobResults(ctx context.Context, fanOutID string) ([]*Job, error)
	CancelSubJobs(ctx context.Context, fanOutID string) (int64, error)

	// Waiting job operations
	SuspendJob(ctx context.Context, jobID string, workerID string) error
	ResumeJob(ctx context.Context, jobID string) error
	GetWaitingJobsToResume(ctx context.Context) ([]*Job, error)
}
```

**Step 2: Verify it compiles (storage implementations will fail - that's expected)**

```bash
go build ./pkg/core/...
```

Expected: Pass (interface only).

**Step 3: Commit**

```bash
git add pkg/core/storage.go
git commit -m "feat(core): add fan-out and sub-job methods to Storage interface"
```

---

## Task 5: Implement FanOut Storage Methods

**Files:**
- Modify: `pkg/storage/gorm.go`

**Step 1: Add CreateFanOut**

```go
func (s *GormStorage) CreateFanOut(ctx context.Context, fanOut *core.FanOut) error {
	if fanOut.ID == "" {
		fanOut.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(fanOut).Error
}
```

**Step 2: Add GetFanOut**

```go
func (s *GormStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).First(&fanOut, "id = ?", fanOutID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &fanOut, err
}
```

**Step 3: Add IncrementFanOutCompleted (atomic)**

```go
func (s *GormStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"UPDATE fan_outs SET completed_count = completed_count + 1, updated_at = ? WHERE id = ?",
			time.Now(), fanOutID,
		).Error; err != nil {
			return err
		}
		return tx.First(&fanOut, "id = ?", fanOutID).Error
	})
	return &fanOut, err
}
```

**Step 4: Add IncrementFanOutFailed (atomic)**

```go
func (s *GormStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"UPDATE fan_outs SET failed_count = failed_count + 1, updated_at = ? WHERE id = ?",
			time.Now(), fanOutID,
		).Error; err != nil {
			return err
		}
		return tx.First(&fanOut, "id = ?", fanOutID).Error
	})
	return &fanOut, err
}
```

**Step 5: Add UpdateFanOutStatus**

```go
func (s *GormStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) error {
	return s.db.WithContext(ctx).
		Model(&core.FanOut{}).
		Where("id = ?", fanOutID).
		Update("status", status).Error
}
```

**Step 6: Add GetFanOutsByParent**

```go
func (s *GormStorage) GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*core.FanOut, error) {
	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id = ?", parentJobID).
		Order("created_at ASC").
		Find(&fanOuts).Error
	return fanOuts, err
}
```

**Step 7: Commit**

```bash
git add pkg/storage/gorm.go
git commit -m "feat(storage): implement fan-out CRUD methods"
```

---

## Task 6: Implement Sub-Job Storage Methods

**Files:**
- Modify: `pkg/storage/gorm.go`

**Step 1: Add EnqueueBatch**

```go
func (s *GormStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		if job.ID == "" {
			job.ID = uuid.New().String()
		}
		if job.Status == "" {
			job.Status = core.StatusPending
		}
		if job.Queue == "" {
			job.Queue = "default"
		}
	}
	return s.db.WithContext(ctx).Create(jobs).Error
}
```

**Step 2: Add GetSubJobs**

```go
func (s *GormStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ?", fanOutID).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	return jobs, err
}
```

**Step 3: Add GetSubJobResults (only completed/failed)**

```go
func (s *GormStorage) GetSubJobResults(ctx context.Context, fanOutID string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusCompleted, core.StatusFailed}).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	return jobs, err
}
```

**Step 4: Add CancelSubJobs**

```go
func (s *GormStorage) CancelSubJobs(ctx context.Context, fanOutID string) (int64, error) {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusPending, core.StatusRunning}).
		Updates(map[string]any{
			"status":     core.StatusCancelled,
			"updated_at": time.Now(),
		})
	return result.RowsAffected, result.Error
}
```

**Step 5: Commit**

```bash
git add pkg/storage/gorm.go
git commit -m "feat(storage): implement sub-job batch and query methods"
```

---

## Task 7: Implement Waiting Job Storage Methods

**Files:**
- Modify: `pkg/storage/gorm.go`

**Step 1: Add SuspendJob**

```go
func (s *GormStorage) SuspendJob(ctx context.Context, jobID string, workerID string) error {
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
		Updates(map[string]any{
			"status":     core.StatusWaiting,
			"locked_by":  "",
			"updated_at": time.Now(),
		}).Error
}
```

**Step 2: Add ResumeJob**

```go
func (s *GormStorage) ResumeJob(ctx context.Context, jobID string) error {
	return s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusWaiting).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"updated_at": time.Now(),
		}).Error
}
```

**Step 3: Add GetWaitingJobsToResume (polling fallback)**

```go
func (s *GormStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	var jobs []*core.Job
	// Find waiting jobs where their fan-out is complete
	err := s.db.WithContext(ctx).Raw(`
		SELECT j.* FROM jobs j
		INNER JOIN fan_outs f ON j.id = f.parent_job_id
		WHERE j.status = ?
		AND f.status IN (?, ?)
	`, core.StatusWaiting, core.FanOutCompleted, core.FanOutFailed).Scan(&jobs).Error
	return jobs, err
}
```

**Step 4: Run build to verify**

```bash
go build ./pkg/storage/...
```

Expected: Pass.

**Step 5: Commit**

```bash
git add pkg/storage/gorm.go
git commit -m "feat(storage): implement job suspend/resume methods"
```

---

## Task 8: Create FanOut Types Package

**Files:**
- Create: `pkg/fanout/types.go`
- Create: `pkg/fanout/options.go`
- Create: `pkg/fanout/doc.go`

**Step 1: Create doc.go**

Create `pkg/fanout/doc.go`:

```go
// Package fanout provides fan-out/fan-in patterns for parallel job execution.
package fanout
```

**Step 2: Create types.go**

Create `pkg/fanout/types.go`:

```go
package fanout

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// SubJob represents a sub-job to be spawned.
type SubJob struct {
	Type     string
	Args     any
	Queue    string
	Priority int
	Retries  int
	Timeout  time.Duration
}

// Result wraps a sub-job result with its index and potential error.
type Result[T any] struct {
	Index int   // Position in original subJobs slice
	Value T     // Result if successful
	Err   error // Error if failed
}

// Error contains details about fan-out failures.
type Error struct {
	FanOutID    string
	TotalCount  int
	FailedCount int
	Strategy    core.FanOutStrategy
	Failures    []SubJobFailure
}

func (e *Error) Error() string {
	return fmt.Sprintf("fan-out failed: %d/%d sub-jobs failed", e.FailedCount, e.TotalCount)
}

// SubJobFailure contains details about a single sub-job failure.
type SubJobFailure struct {
	Index   int
	JobID   string
	Error   string
	Attempt int
}
```

**Step 3: Create options.go**

Create `pkg/fanout/options.go`:

```go
package fanout

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// Option configures fan-out behavior.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type config struct {
	strategy        core.FanOutStrategy
	threshold       float64
	queue           string
	priority        int
	retries         int
	subJobTimeout   time.Duration
	totalTimeout    time.Duration
	cancelOnFailure bool
}

func defaultConfig() *config {
	return &config{
		strategy:  core.StrategyFailFast,
		threshold: 1.0,
		retries:   3,
	}
}

// FailFast fails the parent on first sub-job failure.
func FailFast() Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyFailFast
	})
}

// CollectAll waits for all sub-jobs, returns partial results.
func CollectAll() Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyCollectAll
	})
}

// Threshold succeeds if at least pct% of sub-jobs succeed.
func Threshold(pct float64) Option {
	return optionFunc(func(c *config) {
		c.strategy = core.StrategyThreshold
		c.threshold = pct
	})
}

// WithQueue sets the queue for sub-jobs.
func WithQueue(q string) Option {
	return optionFunc(func(c *config) {
		c.queue = q
	})
}

// WithPriority sets the priority for sub-jobs.
func WithPriority(p int) Option {
	return optionFunc(func(c *config) {
		c.priority = p
	})
}

// WithRetries sets the retry count for sub-jobs.
func WithRetries(n int) Option {
	return optionFunc(func(c *config) {
		c.retries = n
	})
}

// WithSubJobTimeout sets timeout for each sub-job.
func WithSubJobTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.subJobTimeout = d
	})
}

// WithTimeout sets timeout for entire fan-out.
func WithTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.totalTimeout = d
	})
}

// CancelOnParentFailure cancels sub-jobs if parent fails.
func CancelOnParentFailure() Option {
	return optionFunc(func(c *config) {
		c.cancelOnFailure = true
	})
}
```

**Step 4: Fix missing import in types.go**

Add `"fmt"` to imports in `pkg/fanout/types.go`.

**Step 5: Run build**

```bash
go build ./pkg/fanout/...
```

Expected: Pass.

**Step 6: Commit**

```bash
git add pkg/fanout/
git commit -m "feat(fanout): add types and options for fan-out API"
```

---

## Task 9: Create Sub Function

**Files:**
- Create: `pkg/fanout/sub.go`

**Step 1: Create sub.go**

Create `pkg/fanout/sub.go`:

```go
package fanout

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// Sub creates a sub-job definition.
func Sub(jobType string, args any, opts ...queue.Option) SubJob {
	sj := SubJob{
		Type:    jobType,
		Args:    args,
		Retries: 3, // default
	}

	// Apply queue options to extract values
	queueOpts := &queue.Options{}
	for _, opt := range opts {
		opt.Apply(queueOpts)
	}

	if queueOpts.Queue != "" {
		sj.Queue = queueOpts.Queue
	}
	if queueOpts.Priority != 0 {
		sj.Priority = queueOpts.Priority
	}
	if queueOpts.MaxRetries > 0 {
		sj.Retries = queueOpts.MaxRetries
	}

	return sj
}
```

**Step 2: Run build**

```bash
go build ./pkg/fanout/...
```

Expected: Pass.

**Step 3: Commit**

```bash
git add pkg/fanout/sub.go
git commit -m "feat(fanout): add Sub() function for creating sub-jobs"
```

---

## Task 10: Create FanOut Checkpoint Type

**Files:**
- Modify: `pkg/core/job.go`

**Step 1: Add FanOutCheckpoint struct**

In `pkg/core/job.go`, add:

```go
// FanOutCheckpoint stores fan-out state for job replay.
type FanOutCheckpoint struct {
	FanOutID string `json:"fan_out_id"`
	CallIndex int   `json:"call_index"`
}
```

**Step 2: Commit**

```bash
git add pkg/core/job.go
git commit -m "feat(core): add FanOutCheckpoint for replay"
```

---

## Task 11: Implement FanOut Function (Core Logic)

**Files:**
- Create: `pkg/fanout/fanout.go`

**Step 1: Create fanout.go with FanOut function**

Create `pkg/fanout/fanout.go`:

```go
package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
)

// FanOut spawns sub-jobs in parallel and waits for all results.
// Checkpoints progress - safe to retry if parent crashes.
func FanOut[T any](ctx context.Context, subJobs []SubJob, opts ...Option) ([]Result[T], error) {
	if len(subJobs) == 0 {
		return nil, nil
	}

	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil, fmt.Errorf("fanout.FanOut must be used within a job handler")
	}

	cs := intctx.GetCallState(ctx)
	if cs == nil {
		return nil, fmt.Errorf("fanout.FanOut: call state not initialized")
	}

	// Build config
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Get call index for checkpointing
	cs.Mu.Lock()
	callIndex := cs.CallIndex
	cs.CallIndex++
	checkpoint, hasCheckpoint := cs.Checkpoints[callIndex]
	cs.Mu.Unlock()

	var fanOutID string

	if hasCheckpoint {
		// Resume: extract fan-out ID from checkpoint
		var fanOutCP core.FanOutCheckpoint
		if err := json.Unmarshal(checkpoint.Result, &fanOutCP); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fan-out checkpoint: %w", err)
		}
		fanOutID = fanOutCP.FanOutID
	} else {
		// First execution: create fan-out and sub-jobs
		fanOutID = uuid.New().String()

		// Create FanOut record
		fanOut := &core.FanOut{
			ID:           fanOutID,
			ParentJobID:  jc.Job.ID,
			TotalCount:   len(subJobs),
			Strategy:     cfg.strategy,
			Threshold:    cfg.threshold,
			CancelOnFail: cfg.cancelOnFailure,
		}
		if cfg.totalTimeout > 0 {
			timeout := time.Now().Add(cfg.totalTimeout)
			fanOut.TimeoutAt = &timeout
		}

		if err := jc.Storage.CreateFanOut(ctx, fanOut); err != nil {
			return nil, fmt.Errorf("failed to create fan-out: %w", err)
		}

		// Create sub-jobs
		jobs := make([]*core.Job, len(subJobs))
		for i, sj := range subJobs {
			args, err := json.Marshal(sj.Args)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal sub-job args: %w", err)
			}

			queue := sj.Queue
			if queue == "" {
				queue = cfg.queue
			}
			if queue == "" {
				queue = jc.Job.Queue
			}

			priority := sj.Priority
			if priority == 0 {
				priority = cfg.priority
			}

			retries := sj.Retries
			if retries == 0 {
				retries = cfg.retries
			}

			parentID := jc.Job.ID
			rootID := jc.Job.RootJobID
			if rootID == nil {
				rootID = &jc.Job.ID
			}

			jobs[i] = &core.Job{
				ID:          uuid.New().String(),
				Type:        sj.Type,
				Args:        args,
				Queue:       queue,
				Priority:    priority,
				MaxRetries:  retries,
				ParentJobID: &parentID,
				RootJobID:   rootID,
				FanOutID:    &fanOutID,
				FanOutIndex: i,
			}
		}

		if err := jc.Storage.EnqueueBatch(ctx, jobs); err != nil {
			return nil, fmt.Errorf("failed to enqueue sub-jobs: %w", err)
		}

		// Save checkpoint
		cpData, _ := json.Marshal(core.FanOutCheckpoint{
			FanOutID:  fanOutID,
			CallIndex: callIndex,
		})
		cp := &core.Checkpoint{
			ID:        uuid.New().String(),
			JobID:     jc.Job.ID,
			CallIndex: callIndex,
			CallType:  "fanout",
			Result:    cpData,
		}
		if err := jc.SaveCheckpoint(ctx, cp); err != nil {
			return nil, fmt.Errorf("failed to save fan-out checkpoint: %w", err)
		}
	}

	// Suspend parent job and wait for resume
	if err := jc.Storage.SuspendJob(ctx, jc.Job.ID, jc.WorkerID); err != nil {
		return nil, fmt.Errorf("failed to suspend job: %w", err)
	}

	// Signal to worker that this job should not continue
	return nil, &SuspendError{FanOutID: fanOutID}
}

// SuspendError signals the worker to stop processing and wait for resume.
type SuspendError struct {
	FanOutID string
}

func (e *SuspendError) Error() string {
	return fmt.Sprintf("job suspended waiting for fan-out %s", e.FanOutID)
}

// IsSuspendError checks if an error is a suspend signal.
func IsSuspendError(err error) bool {
	_, ok := err.(*SuspendError)
	return ok
}
```

**Step 2: Run build**

```bash
go build ./pkg/fanout/...
```

Expected: May fail due to missing JobContext methods. We'll fix in next task.

**Step 3: Commit (even if build fails)**

```bash
git add pkg/fanout/fanout.go
git commit -m "feat(fanout): implement core FanOut function"
```

---

## Task 12: Update JobContext Interface

**Files:**
- Modify: `pkg/internal/context/context.go`

**Step 1: Add Storage and WorkerID to JobContext**

Check current JobContext and add needed fields. The JobContext needs access to Storage for fan-out operations.

In `pkg/internal/context/context.go`, ensure JobContext has:

```go
type JobContext struct {
	Job           *core.Job
	Storage       core.Storage
	WorkerID      string
	HandlerLookup func(name string) (any, bool)
	SaveCheckpoint func(ctx context.Context, cp *core.Checkpoint) error
}
```

**Step 2: Run build**

```bash
go build ./...
```

**Step 3: Commit**

```bash
git add pkg/internal/context/context.go
git commit -m "feat(context): add Storage and WorkerID to JobContext"
```

---

## Task 13: Handle SuspendError in Worker

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Import fanout package**

Add to imports:

```go
import (
	// ... existing ...
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
)
```

**Step 2: Handle SuspendError in processJob**

Find the error handling in processJob and add:

```go
// After job execution, check for suspend
if err != nil {
	if fanout.IsSuspendError(err) {
		// Job suspended waiting for sub-jobs - don't mark as failed
		w.logger.Info("job suspended waiting for fan-out", "job_id", job.ID)
		return nil
	}
	// ... existing error handling ...
}
```

**Step 3: Run build**

```bash
go build ./pkg/worker/...
```

**Step 4: Commit**

```bash
git add pkg/worker/worker.go
git commit -m "feat(worker): handle SuspendError for fan-out jobs"
```

---

## Task 14: Handle Sub-Job Completion (Resume Parent)

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Add sub-job completion handler**

After a job completes, check if it's a sub-job and update fan-out:

```go
func (w *Worker) handleSubJobCompletion(ctx context.Context, job *core.Job, succeeded bool) error {
	if job.FanOutID == nil {
		return nil // Not a sub-job
	}

	var fanOut *core.FanOut
	var err error

	if succeeded {
		fanOut, err = w.queue.Storage().IncrementFanOutCompleted(ctx, *job.FanOutID)
	} else {
		fanOut, err = w.queue.Storage().IncrementFanOutFailed(ctx, *job.FanOutID)
	}
	if err != nil {
		return fmt.Errorf("failed to update fan-out: %w", err)
	}

	// Check if fan-out is complete
	if err := w.checkFanOutCompletion(ctx, fanOut); err != nil {
		return err
	}

	return nil
}

func (w *Worker) checkFanOutCompletion(ctx context.Context, fanOut *core.FanOut) error {
	total := fanOut.CompletedCount + fanOut.FailedCount
	if total < fanOut.TotalCount {
		// Not all sub-jobs done yet

		// Check fail-fast
		if fanOut.Strategy == core.StrategyFailFast && fanOut.FailedCount > 0 {
			return w.completeFanOut(ctx, fanOut, core.FanOutFailed)
		}

		// Check threshold
		if fanOut.Strategy == core.StrategyThreshold {
			maxFailures := int(float64(fanOut.TotalCount) * (1 - fanOut.Threshold))
			if fanOut.FailedCount > maxFailures {
				return w.completeFanOut(ctx, fanOut, core.FanOutFailed)
			}
		}

		return nil
	}

	// All sub-jobs done
	status := core.FanOutCompleted
	if fanOut.Strategy == core.StrategyFailFast && fanOut.FailedCount > 0 {
		status = core.FanOutFailed
	} else if fanOut.Strategy == core.StrategyThreshold {
		successRate := float64(fanOut.CompletedCount) / float64(fanOut.TotalCount)
		if successRate < fanOut.Threshold {
			status = core.FanOutFailed
		}
	} else if fanOut.Strategy == core.StrategyCollectAll && fanOut.FailedCount > 0 {
		// CollectAll still marks as completed, error returned to parent
		status = core.FanOutCompleted
	}

	return w.completeFanOut(ctx, fanOut, status)
}

func (w *Worker) completeFanOut(ctx context.Context, fanOut *core.FanOut, status core.FanOutStatus) error {
	if err := w.queue.Storage().UpdateFanOutStatus(ctx, fanOut.ID, status); err != nil {
		return err
	}

	// Cancel remaining sub-jobs if needed
	if status == core.FanOutFailed && fanOut.CancelOnFail {
		w.queue.Storage().CancelSubJobs(ctx, fanOut.ID)
	}

	// Resume parent job
	return w.queue.Storage().ResumeJob(ctx, fanOut.ParentJobID)
}
```

**Step 2: Call handleSubJobCompletion after job completes**

In the complete/fail paths, add:

```go
// After marking job as completed:
if err := w.handleSubJobCompletion(ctx, job, true); err != nil {
	w.logger.Error("failed to handle sub-job completion", "error", err)
}

// After marking job as failed (retries exhausted):
if err := w.handleSubJobCompletion(ctx, job, false); err != nil {
	w.logger.Error("failed to handle sub-job failure", "error", err)
}
```

**Step 3: Run build**

```bash
go build ./pkg/worker/...
```

**Step 4: Commit**

```bash
git add pkg/worker/worker.go
git commit -m "feat(worker): handle sub-job completion and resume parent"
```

---

## Task 15: Add Polling Fallback for Waiting Jobs

**Files:**
- Modify: `pkg/worker/worker.go`

**Step 1: Add polling loop for waiting jobs**

In the Start method, add a goroutine for polling:

```go
// In Start(), after scheduler:
go w.pollWaitingJobs(ctx)
```

**Step 2: Implement pollWaitingJobs**

```go
func (w *Worker) pollWaitingJobs(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // Poll every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobs, err := w.queue.Storage().GetWaitingJobsToResume(ctx)
			if err != nil {
				w.logger.Error("failed to get waiting jobs", "error", err)
				continue
			}
			for _, job := range jobs {
				if err := w.queue.Storage().ResumeJob(ctx, job.ID); err != nil {
					w.logger.Error("failed to resume job", "job_id", job.ID, "error", err)
				} else {
					w.logger.Info("resumed waiting job via polling", "job_id", job.ID)
				}
			}
		}
	}
}
```

**Step 3: Commit**

```bash
git add pkg/worker/worker.go
git commit -m "feat(worker): add polling fallback for waiting jobs"
```

---

## Task 16: Implement Result Collection on Resume

**Files:**
- Modify: `pkg/fanout/fanout.go`

**Step 1: Add CollectResults function**

```go
// CollectResults gathers results from completed sub-jobs.
// Called when parent job resumes after fan-out.
func CollectResults[T any](ctx context.Context, fanOutID string) ([]Result[T], error) {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil, fmt.Errorf("CollectResults must be used within a job handler")
	}

	fanOut, err := jc.Storage.GetFanOut(ctx, fanOutID)
	if err != nil {
		return nil, fmt.Errorf("failed to get fan-out: %w", err)
	}
	if fanOut == nil {
		return nil, fmt.Errorf("fan-out not found: %s", fanOutID)
	}

	jobs, err := jc.Storage.GetSubJobResults(ctx, fanOutID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sub-job results: %w", err)
	}

	results := make([]Result[T], len(jobs))
	for i, job := range jobs {
		results[job.FanOutIndex] = Result[T]{
			Index: job.FanOutIndex,
		}
		if job.Status == core.StatusFailed {
			results[job.FanOutIndex].Err = fmt.Errorf("%s", job.LastError)
		} else if job.Result != nil {
			var value T
			if err := json.Unmarshal(job.Result, &value); err != nil {
				results[job.FanOutIndex].Err = fmt.Errorf("failed to unmarshal result: %w", err)
			} else {
				results[job.FanOutIndex].Value = value
			}
		}
	}

	// Check if fan-out failed
	if fanOut.Status == core.FanOutFailed {
		failures := make([]SubJobFailure, 0)
		for _, job := range jobs {
			if job.Status == core.StatusFailed {
				failures = append(failures, SubJobFailure{
					Index:   job.FanOutIndex,
					JobID:   job.ID,
					Error:   job.LastError,
					Attempt: job.Attempt,
				})
			}
		}
		return results, &Error{
			FanOutID:    fanOutID,
			TotalCount:  fanOut.TotalCount,
			FailedCount: fanOut.FailedCount,
			Strategy:    fanOut.Strategy,
			Failures:    failures,
		}
	}

	return results, nil
}
```

**Step 2: Commit**

```bash
git add pkg/fanout/fanout.go
git commit -m "feat(fanout): implement CollectResults for resume"
```

---

## Task 17: Add Helper Functions

**Files:**
- Create: `pkg/fanout/helpers.go`

**Step 1: Create helpers.go**

```go
package fanout

// Values extracts values from successful results.
func Values[T any](results []Result[T]) []T {
	values := make([]T, 0, len(results))
	for _, r := range results {
		if r.Err == nil {
			values = append(values, r.Value)
		}
	}
	return values
}

// Partition splits results into successes and failures.
func Partition[T any](results []Result[T]) ([]T, []error) {
	successes := make([]T, 0)
	failures := make([]error, 0)
	for _, r := range results {
		if r.Err == nil {
			successes = append(successes, r.Value)
		} else {
			failures = append(failures, r.Err)
		}
	}
	return successes, failures
}

// AllSucceeded checks if all results succeeded.
func AllSucceeded[T any](results []Result[T]) bool {
	for _, r := range results {
		if r.Err != nil {
			return false
		}
	}
	return true
}

// SuccessCount returns the number of successful results.
func SuccessCount[T any](results []Result[T]) int {
	count := 0
	for _, r := range results {
		if r.Err == nil {
			count++
		}
	}
	return count
}
```

**Step 2: Commit**

```bash
git add pkg/fanout/helpers.go
git commit -m "feat(fanout): add result helper functions"
```

---

## Task 18: Export FanOut API from Root Package

**Files:**
- Modify: `jobs.go` (root facade)

**Step 1: Add fan-out exports**

```go
import (
	// ... existing ...
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
)

// Fan-out types
type (
	SubJob        = fanout.SubJob
	FanOutResult  = fanout.Result
	FanOutError   = fanout.Error
	FanOutOption  = fanout.Option
)

// Fan-out functions
var (
	Sub            = fanout.Sub
	FanOut         = fanout.FanOut
	CollectResults = fanout.CollectResults
	Values         = fanout.Values
	Partition      = fanout.Partition
	AllSucceeded   = fanout.AllSucceeded
)

// Fan-out options
var (
	FailFast            = fanout.FailFast
	CollectAll          = fanout.CollectAll
	Threshold           = fanout.Threshold
	WithFanOutQueue     = fanout.WithQueue
	WithFanOutPriority  = fanout.WithPriority
	WithFanOutRetries   = fanout.WithRetries
	WithSubJobTimeout   = fanout.WithSubJobTimeout
	WithFanOutTimeout   = fanout.WithTimeout
	CancelOnParentFailure = fanout.CancelOnParentFailure
)
```

**Step 2: Run build**

```bash
go build ./...
```

**Step 3: Commit**

```bash
git add jobs.go
git commit -m "feat: export fan-out API from root package"
```

---

## Task 19: Write Unit Tests for FanOut Options

**Files:**
- Create: `pkg/fanout/options_test.go`

**Step 1: Create options_test.go**

```go
package fanout

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/stretchr/testify/assert"
)

func TestFailFast(t *testing.T) {
	cfg := defaultConfig()
	FailFast().apply(cfg)
	assert.Equal(t, core.StrategyFailFast, cfg.strategy)
}

func TestCollectAll(t *testing.T) {
	cfg := defaultConfig()
	CollectAll().apply(cfg)
	assert.Equal(t, core.StrategyCollectAll, cfg.strategy)
}

func TestThreshold(t *testing.T) {
	cfg := defaultConfig()
	Threshold(0.8).apply(cfg)
	assert.Equal(t, core.StrategyThreshold, cfg.strategy)
	assert.Equal(t, 0.8, cfg.threshold)
}

func TestWithQueue(t *testing.T) {
	cfg := defaultConfig()
	WithQueue("high-priority").apply(cfg)
	assert.Equal(t, "high-priority", cfg.queue)
}

func TestWithTimeout(t *testing.T) {
	cfg := defaultConfig()
	WithTimeout(1 * time.Hour).apply(cfg)
	assert.Equal(t, 1*time.Hour, cfg.totalTimeout)
}

func TestCancelOnParentFailure(t *testing.T) {
	cfg := defaultConfig()
	CancelOnParentFailure().apply(cfg)
	assert.True(t, cfg.cancelOnFailure)
}
```

**Step 2: Run tests**

```bash
go test ./pkg/fanout/... -v
```

Expected: Pass.

**Step 3: Commit**

```bash
git add pkg/fanout/options_test.go
git commit -m "test(fanout): add unit tests for options"
```

---

## Task 20: Write Unit Tests for Helpers

**Files:**
- Create: `pkg/fanout/helpers_test.go`

**Step 1: Create helpers_test.go**

```go
package fanout

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues(t *testing.T) {
	results := []Result[int]{
		{Index: 0, Value: 10},
		{Index: 1, Err: errors.New("failed")},
		{Index: 2, Value: 30},
	}
	values := Values(results)
	assert.Equal(t, []int{10, 30}, values)
}

func TestPartition(t *testing.T) {
	results := []Result[string]{
		{Index: 0, Value: "a"},
		{Index: 1, Err: errors.New("err1")},
		{Index: 2, Value: "c"},
		{Index: 3, Err: errors.New("err2")},
	}
	successes, failures := Partition(results)
	assert.Equal(t, []string{"a", "c"}, successes)
	assert.Len(t, failures, 2)
}

func TestAllSucceeded(t *testing.T) {
	t.Run("all success", func(t *testing.T) {
		results := []Result[int]{{Value: 1}, {Value: 2}}
		assert.True(t, AllSucceeded(results))
	})

	t.Run("has failure", func(t *testing.T) {
		results := []Result[int]{{Value: 1}, {Err: errors.New("fail")}}
		assert.False(t, AllSucceeded(results))
	})

	t.Run("empty", func(t *testing.T) {
		results := []Result[int]{}
		assert.True(t, AllSucceeded(results))
	})
}

func TestSuccessCount(t *testing.T) {
	results := []Result[int]{
		{Value: 1},
		{Err: errors.New("fail")},
		{Value: 3},
		{Value: 4},
	}
	assert.Equal(t, 3, SuccessCount(results))
}
```

**Step 2: Run tests**

```bash
go test ./pkg/fanout/... -v
```

Expected: Pass.

**Step 3: Commit**

```bash
git add pkg/fanout/helpers_test.go
git commit -m "test(fanout): add unit tests for helpers"
```

---

## Task 21: Write Integration Test for FanOut

**Files:**
- Create: `tests/fanout_test.go`

**Step 1: Create fanout_test.go**

```go
package jobs_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var fanoutTestCounter int

func setupFanOutTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	fanoutTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_fanout_test_%d_%d.db", os.Getpid(), fanoutTestCounter)
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

func TestFanOut_Basic(t *testing.T) {
	queue, store := setupFanOutTestQueue(t)

	var processedCount atomic.Int32

	queue.Register("processItem", func(ctx context.Context, n int) (int, error) {
		processedCount.Add(1)
		return n * 2, nil
	})

	queue.Register("batchJob", func(ctx context.Context, items []int) error {
		subJobs := make([]jobs.SubJob, len(items))
		for i, item := range items {
			subJobs[i] = jobs.Sub("processItem", item)
		}

		results, err := jobs.FanOut[int](ctx, subJobs, jobs.FailFast())
		if err != nil {
			return err
		}

		// Verify results
		values := jobs.Values(results)
		if len(values) != len(items) {
			return fmt.Errorf("expected %d results, got %d", len(items), len(values))
		}

		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := queue.Enqueue(ctx, "batchJob", []int{1, 2, 3, 4, 5})
	require.NoError(t, err)

	worker := queue.NewWorker()
	go worker.Start(ctx)

	// Wait for completion
	time.Sleep(5 * time.Second)

	assert.Equal(t, int32(5), processedCount.Load())
}
```

**Step 2: Run integration test**

```bash
go test ./tests/fanout_test.go -v -timeout 60s
```

Note: This test may fail until all components are wired together correctly. That's expected.

**Step 3: Commit**

```bash
git add tests/fanout_test.go
git commit -m "test: add integration test for fan-out"
```

---

## Remaining Tasks (Summary)

The following tasks should be implemented after the core functionality is working:

### Task 22-25: Additional Integration Tests
- TestFanOut_FailFast
- TestFanOut_CollectAll
- TestFanOut_Threshold
- TestFanOut_ResumeAfterCrash

### Task 26-28: Timeout Handling
- Fan-out timeout checker goroutine
- Sub-job cancellation on timeout
- Parent resume with timeout error

### Task 29-32: UI Updates
- Add fan-out info to GetJobResponse
- Add job tree view endpoint
- Add fan-out progress endpoint
- Update frontend components

### Task 33-35: Documentation
- Update README with fan-out examples
- Add fan-out usage guide
- Update API documentation

---

## Commands Reference

```bash
# Run all tests
go test ./... -v

# Run specific package tests
go test ./pkg/fanout/... -v

# Run integration tests
go test ./tests/... -v -timeout 120s

# Build everything
go build ./...

# Check for issues
go vet ./...
```
