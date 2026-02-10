# Sub-Jobs and Fan-Out/Fan-In Design

## Overview

Add support for sub-jobs with fan-out/fan-in patterns to enable:
- **Parallel data processing** - Spawn N sub-jobs, wait for all, aggregate results
- **Workflow orchestration** - Sequential and parallel job dependencies (A → B+C → D)
- **Dynamic task graphs** - Runtime decisions on sub-job count based on input data

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Execution model | Truly parallel (distributed) | Sub-jobs are independent DB records picked up by any worker. Scales horizontally, survives crashes. |
| Fan-in mechanism | Event-driven + polling fallback | Events for speed, polling as safety net for missed events. |
| Failure handling | Configurable per fan-out | FailFast, CollectAll, or Threshold - different workflows need different strategies. |
| API style | Hybrid (imperative + helpers) | Go-idiomatic, allows runtime decisions, no DSL to learn. |
| Result passing | Slice + streaming options | Slice for small batches, streaming for large batches to prevent OOM. |

---

## Data Model

### Job struct additions

```go
type Job struct {
    // ... existing fields ...

    // Parent-child relationship
    ParentJobID  *string   `gorm:"index;size:36"`
    RootJobID    *string   `gorm:"index;size:36"`  // Top-level workflow job

    // Fan-out tracking
    FanOutID     *string   `gorm:"index;size:36"`  // Groups sibling sub-jobs
    FanOutIndex  int       `gorm:"default:0"`      // Position in fan-out batch

    // Result storage for parent retrieval
    Result       []byte    `gorm:"type:bytes"`     // Serialized return value
}
```

### New FanOut tracking table

```go
type FanOut struct {
    ID              string         `gorm:"primaryKey;size:36"`
    ParentJobID     string         `gorm:"index;size:36;not null"`
    TotalCount      int            `gorm:"not null"`
    CompletedCount  int            `gorm:"default:0"`
    FailedCount     int            `gorm:"default:0"`
    Strategy        string         `gorm:"size:20"` // "fail_fast", "collect_all", "threshold"
    Threshold       float64        `gorm:"default:1.0"`
    Status          string         `gorm:"size:20;default:'pending'"` // pending, completed, failed
    Timeout         *time.Duration // Max duration for entire fan-out
    TimeoutAt       *time.Time     // Computed deadline
    CancelOnFail    bool           // Cancel sub-jobs if parent fails
    CreatedAt       time.Time      `gorm:"autoCreateTime"`
}
```

### New job statuses

```go
const (
    StatusPending   JobStatus = "pending"
    StatusRunning   JobStatus = "running"
    StatusWaiting   JobStatus = "waiting"    // Suspended waiting for sub-jobs
    StatusCompleted JobStatus = "completed"
    StatusFailed    JobStatus = "failed"
    StatusCancelled JobStatus = "cancelled"  // Terminated before completion
)
```

---

## Core API

### Spawning sub-jobs (fan-out)

```go
// Sub creates a sub-job definition (not yet enqueued)
func Sub(jobType string, args any, opts ...Option) SubJob

// FanOut spawns sub-jobs in parallel and waits for all results
// Checkpoints progress - safe to retry if parent crashes
func FanOut[T any](ctx context.Context, subJobs []SubJob, opts ...FanOutOption) ([]Result[T], error)

// FanOutStream spawns sub-jobs and streams results as they complete
// For large batches where loading all results into memory isn't feasible
func FanOutStream[T any](ctx context.Context, subJobs []SubJob, opts ...FanOutOption) <-chan Result[T]
```

### Result wrapper

```go
type Result[T any] struct {
    Index int    // Position in original subJobs slice
    Value T      // Result if successful
    Err   error  // Error if failed
}
```

### Fan-out options

```go
func FailFast() FanOutOption              // Fail parent on first sub-job failure
func CollectAll() FanOutOption            // Wait for all, return partial results
func Threshold(pct float64) FanOutOption  // Ok if pct% succeed (e.g., 0.9 = 90%)
func WithQueue(q string) FanOutOption     // Run sub-jobs on specific queue
func WithPriority(p int) FanOutOption     // Sub-job priority
func WithRetries(n int) FanOutOption      // Override retry count for sub-jobs
func WithSubJobTimeout(d time.Duration) FanOutOption  // Per sub-job timeout
func WithTimeout(d time.Duration) FanOutOption        // Entire fan-out timeout
func CancelOnParentFailure() FanOutOption             // Kill sub-jobs if parent fails
```

### Usage example

```go
func ProcessBatch(ctx context.Context, items []Item) error {
    // Build sub-jobs dynamically
    subJobs := make([]jobs.SubJob, len(items))
    for i, item := range items {
        subJobs[i] = jobs.Sub("processItem", item)
    }

    // Fan-out and wait
    results, err := jobs.FanOut[ProcessedItem](ctx, subJobs, jobs.FailFast())
    if err != nil {
        return err
    }

    // Aggregate successful results
    var processed []ProcessedItem
    for _, r := range results {
        if r.Err == nil {
            processed = append(processed, r.Value)
        }
    }

    return jobs.Call[any](ctx, "saveResults", processed)
}
```

---

## Execution Flow

### When FanOut is called

1. **Create FanOut record** - Insert row with TotalCount, strategy, status=pending
2. **Enqueue sub-jobs** - Batch insert all sub-jobs with ParentJobID, FanOutID, FanOutIndex
3. **Checkpoint parent** - Save checkpoint with FanOutID so replay knows to wait
4. **Suspend parent** - Set parent job status to `waiting`, release worker

### When each sub-job completes

1. **Store result** - Save serialized result in `job.Result` field
2. **Increment counter** - Atomic `UPDATE fan_outs SET completed_count = completed_count + 1`
3. **Check completion** - If `completed_count + failed_count == total_count`:
   - Set `fan_out.status = completed` (or `failed` based on strategy)
   - **Resume parent** - Set parent job status back to `pending`

### When sub-job fails (after retries exhausted)

1. **Increment failed counter** - Atomic update
2. **Check strategy:**
   - `FailFast`: Immediately mark fan-out as failed, resume parent
   - `CollectAll`: Continue, only resume parent when all done
   - `Threshold`: Check if failure rate exceeds threshold, fail early if so

### Polling fallback (safety net)

Worker periodically checks for `waiting` jobs where fan-out is complete but parent wasn't resumed:

```sql
SELECT * FROM jobs
WHERE status = 'waiting'
AND EXISTS (
    SELECT 1 FROM fan_outs
    WHERE fan_outs.parent_job_id = jobs.id
    AND fan_outs.status IN ('completed', 'failed')
)
```

---

## Retry & Timeout Handling

### Sub-job retries

```go
// Sub-jobs inherit parent's retry config by default
jobs.Sub("processItem", item)  // Uses parent's MaxRetries

// Override per sub-job
jobs.Sub("processItem", item, jobs.Retries(5))

// Override for entire fan-out
jobs.FanOut[T](ctx, subJobs, jobs.WithRetries(5))
```

### Timeouts

```go
// Per sub-job timeout
jobs.Sub("processItem", item, jobs.Timeout(5*time.Minute))

// Fan-out level (all sub-jobs)
jobs.FanOut[T](ctx, subJobs, jobs.WithSubJobTimeout(5*time.Minute))

// Entire fan-out must complete within duration
jobs.FanOut[T](ctx, subJobs, jobs.WithTimeout(1*time.Hour))
```

### Timeout behavior

1. Mark fan-out as `failed` with error "timeout exceeded"
2. Cancel in-progress sub-jobs - Set status to `cancelled`
3. Resume parent with timeout error
4. Optional: `CancelOnParentFailure()` to kill sub-jobs if parent fails

---

## Error Handling & Result Aggregation

### Error types

```go
type FanOutError struct {
    FanOutID     string
    TotalCount   int
    FailedCount  int
    Strategy     string
    Failures     []SubJobFailure
}

type SubJobFailure struct {
    Index   int
    JobID   string
    Error   string
    Attempt int
}

func (e *FanOutError) Error() string {
    return fmt.Sprintf("fan-out failed: %d/%d sub-jobs failed", e.FailedCount, e.TotalCount)
}
```

### Result aggregation patterns

```go
// Pattern 1: All must succeed (FailFast)
results, err := jobs.FanOut[Item](ctx, subJobs, jobs.FailFast())
if err != nil {
    return err
}
items := jobs.Values(results)  // Extract []Item

// Pattern 2: Collect partial results (CollectAll)
results, err := jobs.FanOut[Item](ctx, subJobs, jobs.CollectAll())
successes, failures := jobs.Partition(results)

// Pattern 3: Threshold
results, err := jobs.FanOut[Item](ctx, subJobs, jobs.Threshold(0.8))
// Fails only if more than 20% fail
```

### Helper functions

```go
func Values[T any](results []Result[T]) []T
func Partition[T any](results []Result[T]) ([]T, []error)
func AllSucceeded[T any](results []Result[T]) bool
```

---

## Workflow Orchestration

### Chaining fan-outs

```go
func OrderWorkflow(ctx context.Context, order Order) error {
    // Step 1: Validate (single job)
    validated, err := jobs.Call[ValidatedOrder](ctx, "validateOrder", order)
    if err != nil {
        return err
    }

    // Step 2: Fan-out - process line items in parallel
    itemJobs := make([]jobs.SubJob, len(validated.Items))
    for i, item := range validated.Items {
        itemJobs[i] = jobs.Sub("processLineItem", item)
    }
    itemResults, err := jobs.FanOut[ProcessedItem](ctx, itemJobs, jobs.FailFast())
    if err != nil {
        return err
    }

    // Step 3: Fan-out - parallel post-processing
    postJobs := []jobs.SubJob{
        jobs.Sub("sendNotification", order.CustomerID),
        jobs.Sub("generateInvoice", order.ID),
    }
    _, err = jobs.FanOut[any](ctx, postJobs, jobs.CollectAll())

    // Step 4: Finalize
    return jobs.Call[any](ctx, "finalizeOrder", order.ID)
}
```

### Nested fan-outs

```go
func ProcessCategory(ctx context.Context, category Category) error {
    productJobs := make([]jobs.SubJob, len(category.Products))
    for i, p := range category.Products {
        productJobs[i] = jobs.Sub("processProduct", p)
    }
    _, err := jobs.FanOut[any](ctx, productJobs, jobs.Threshold(0.9))
    return err
}

func processProduct(ctx context.Context, product Product) error {
    // Nested fan-out for variants
    variantJobs := make([]jobs.SubJob, len(product.Variants))
    for i, v := range product.Variants {
        variantJobs[i] = jobs.Sub("processVariant", v)
    }
    _, err := jobs.FanOut[any](ctx, variantJobs, jobs.FailFast())
    return err
}
```

The `RootJobID` field tracks the top-level workflow for querying the entire job tree.

---

## Observability & UI

### API additions

```go
type GetJobResponse struct {
    Job         *Job
    Checkpoints []*Checkpoint
    ParentJob   *Job           // If this is a sub-job
    FanOuts     []*FanOutInfo  // Fan-outs initiated by this job
    SubJobs     []*Job         // Direct children (paginated)
}

type FanOutInfo struct {
    ID             string
    TotalCount     int
    CompletedCount int
    FailedCount    int
    Status         string
    Strategy       string
    CreatedAt      time.Time
}
```

### New filters

```go
type ListJobsRequest struct {
    // ... existing ...
    ParentJobID  string  // Children of this job
    RootJobID    string  // Entire workflow tree
    HasSubJobs   *bool   // Jobs with/without sub-jobs
    FanOutID     string  // Jobs in a fan-out batch
}
```

### Dashboard stats

```go
type GetStatsResponse struct {
    // ... existing ...
    TotalWaiting    int64  // Jobs waiting for sub-jobs
    ActiveFanOuts   int64  // In-progress fan-out batches
    ActiveWorkflows int64  // Root jobs with pending sub-jobs
}
```

### UI features

- Job tree view - Expandable parent → children hierarchy
- Fan-out progress - Progress bar (completed/failed/pending)
- Workflow timeline - Visual execution timeline with parallel branches
- Bulk actions - Cancel workflow, retry workflow, view graph

---

## Testing Strategy

### Unit tests

```go
func TestFanOut_FailFast(t *testing.T)
func TestFanOut_CollectAll(t *testing.T)
func TestFanOut_Threshold(t *testing.T)
func TestFanOut_ResumeAfterCrash(t *testing.T)
func TestFanOut_Timeout(t *testing.T)
func TestFanOut_CancelOnParentFailure(t *testing.T)
```

### Integration tests

```go
func TestNestedFanOut(t *testing.T)
func TestWorkflowResume(t *testing.T)
func TestFanOutWithPollingFallback(t *testing.T)
```

### Test helpers

```go
func (q *TestQueue) CompleteSubJob(jobID string, result any)
func (q *TestQueue) FailSubJob(jobID string, err error)
func (q *TestQueue) WaitForFanOutComplete(fanOutID string, timeout time.Duration)
```

---

## Implementation Order

1. **Data model** - Add new fields to Job, create FanOut table, add new statuses
2. **Storage layer** - CRUD for FanOut, atomic counter updates, parent resume logic
3. **Core API** - Sub(), FanOut(), FanOutStream(), options
4. **Worker changes** - Handle `waiting` status, polling fallback, sub-job completion triggers
5. **Timeout handling** - Fan-out timeouts, cancellation propagation
6. **UI updates** - Job tree view, fan-out progress, new filters
7. **Tests** - Unit and integration tests for all scenarios
