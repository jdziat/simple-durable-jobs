# Persist Workflow Result Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist top-level workflow handler return values to `Job.Result` so consumers can read them via a typed `LoadResult[T]` facade. Same fix unblocks fan-out sub-job result collection (currently broken in production — `pkg/fanout/fanout.go:263` reads `Job.Result` but nothing writes it).

**Architecture:** `Handler.Execute` (internal) gains a `[]byte` return alongside `error` carrying the JSON-marshaled handler return value. Worker captures those bytes and calls the already-existing `Storage.SaveJobResult` before `Storage.Complete`. Facade adds `*Queue.LoadStatus(ctx, id)` and a generic top-level `jobs.LoadResult[T](ctx, q, id)` function for type-safe result reads.

**Tech Stack:** Go 1.22+, GORM, sqlite/postgres/mysql, testify.

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `pkg/core/errors.go` | Modify | Add `ErrJobNotCompleted` sentinel for `LoadResult[T]` |
| `pkg/internal/handler/handler.go` | Modify | `Execute` returns `(resultJSON []byte, err error)` |
| `pkg/internal/handler/handler_test.go` | Modify | Update tests for new arity; cover bytes return |
| `pkg/queue/queue.go` | Modify | `CallDirect` discards bytes (preserve API); add `LoadStatus` method |
| `pkg/worker/worker.go` | Modify | Capture handler bytes; `SaveJobResult` before `Complete` |
| `pkg/worker/worker_test.go` | Modify | Assert `SaveJobResult` invoked with handler bytes |
| `jobs.go` | Modify | Add `LoadResult[T]` and re-export `LoadStatus` & `ErrJobNotCompleted` |
| `facade_test.go` | Modify | Test `LoadStatus` and `LoadResult[T]` |
| `tests/fanout_test.go` | Modify | Add result-collection assertion to existing fan-out integration test |
| `CHANGELOG.md` | Modify | Document the v1.2.0 feature |

---

## Task 1: Add `ErrJobNotCompleted` sentinel

**Files:**
- Modify: `pkg/core/errors.go`
- Test: `pkg/core/errors_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/core/errors_test.go`:

```go
func TestErrJobNotCompleted_Defined(t *testing.T) {
	require.Error(t, core.ErrJobNotCompleted)
	assert.Contains(t, core.ErrJobNotCompleted.Error(), "not completed")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/core/ -run TestErrJobNotCompleted_Defined -v`
Expected: FAIL with `undefined: core.ErrJobNotCompleted`.

- [ ] **Step 3: Add the error**

In `pkg/core/errors.go`, inside the existing `var (...)` block, add:

```go
	ErrJobNotCompleted    = errors.New("jobs: job has not completed")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/core/ -run TestErrJobNotCompleted_Defined -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/core/errors.go pkg/core/errors_test.go
git commit -m "feat(core): add ErrJobNotCompleted sentinel"
```

---

## Task 2: Change `Handler.Execute` signature to `([]byte, error)`

**Files:**
- Modify: `pkg/internal/handler/handler.go:78-113`
- Modify: `pkg/internal/handler/handler_test.go` (all `Execute` callers)

The handler currently returns only `error`, dropping `results[0]` for `(T, error)` handlers. Change it to JSON-marshal `results[0]` and return alongside the error.

- [ ] **Step 1: Write the failing test**

Append to `pkg/internal/handler/handler_test.go`:

```go
func TestHandler_Execute_TwoReturnValues_ReturnsResultBytes(t *testing.T) {
	type out struct {
		Name string `json:"name"`
		Code int    `json:"code"`
	}
	fn := func(ctx context.Context, in struct{}) (out, error) {
		return out{Name: "video-42", Code: 7}, nil
	}
	h, err := handler.NewHandler(fn)
	require.NoError(t, err)

	resultBytes, err := h.Execute(context.Background(), []byte("{}"))
	require.NoError(t, err)

	var got out
	require.NoError(t, json.Unmarshal(resultBytes, &got))
	assert.Equal(t, "video-42", got.Name)
	assert.Equal(t, 7, got.Code)
}

func TestHandler_Execute_ErrorOnly_ReturnsNilBytes(t *testing.T) {
	fn := func(ctx context.Context, in struct{}) error {
		return nil
	}
	h, err := handler.NewHandler(fn)
	require.NoError(t, err)

	resultBytes, err := h.Execute(context.Background(), []byte("{}"))
	require.NoError(t, err)
	assert.Nil(t, resultBytes)
}

func TestHandler_Execute_TwoReturnValues_ErrorReturnsNilBytes(t *testing.T) {
	fn := func(ctx context.Context, in struct{}) (string, error) {
		return "should-be-ignored", errors.New("boom")
	}
	h, err := handler.NewHandler(fn)
	require.NoError(t, err)

	resultBytes, err := h.Execute(context.Background(), []byte("{}"))
	require.Error(t, err)
	assert.Nil(t, resultBytes, "result bytes must be nil when handler returns an error")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/internal/handler/ -run "TestHandler_Execute_TwoReturnValues_ReturnsResultBytes|TestHandler_Execute_ErrorOnly_ReturnsNilBytes|TestHandler_Execute_TwoReturnValues_ErrorReturnsNilBytes" -v`
Expected: FAIL — current `Execute` returns only `error`, so `resultBytes, err := h.Execute(...)` won't compile (`assignment mismatch`).

- [ ] **Step 3: Change `Handler.Execute` signature**

Replace `pkg/internal/handler/handler.go:77-113` with:

```go
// Execute runs the handler with the given context and arguments.
// Returns the JSON-marshaled return value (or nil for error-only handlers
// or when the handler returned an error) and any handler error.
func (h *Handler) Execute(ctx context.Context, argsJSON []byte) ([]byte, error) {
	// Defensive check: ensure handler function is valid
	if !h.Fn.IsValid() || h.Fn.IsNil() {
		return nil, fmt.Errorf("handler function is nil or invalid")
	}

	var args []reflect.Value

	if h.HasContext {
		args = append(args, reflect.ValueOf(ctx))
	}

	if h.ArgsType != nil {
		argVal := reflect.New(h.ArgsType)
		if err := json.Unmarshal(argsJSON, argVal.Interface()); err != nil {
			return nil, fmt.Errorf("failed to unmarshal args: %w", err)
		}
		args = append(args, argVal.Elem())
	}

	results := h.Fn.Call(args)

	// Handle return values
	numOut := h.Fn.Type().NumOut()
	switch numOut {
	case 1:
		if !results[0].IsNil() {
			return nil, results[0].Interface().(error)
		}
		return nil, nil
	case 2:
		if !results[1].IsNil() {
			return nil, results[1].Interface().(error)
		}
		// Marshal the return value to JSON bytes.
		resultBytes, marshalErr := json.Marshal(results[0].Interface())
		if marshalErr != nil {
			return nil, fmt.Errorf("failed to marshal handler result: %w", marshalErr)
		}
		return resultBytes, nil
	}
	return nil, nil
}
```

- [ ] **Step 4: Update existing handler tests for new arity**

Search and update all existing `Execute` callers in `pkg/internal/handler/handler_test.go` from:

```go
err := h.Execute(ctx, args)
```

to:

```go
_, err := h.Execute(ctx, args)
```

Use this command to find them: `grep -n "h\.Execute(\|handler\.Execute(\|.Execute(ctx" pkg/internal/handler/handler_test.go`. Patch each callsite individually with the underscore for the new bytes return.

- [ ] **Step 5: Run all handler tests**

Run: `go test ./pkg/internal/handler/ -v`
Expected: PASS — all existing tests plus the three new ones.

- [ ] **Step 6: Commit**

```bash
git add pkg/internal/handler/handler.go pkg/internal/handler/handler_test.go
git commit -m "feat(handler): return result bytes from Execute"
```

---

## Task 3: Update `CallDirect` to discard bytes

**Files:**
- Modify: `pkg/queue/queue.go:119-127`

`CallDirect` is part of the public `Queue` API and returns only `error`. We discard the bytes to preserve the public signature. Compilation will already be broken from Task 2 — this task fixes the build.

- [ ] **Step 1: Verify build is broken**

Run: `go build ./...`
Expected: FAIL with `cannot use h.Execute(ctx, argsJSON) (multiple-value error) ...` at `pkg/queue/queue.go:126`.

- [ ] **Step 2: Update `CallDirect`**

Replace `pkg/queue/queue.go:126` (`return h.Execute(ctx, argsJSON)`) with:

```go
	_, err := h.Execute(ctx, argsJSON)
	return err
```

- [ ] **Step 3: Verify build passes**

Run: `go build ./...`
Expected: PASS (worker callsite still broken — that's Task 4).

Note: if `go build` still fails on `pkg/worker/worker.go:422`, that's expected; Task 4 fixes it.

- [ ] **Step 4: Commit**

```bash
git add pkg/queue/queue.go
git commit -m "refactor(queue): adapt CallDirect to new Execute signature"
```

---

## Task 4: Worker captures and persists handler result

**Files:**
- Modify: `pkg/worker/worker.go:294-329, 365-423`
- Modify: `pkg/worker/worker_test.go:199` (mockStorage.SaveJobResult)

Worker captures the bytes from `executeHandler`, then calls `SaveJobResult` on the success path before `Complete`. Both run while the worker still holds the lock.

- [ ] **Step 1: Make mockStorage record SaveJobResult calls**

In `pkg/worker/worker_test.go`, find `mockStorage` and:

1. Add a field to capture saved results. Locate the `mockStorage` struct definition and add:

```go
	savedResults map[string][]byte
```

2. Initialize it where `mockStorage` is constructed (search for `&mockStorage{` and `mockStorage{` in the file — initialize `savedResults: map[string][]byte{}` for each).

3. Replace the existing `SaveJobResult` no-op (line 199) with:

```go
func (m *mockStorage) SaveJobResult(_ context.Context, jobID string, _ string, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.savedResults == nil {
		m.savedResults = map[string][]byte{}
	}
	m.savedResults[jobID] = result
	return nil
}
```

(If `mockStorage` does not already have a `mu sync.Mutex`, add one — many of its other methods will need protection too. If concurrency is not a concern for this mock, drop the lock and use plain map writes — match the pattern of the surrounding methods.)

- [ ] **Step 2: Write the failing test**

Append to `pkg/worker/worker_test.go`:

```go
func TestWorker_PersistsHandlerResult(t *testing.T) {
	store := newMockStorage()
	q := queue.New(store)
	q.Register("make-video", func(ctx context.Context, name string) (map[string]any, error) {
		return map[string]any{"name": name, "duration_s": 12}, nil
	})

	jobID, err := q.Enqueue(context.Background(), "make-video", "demo")
	require.NoError(t, err)

	w := worker.NewWorker(q, worker.WithScheduler(false), worker.WithWorkerID("w1"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		_, ok := store.savedResults[jobID]
		return ok
	}, 2*time.Second, 10*time.Millisecond, "handler result must be persisted via SaveJobResult")

	store.mu.Lock()
	got := store.savedResults[jobID]
	store.mu.Unlock()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(got, &decoded))
	assert.Equal(t, "demo", decoded["name"])
}
```

(Adapt `newMockStorage()` and `worker.WithWorkerID` to whatever constructors exist in the file — match the existing test style. If the surrounding tests use a different way to drive the worker through one job, follow that pattern.)

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./pkg/worker/ -run TestWorker_PersistsHandlerResult -v`
Expected: FAIL — `savedResults` is empty because the worker still doesn't call `SaveJobResult`.

- [ ] **Step 4: Update `executeHandler` to return bytes**

Replace `pkg/worker/worker.go:365` (signature of `executeHandler`) and the final return at line 422.

Change line 365 from:
```go
func (w *Worker) executeHandler(ctx context.Context, job *core.Job, h *handler.Handler) (err error) {
```
to:
```go
func (w *Worker) executeHandler(ctx context.Context, job *core.Job, h *handler.Handler) (resultBytes []byte, err error) {
```

Update the panic-recovery block (lines 366-399) to leave `resultBytes` nil on panic — the named return defaults are sufficient, no code change required to that block other than awareness.

Change line 422 from:
```go
	return h.Execute(jobCtx, job.Args)
```
to:
```go
	return h.Execute(jobCtx, job.Args)
```

(That literal line is already correct — `Execute` now returns `([]byte, error)`. The change is in the function signature only.)

- [ ] **Step 5: Update the worker success path to save result**

In `pkg/worker/worker.go:294`, replace the call site:

```go
	err := w.executeHandler(jobCtx, job, h)
```

with:

```go
	resultBytes, err := w.executeHandler(jobCtx, job, h)
```

Then locate the success branch (after `if err == nil` — the block around lines 305-318 that calls `completeWithRetry`). Just before the existing `completeErr := w.completeWithRetry(...)` call, insert:

```go
		if resultBytes != nil {
			if saveErr := w.queue.Storage().SaveJobResult(ctx, job.ID, w.config.WorkerID, resultBytes); saveErr != nil {
				w.logger.Error("failed to persist job result", "job_id", job.ID, "error", saveErr)
				// fall through — completion still proceeds
			}
		}
```

(Open the file and confirm the exact location — the success branch starts after the `WaitingError` and error-handling branches. Read 30 lines around line 294 first to land the patch correctly.)

- [ ] **Step 6: Run worker tests**

Run: `go test ./pkg/worker/ -v -count=1`
Expected: PASS — including the new test.

- [ ] **Step 7: Commit**

```bash
git add pkg/worker/worker.go pkg/worker/worker_test.go
git commit -m "feat(worker): persist handler result before completing job"
```

---

## Task 5: Add `LoadStatus` method on `*Queue`

**Files:**
- Modify: `pkg/queue/queue.go`
- Test: `pkg/queue/queue_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/queue/queue_test.go`:

```go
func TestQueue_LoadStatus_ReturnsJobStatus(t *testing.T) {
	store := newMockStorage()
	q := queue.New(store)

	store.mu.Lock()
	store.jobs["job-1"] = &core.Job{ID: "job-1", Status: core.StatusCompleted}
	store.mu.Unlock()

	status, err := q.LoadStatus(context.Background(), "job-1")
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, status)
}

func TestQueue_LoadStatus_UnknownJob(t *testing.T) {
	store := newMockStorage()
	q := queue.New(store)

	_, err := q.LoadStatus(context.Background(), "missing")
	require.Error(t, err)
}
```

(Adapt `newMockStorage`, `store.jobs`, and the locking pattern to whatever the existing `mockStorage` in this file uses — read the file's existing tests first.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/queue/ -run TestQueue_LoadStatus -v`
Expected: FAIL — `q.LoadStatus undefined`.

- [ ] **Step 3: Implement the method**

Append to `pkg/queue/queue.go` (anywhere after `func (q *Queue) Storage()`):

```go
// LoadStatus returns the current status of a job by ID.
// Returns an error if the job cannot be found.
func (q *Queue) LoadStatus(ctx context.Context, jobID string) (core.JobStatus, error) {
	job, err := q.storage.GetJob(ctx, jobID)
	if err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("jobs: job not found: %s", jobID)
	}
	return job.Status, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/queue/ -run TestQueue_LoadStatus -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/queue/queue.go pkg/queue/queue_test.go
git commit -m "feat(queue): add LoadStatus method"
```

---

## Task 6: Add `LoadResult[T]` to facade

**Files:**
- Modify: `jobs.go`
- Test: `facade_test.go`

`LoadResult[T]` is a top-level generic function (Go methods can't be generic). It reads `Job.Result` and JSON-decodes into `T`. Returns `core.ErrJobNotCompleted` for non-terminal statuses, the wrapped `LastError` for failed jobs, and the zero value with nil error when `Result` is nil on a completed job (handler returned only `error`).

- [ ] **Step 1: Write failing tests**

Append to `facade_test.go`:

```go
func TestLoadResult_DecodesCompletedJob(t *testing.T) {
	q, store := newFacadeTestQueue(t)

	type Video struct {
		Name string `json:"name"`
		Sec  int    `json:"sec"`
	}
	want := Video{Name: "demo", Sec: 12}
	resultBytes, err := json.Marshal(want)
	require.NoError(t, err)

	require.NoError(t, store.Enqueue(context.Background(), &core.Job{
		ID:     "job-ok",
		Type:   "x",
		Status: core.StatusCompleted,
		Result: resultBytes,
	}))

	got, err := jobs.LoadResult[Video](context.Background(), q, "job-ok")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestLoadResult_NotCompleted(t *testing.T) {
	q, store := newFacadeTestQueue(t)
	require.NoError(t, store.Enqueue(context.Background(), &core.Job{
		ID:     "job-pending",
		Type:   "x",
		Status: core.StatusPending,
	}))

	_, err := jobs.LoadResult[string](context.Background(), q, "job-pending")
	require.ErrorIs(t, err, core.ErrJobNotCompleted)
}

func TestLoadResult_FailedJobReturnsError(t *testing.T) {
	q, store := newFacadeTestQueue(t)
	require.NoError(t, store.Enqueue(context.Background(), &core.Job{
		ID:        "job-failed",
		Type:      "x",
		Status:    core.StatusFailed,
		LastError: "boom",
	}))

	_, err := jobs.LoadResult[string](context.Background(), q, "job-failed")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestLoadResult_CompletedWithNilResult(t *testing.T) {
	q, store := newFacadeTestQueue(t)
	require.NoError(t, store.Enqueue(context.Background(), &core.Job{
		ID:     "job-no-result",
		Type:   "x",
		Status: core.StatusCompleted,
	}))

	got, err := jobs.LoadResult[string](context.Background(), q, "job-no-result")
	require.NoError(t, err)
	assert.Equal(t, "", got)
}
```

If `newFacadeTestQueue` does not exist in `facade_test.go`, add a small helper near the top:

```go
func newFacadeTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	store := jobs.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return jobs.New(store), store
}
```

(Match the imports already present in `facade_test.go` — read it first to avoid duplicate-import compile errors.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test . -run TestLoadResult -v`
Expected: FAIL — `jobs.LoadResult undefined`.

- [ ] **Step 3: Implement `LoadResult[T]`**

Append to `jobs.go` (after the type alias block, near the bottom of the file):

```go
// LoadResult decodes the persisted return value of a completed job into T.
//
// Returns:
//   - core.ErrJobNotCompleted if the job has not reached a terminal state.
//   - An error wrapping job.LastError if the job failed.
//   - The zero value of T with nil error if the job completed but the
//     handler returned only an error (no result value).
func LoadResult[T any](ctx context.Context, q *Queue, jobID string) (T, error) {
	var zero T
	job, err := q.Storage().GetJob(ctx, jobID)
	if err != nil {
		return zero, err
	}
	if job == nil {
		return zero, fmt.Errorf("jobs: job not found: %s", jobID)
	}
	switch job.Status {
	case core.StatusCompleted:
		if job.Result == nil {
			return zero, nil
		}
		var out T
		if err := json.Unmarshal(job.Result, &out); err != nil {
			return zero, fmt.Errorf("jobs: failed to decode result: %w", err)
		}
		return out, nil
	case core.StatusFailed:
		return zero, fmt.Errorf("jobs: job %s failed: %s", jobID, job.LastError)
	default:
		return zero, core.ErrJobNotCompleted
	}
}
```

Add `encoding/json` and `fmt` to the import block at the top of `jobs.go` if not already present.

- [ ] **Step 4: Re-export the new error**

In `jobs.go`, find the `// Error variables` (or similar) section that re-exports `ErrJobNotOwned`, `ErrCannotPauseStatus`, etc. Add:

```go
	ErrJobNotCompleted    = core.ErrJobNotCompleted
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test . -run TestLoadResult -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add jobs.go facade_test.go
git commit -m "feat: add LoadResult[T] facade for typed job result reads"
```

---

## Task 7: Integration test — fan-out result collection now works

**Files:**
- Modify: `tests/fanout_test.go:21-79` (extend `TestFanOut_Basic`)

The existing test asserts sub-jobs ran but never validated that `jobs.Values(results)` returns the actual values — they would have been zero-valued in production prior to this fix. Extend it to assert real values come back.

- [ ] **Step 1: Strengthen the existing assertion**

Open `tests/fanout_test.go` and locate `TestFanOut_Basic` (line 21). Find the parent handler (`batchMultiply`, line 36) and the `Enqueue` call (line 57). After the existing `processedCount` assertion on line 78, append:

```go
	// Verify the parent workflow's return value is now persisted and decodes correctly.
	require.Eventually(t, func() bool {
		status, err := queue.LoadStatus(ctx, jobID)
		return err == nil && status == jobs.StatusCompleted
	}, 5*time.Second, 50*time.Millisecond, "parent job must reach completed")

	got, err := jobs.LoadResult[[]int](ctx, queue, jobID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, got, "parent must return doubled values from sub-jobs")
```

You'll also need to capture the `jobID` — change line 57 from:

```go
	_, err := queue.Enqueue(ctx, "batchMultiply", []int{1, 2, 3, 4, 5})
```

to:

```go
	jobID, err := queue.Enqueue(ctx, "batchMultiply", []int{1, 2, 3, 4, 5})
```

- [ ] **Step 2: Run the integration test**

Run: `go test ./tests/ -run TestFanOut_Basic -v -count=1`
Expected: PASS — sub-jobs run, `Job.Result` is populated for each, `fanout.Result[T].Value` carries the doubled int, parent returns `[]int{2,4,6,8,10}`, `LoadResult[[]int]` decodes it.

- [ ] **Step 3: Run the full integration suite once**

Run: `go test ./tests/ -count=1 -timeout=120s`
Expected: PASS. If a previously-passing test now fails because it implicitly relied on `Job.Result` being nil, fix the test to expect the new (correct) behavior.

- [ ] **Step 4: Commit**

```bash
git add tests/fanout_test.go
git commit -m "test(fanout): assert parent workflow result via LoadResult"
```

---

## Task 8: Update CHANGELOG.md

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add a v1.2.0 entry**

Insert at the top of `CHANGELOG.md` (after the `# Changelog` header):

```markdown
## [1.2.0](https://github.com/jdziat/simple-durable-jobs/compare/v1.1.0...v1.2.0) (2026-05-01)


### Features

* persist top-level workflow handler return values to `Job.Result`
* add `Queue.LoadStatus(ctx, id)` for status reads outside a job handler
* add `jobs.LoadResult[T](ctx, q, id)` generic facade for type-safe result reads
* add `core.ErrJobNotCompleted` sentinel returned by `LoadResult[T]` for non-terminal jobs


### Bug Fixes

* fan-out sub-job results are now actually persisted to `Job.Result`, fixing silent zero-value reads in `pkg/fanout/fanout.go` result collection
```

(If the project uses `go-semantic-release` to auto-generate the changelog from commits — visible in `2eeba7a` history — skip this manual edit and let the release tool generate it. Confirm with the maintainer; commits already follow conventional-commit format from the prior tasks.)

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for v1.2.0"
```

---

## Final Verification

- [ ] **Run full test suite**

Run: `go test ./... -count=1 -timeout=180s`
Expected: PASS across all packages.

- [ ] **Run linters**

Run: `make lint` (or whatever the project uses — check `Makefile`).
Expected: clean.

- [ ] **Smoke check: build**

Run: `go build ./...`
Expected: PASS.

---

## Self-Review Notes

- **Spec coverage:** All three user decisions covered — (1) library-side fix, (2) checked fan-out semantics & confirmed no double-write conflict (Task 7 documents that the existing pre-population masking is replaced by real worker-driven persistence), (3) `LoadStatus` + `LoadResult[T]` per option C.
- **Public API surface added:** `Queue.LoadStatus`, top-level `LoadResult[T]`, `core.ErrJobNotCompleted` (re-exported as `jobs.ErrJobNotCompleted`).
- **Public API surface broken:** None — `Handler.Execute` is in `pkg/internal/`. `CallDirect` keeps its `error`-only signature.
- **Schema migration:** None needed. `Job.Result []byte` already exists.
- **Storage interface:** Unchanged. `SaveJobResult` already on the interface, all in-tree mocks already implement it.
