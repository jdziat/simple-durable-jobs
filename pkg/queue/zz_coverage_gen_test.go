package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// CallDirect
// ---------------------------------------------------------------------------

func TestQueue_CallDirect_InvokesHandler(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	var got string
	q.Register("direct-job", func(ctx context.Context, args string) error {
		got = args
		return nil
	})

	err := q.CallDirect(context.Background(), "direct-job", []byte(`"hello"`))
	require.NoError(t, err)
	assert.Equal(t, "hello", got)
}

func TestQueue_CallDirect_NoHandler_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	err := q.CallDirect(context.Background(), "missing-job", []byte(`"x"`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no handler registered")
}

func TestQueue_CallDirect_HandlerReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	sentinel := errors.New("boom")
	q.Register("err-job", func(ctx context.Context, args string) error {
		return sentinel
	})

	err := q.CallDirect(context.Background(), "err-job", []byte(`"x"`))
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
}

// ---------------------------------------------------------------------------
// EnqueueRemote — does not require a registered handler
// ---------------------------------------------------------------------------

func TestQueue_EnqueueRemote_NoHandlerNeeded(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	jobID, err := q.EnqueueRemote(context.Background(), "remote-job", map[string]string{"k": "v"})
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "remote-job", job.Type)
	assert.Equal(t, "default", job.Queue)
	assert.Equal(t, core.StatusPending, job.Status)
	// No handler was registered for this type.
	assert.False(t, q.HasHandler("remote-job"))
}

func TestQueue_EnqueueRemote_WithOptions(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	jobID, err := q.EnqueueRemote(context.Background(), "remote-job", "args",
		QueueOpt("remote-queue"),
		Priority(9),
	)
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, "remote-queue", job.Queue)
	assert.Equal(t, 9, job.Priority)
}

// ---------------------------------------------------------------------------
// enqueueWithOptions — option/validation branches
// ---------------------------------------------------------------------------

func TestQueue_Enqueue_InvalidQueueName_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	_, err := q.EnqueueRemote(context.Background(), "job", "args", QueueOpt(""))
	require.Error(t, err)
}

func TestQueue_Enqueue_ArgsTooLarge_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// A string just over the 1MB args limit, which marshals to > MaxJobArgsSize.
	big := strings.Repeat("a", (1<<20)+16)

	_, err := q.EnqueueRemote(context.Background(), "big-job", big)
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrJobArgsTooLarge)
}

func TestQueue_Enqueue_UnmarshalableArgs_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	// channels cannot be JSON-marshaled
	_, err := q.EnqueueRemote(context.Background(), "bad-args-job", make(chan int))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal args")
}

func TestQueue_Enqueue_DelaySetsRunAt(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	before := time.Now()
	jobID, err := q.EnqueueRemote(context.Background(), "delayed-job", "args", Delay(time.Hour))
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job.RunAt)
	assert.True(t, job.RunAt.After(before.Add(30*time.Minute)))
}

func TestQueue_Enqueue_AtSetsRunAt(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	runAt := time.Now().Add(2 * time.Hour)
	jobID, err := q.EnqueueRemote(context.Background(), "at-job", "args", At(runAt))
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job.RunAt)
	assert.WithinDuration(t, runAt, *job.RunAt, time.Second)
}

func TestQueue_Enqueue_TimeoutSetsJobTimeout(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	jobID, err := q.EnqueueRemote(context.Background(), "timeout-job", "args", Timeout(45*time.Second))
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, job.Timeout)
}

func TestQueue_Enqueue_UniqueKey_UsesEnqueueUnique(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	jobID, err := q.EnqueueRemote(context.Background(), "unique-job", "args", Unique("my-key"))
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// mockStorage.EnqueueUnique stores the job like Enqueue.
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
}

func TestQueue_Enqueue_UniqueKey_TooLong_ReturnsError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	tooLong := strings.Repeat("k", 256) // > MaxUniqueKeyLength (255)
	_, err := q.EnqueueRemote(context.Background(), "unique-job", "args", Unique(tooLong))
	require.Error(t, err)
}

// duplicateUniqueStorage returns ErrDuplicateJob from EnqueueUnique.
type duplicateUniqueStorage struct {
	*mockStorage
}

func (d *duplicateUniqueStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	return core.ErrDuplicateJob
}

func TestQueue_Enqueue_UniqueKey_Duplicate_ReturnsErr(t *testing.T) {
	store := &duplicateUniqueStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueRemote(context.Background(), "unique-job", "args", Unique("dup-key"))
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrDuplicateJob)
}

// failingUniqueStorage returns a non-duplicate error from EnqueueUnique.
type failingUniqueStorage struct {
	*mockStorage
}

func (f *failingUniqueStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	return errors.New("db down")
}

func TestQueue_Enqueue_UniqueKey_StorageError_Wrapped(t *testing.T) {
	store := &failingUniqueStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueRemote(context.Background(), "unique-job", "args", Unique("key2"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue")
}

// failingEnqueueStorage returns an error from Enqueue (non-unique path).
type failingEnqueueStorage struct {
	*mockStorage
}

func (f *failingEnqueueStorage) Enqueue(ctx context.Context, job *core.Job) error {
	return errors.New("db down")
}

func TestQueue_Enqueue_StorageError_Wrapped(t *testing.T) {
	store := &failingEnqueueStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.EnqueueRemote(context.Background(), "job", "args")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enqueue")
}

// ---------------------------------------------------------------------------
// UseEnqueueMiddleware + runEnqueueMiddleware
// ---------------------------------------------------------------------------

func TestQueue_UseEnqueueMiddleware_InvokedInOrder(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	var order []string
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		order = append(order, "first-before")
		err := next(ctx, job)
		order = append(order, "first-after")
		return err
	})
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		order = append(order, "second-before")
		err := next(ctx, job)
		order = append(order, "second-after")
		return err
	})

	jobID, err := q.EnqueueRemote(context.Background(), "mw-job", "args")
	require.NoError(t, err)
	require.NotEmpty(t, jobID)

	// Registration order: first wraps second wraps persist.
	assert.Equal(t, []string{"first-before", "second-before", "second-after", "first-after"}, order)

	// Persist still happened.
	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
}

func TestQueue_UseEnqueueMiddleware_CanModifyJob(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		job.Priority = 123
		return next(ctx, job)
	})

	jobID, err := q.EnqueueRemote(context.Background(), "mw-job", "args")
	require.NoError(t, err)

	job, err := store.GetJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, 123, job.Priority)
}

func TestQueue_UseEnqueueMiddleware_ShortCircuitsOnError(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	sentinel := errors.New("reject")
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		// Do not call next: short-circuit before persist.
		return sentinel
	})

	_, err := q.EnqueueRemote(context.Background(), "mw-job", "args")
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)

	// Nothing should have been persisted.
	assert.Empty(t, store.jobs)
}

// ---------------------------------------------------------------------------
// OnJobStartCtx + CallStartCtxHooks
// ---------------------------------------------------------------------------

type ctxKeyType string

func TestQueue_OnJobStartCtx_ModifiesContext(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	const key ctxKeyType = "trace"
	q.OnJobStartCtx(func(ctx context.Context, job *core.Job) context.Context {
		return context.WithValue(ctx, key, "span-1")
	})

	job := &core.Job{ID: "j1"}
	out := q.CallStartCtxHooks(context.Background(), job)
	assert.Equal(t, "span-1", out.Value(key))
}

func TestQueue_CallStartCtxHooks_NoHooks_ReturnsSameContext(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	ctx := context.Background()
	out := q.CallStartCtxHooks(ctx, &core.Job{ID: "j2"})
	assert.Equal(t, ctx, out)
}

func TestQueue_OnJobStartCtx_ChainsMultipleHooks(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	const k1 ctxKeyType = "a"
	const k2 ctxKeyType = "b"
	q.OnJobStartCtx(func(ctx context.Context, job *core.Job) context.Context {
		return context.WithValue(ctx, k1, 1)
	})
	q.OnJobStartCtx(func(ctx context.Context, job *core.Job) context.Context {
		// Should see value from previous hook.
		require.Equal(t, 1, ctx.Value(k1))
		return context.WithValue(ctx, k2, 2)
	})

	out := q.CallStartCtxHooks(context.Background(), &core.Job{ID: "j3"})
	assert.Equal(t, 1, out.Value(k1))
	assert.Equal(t, 2, out.Value(k2))
}

// ---------------------------------------------------------------------------
// RegisterE — registration options (Timeout) path
// ---------------------------------------------------------------------------

func TestQueue_RegisterE_WithTimeoutOption(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	err := q.RegisterE("timed-handler", func(ctx context.Context, args string) error {
		return nil
	}, Timeout(15*time.Second))
	require.NoError(t, err)

	h, ok := q.GetHandler("timed-handler")
	require.True(t, ok)
	assert.Equal(t, 15*time.Second, h.Timeout)
}

func TestQueue_Register_WithTimeoutOption(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	q.Register("timed-handler2", func(ctx context.Context, args string) error {
		return nil
	}, Timeout(7*time.Second))

	h, ok := q.GetHandler("timed-handler2")
	require.True(t, ok)
	assert.Equal(t, 7*time.Second, h.Timeout)
}

// ---------------------------------------------------------------------------
// Storage-error branches for LoadStatus / ResumeJob / PauseQueue / ResumeQueue
// ---------------------------------------------------------------------------

// getJobErrStorage returns an error from GetJob.
type getJobErrStorage struct {
	*mockStorage
}

func (g *getJobErrStorage) GetJob(ctx context.Context, jobID core.UUID) (*core.Job, error) {
	return nil, errors.New("get failed")
}

func (g *getJobErrStorage) PauseJobWithMode(ctx context.Context, jobID core.UUID, mode core.PauseMode) error {
	return errors.New("get failed")
}

func TestQueue_LoadStatus_StorageError(t *testing.T) {
	store := &getJobErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.LoadStatus(context.Background(), "j")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get failed")
}

func TestQueue_PauseJob_GetJobError(t *testing.T) {
	store := &getJobErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	err := q.PauseJob(context.Background(), "j")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get failed")
}

// unpauseJobErrStorage returns an error from UnpauseJob.
type unpauseJobErrStorage struct {
	*mockStorage
}

func (u *unpauseJobErrStorage) UnpauseJob(ctx context.Context, jobID core.UUID) error {
	return errors.New("unpause failed")
}

func TestQueue_ResumeJob_StorageError(t *testing.T) {
	store := &unpauseJobErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	err := q.ResumeJob(context.Background(), "j")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unpause failed")
}

// pauseQueueErrStorage returns errors from PauseQueue / UnpauseQueue.
type pauseQueueErrStorage struct {
	*mockStorage
}

func (p *pauseQueueErrStorage) PauseQueue(ctx context.Context, queue string) error {
	return errors.New("pause queue failed")
}

func (p *pauseQueueErrStorage) UnpauseQueue(ctx context.Context, queue string) error {
	return errors.New("unpause queue failed")
}

func TestQueue_PauseQueue_StorageError(t *testing.T) {
	store := &pauseQueueErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	err := q.PauseQueue(context.Background(), "valid-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pause queue failed")
}

func TestQueue_ResumeQueue_StorageError(t *testing.T) {
	store := &pauseQueueErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	err := q.ResumeQueue(context.Background(), "valid-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unpause queue failed")
}

// ---------------------------------------------------------------------------
// CancelSubJob error branches
// ---------------------------------------------------------------------------

// cancelSubJobErrStorage returns an error from CancelSubJob.
type cancelSubJobErrStorage struct {
	*mockStorage
}

func (c *cancelSubJobErrStorage) CancelSubJob(ctx context.Context, jobID core.UUID) (*core.FanOut, error) {
	return nil, errors.New("cancel sub-job failed")
}

func TestQueue_CancelSubJob_StorageError(t *testing.T) {
	store := &cancelSubJobErrStorage{mockStorage: newMockStorage()}
	q := New(store)

	_, err := q.CancelSubJob(context.Background(), "sub")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancel sub-job failed")
}

// fanOutUpdateErrStorage returns a complete FanOut then errors on UpdateFanOutStatus.
type fanOutUpdateErrStorage struct {
	*mockStorage
	fo *core.FanOut
}

func (f *fanOutUpdateErrStorage) CancelSubJob(ctx context.Context, jobID core.UUID) (*core.FanOut, error) {
	return f.fo, nil
}

func (f *fanOutUpdateErrStorage) UpdateFanOutStatus(ctx context.Context, fanOutID core.UUID, status core.FanOutStatus) (bool, error) {
	return false, errors.New("update status failed")
}

func TestQueue_CancelSubJob_UpdateFanOutStatusError(t *testing.T) {
	fo := &core.FanOut{
		ID:             "fo",
		ParentJobID:    "parent",
		TotalCount:     2,
		CompletedCount: 2,
		Status:         core.FanOutPending,
	}
	store := &fanOutUpdateErrStorage{mockStorage: newMockStorage(), fo: fo}
	q := New(store)

	result, err := q.CancelSubJob(context.Background(), "sub")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update fan-out status")
	assert.Equal(t, fo, result)
}

// fanOutResumeErrStorage returns a complete FanOut, succeeds on update, errors on ResumeJob.
type fanOutResumeErrStorage struct {
	*mockStorage
	fo *core.FanOut
}

func (f *fanOutResumeErrStorage) CancelSubJob(ctx context.Context, jobID core.UUID) (*core.FanOut, error) {
	return f.fo, nil
}

func (f *fanOutResumeErrStorage) UpdateFanOutStatus(ctx context.Context, fanOutID core.UUID, status core.FanOutStatus) (bool, error) {
	return true, nil
}

func (f *fanOutResumeErrStorage) ResumeJob(ctx context.Context, jobID core.UUID) (bool, error) {
	return false, errors.New("resume parent failed")
}

func TestQueue_CancelSubJob_ResumeParentError(t *testing.T) {
	fo := &core.FanOut{
		ID:             "fo",
		ParentJobID:    "parent",
		TotalCount:     2,
		CompletedCount: 2,
		Status:         core.FanOutPending,
	}
	store := &fanOutResumeErrStorage{mockStorage: newMockStorage(), fo: fo}
	q := New(store)

	result, err := q.CancelSubJob(context.Background(), "sub")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resume parent job")
	assert.Equal(t, fo, result)
}

// ---------------------------------------------------------------------------
// GetScheduledJobs — nil map branch
// ---------------------------------------------------------------------------

func TestQueue_GetScheduledJobs_NilWhenNoneScheduled(t *testing.T) {
	store := newMockStorage()
	q := New(store)

	assert.Nil(t, q.GetScheduledJobs())
}
