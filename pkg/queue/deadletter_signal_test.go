package queue

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Covers the Queue dead-letter triage API, Requeue/Signal facades, and the
// waiting-hook registry — all previously at 0%. The storage-layer SQL for
// ListDeadLettered/Requeue/SendSignal is tested in pkg/storage; here we pin the
// facade contract: capability detection, filter assembly, delegation, and the
// emitted events.

// dlqMock augments the queue mockStorage with the optional capability
// interfaces the dead-letter / requeue / signal facades probe for, recording
// what they were called with.
type dlqMock struct {
	*mockStorage
	gotFilter    core.DeadLetterFilter
	listJobs     []*core.Job
	count        int64
	requeued     bool
	requeueErr   error
	sentSignal   bool
	sentName     string
	resumeCalled bool
}

func (d *dlqMock) ListDeadLettered(ctx context.Context, filter core.DeadLetterFilter) ([]*core.Job, error) {
	d.gotFilter = filter
	return d.listJobs, nil
}

func (d *dlqMock) CountDeadLettered(ctx context.Context, filter core.DeadLetterFilter) (int64, error) {
	d.gotFilter = filter
	return d.count, nil
}

func (d *dlqMock) Requeue(ctx context.Context, jobID core.UUID) (bool, error) {
	return d.requeued, d.requeueErr
}

func (d *dlqMock) SendSignal(ctx context.Context, jobID core.UUID, name string, payload []byte) error {
	d.sentSignal = true
	d.sentName = name
	return nil
}

func (d *dlqMock) ResumeSignalWaitingJob(ctx context.Context, jobID core.UUID) (bool, error) {
	d.resumeCalled = true
	return true, nil
}

func newDLQMock() *dlqMock { return &dlqMock{mockStorage: newMockStorage()} }

// ──────────────────────────────────────────────────────────────────────────────
// newDeadLetterFilter + option builders
// ──────────────────────────────────────────────────────────────────────────────

func TestNewDeadLetterFilter_AppliesAllOptions(t *testing.T) {
	f := newDeadLetterFilter(
		DeadLetterQueue("emails"),
		DeadLetterType("welcome"),
		DeadLetterLimit(25),
		DeadLetterOffset(50),
	)
	assert.Equal(t, "emails", f.Queue)
	assert.Equal(t, "welcome", f.Type)
	assert.Equal(t, 25, f.Limit)
	assert.Equal(t, 50, f.Offset)
}

func TestNewDeadLetterFilter_ZeroValueWhenNoOptions(t *testing.T) {
	f := newDeadLetterFilter()
	assert.Equal(t, core.DeadLetterFilter{}, f)
}

// ──────────────────────────────────────────────────────────────────────────────
// ListDeadLettered / CountDeadLettered
// ──────────────────────────────────────────────────────────────────────────────

func TestQueue_ListDeadLettered_DelegatesWithFilter(t *testing.T) {
	store := newDLQMock()
	store.listJobs = []*core.Job{{ID: "dead-1"}}
	q := New(store)

	jobs, err := q.ListDeadLettered(context.Background(), DeadLetterQueue("emails"), DeadLetterLimit(10))
	require.NoError(t, err)
	assert.Equal(t, store.listJobs, jobs)
	assert.Equal(t, "emails", store.gotFilter.Queue, "options must reach the storage filter")
	assert.Equal(t, 10, store.gotFilter.Limit)
}

func TestQueue_CountDeadLettered_DelegatesWithFilter(t *testing.T) {
	store := newDLQMock()
	store.count = 7
	q := New(store)

	n, err := q.CountDeadLettered(context.Background(), DeadLetterType("welcome"))
	require.NoError(t, err)
	assert.Equal(t, int64(7), n)
	assert.Equal(t, "welcome", store.gotFilter.Type)
}

func TestQueue_ListDeadLettered_UnsupportedBackendErrors(t *testing.T) {
	// Plain mockStorage does not implement the dead-letter interface.
	q := New(newMockStorage())
	_, err := q.ListDeadLettered(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support dead-letter")
}

func TestQueue_CountDeadLettered_UnsupportedBackendErrors(t *testing.T) {
	q := New(newMockStorage())
	_, err := q.CountDeadLettered(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support dead-letter")
}

// ──────────────────────────────────────────────────────────────────────────────
// Requeue
// ──────────────────────────────────────────────────────────────────────────────

func TestQueue_Requeue_DelegatesResult(t *testing.T) {
	store := newDLQMock()
	store.requeued = true
	q := New(store)

	ok, err := q.Requeue(context.Background(), core.UUID("j"))
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestQueue_Requeue_PropagatesError(t *testing.T) {
	store := newDLQMock()
	store.requeueErr = errors.New("cannot requeue")
	q := New(store)

	_, err := q.Requeue(context.Background(), core.UUID("j"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot requeue")
}

func TestQueue_Requeue_UnsupportedBackendErrors(t *testing.T) {
	q := New(newMockStorage())
	_, err := q.Requeue(context.Background(), core.UUID("j"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support Requeue")
}

// ──────────────────────────────────────────────────────────────────────────────
// Signal — validation, capability, delivery
// ──────────────────────────────────────────────────────────────────────────────

func TestQueue_Signal_RejectsInvalidNames(t *testing.T) {
	q := New(newDLQMock())
	ctx := context.Background()

	err := q.Signal(ctx, core.UUID("j"), "", nil)
	require.Error(t, err, "empty name")
	assert.Contains(t, err.Error(), "must not be empty")

	err = q.Signal(ctx, core.UUID("j"), "_reserved", nil)
	require.ErrorIs(t, err, signal.ErrSignalNameReserved, "underscore-prefixed names are reserved")

	err = q.Signal(ctx, core.UUID("j"), strings.Repeat("x", security.MaxSignalNameLength+1), nil)
	require.ErrorIs(t, err, core.ErrSignalNameTooLong)
}

func TestQueue_Signal_UnsupportedBackendReturnsErrStorageNoSignals(t *testing.T) {
	// Plain mockStorage has no SendSignal.
	q := New(newMockStorage())
	err := q.Signal(context.Background(), core.UUID("j"), "ping", nil)
	require.ErrorIs(t, err, core.ErrStorageNoSignals)
}

func TestQueue_Signal_JobNotFound(t *testing.T) {
	store := newDLQMock() // SendSignal exists, but the job is not in storage
	q := New(store)

	err := q.Signal(context.Background(), core.UUID("missing"), "ping", nil)
	require.ErrorIs(t, err, core.ErrJobNotFound)
	assert.False(t, store.sentSignal, "no signal sent when the job does not exist")
}

func TestQueue_Signal_DeliversAndEmitsEvent(t *testing.T) {
	store := newDLQMock()
	q := New(store)
	ctx := context.Background()

	// Seed a non-waiting job so the fast-path resume is not taken.
	store.jobs["j"] = &core.Job{ID: "j", Type: "t", Queue: "default", Status: core.StatusRunning}

	events := q.Events()
	defer q.Unsubscribe(events)

	require.NoError(t, q.Signal(ctx, core.UUID("j"), "ping", map[string]string{"k": "v"}))
	assert.True(t, store.sentSignal, "signal delivered to storage")
	assert.Equal(t, "ping", store.sentName)
	assert.False(t, store.resumeCalled, "a running job is not signal-resumed")

	select {
	case ev := <-events:
		d, ok := ev.(*core.SignalDelivered)
		require.True(t, ok, "expected *core.SignalDelivered, got %T", ev)
		assert.Equal(t, "ping", d.Name)
	default:
		t.Fatal("expected a SignalDelivered event")
	}
}

func TestQueue_Signal_WaitingJobTakesResumeFastPath(t *testing.T) {
	store := newDLQMock()
	q := New(store)
	ctx := context.Background()

	// A waiting job triggers the immediate signal-resume fast path.
	store.jobs["w"] = &core.Job{ID: "w", Type: "t", Queue: "default", Status: core.StatusWaiting}

	require.NoError(t, q.Signal(ctx, core.UUID("w"), "ping", nil))
	assert.True(t, store.sentSignal)
	assert.True(t, store.resumeCalled, "a waiting job is resumed immediately on signal")
}

// ──────────────────────────────────────────────────────────────────────────────
// OnJobWaiting / CallWaitingHooks
// ──────────────────────────────────────────────────────────────────────────────

func TestCallWaitingHooks_NoHooksIsSafeNoop(t *testing.T) {
	q := New(newMockStorage())
	q.CallWaitingHooks(context.Background(), &core.Job{ID: "j"}) // must not panic
}

func TestOnJobWaiting_HooksInvokedInRegistrationOrder(t *testing.T) {
	q := New(newMockStorage())
	job := &core.Job{ID: "j"}

	var order []string
	q.OnJobWaiting(func(ctx context.Context, j *core.Job) { order = append(order, "first:"+string(j.ID)) })
	q.OnJobWaiting(func(ctx context.Context, j *core.Job) { order = append(order, "second") })

	q.CallWaitingHooks(context.Background(), job)
	assert.Equal(t, []string{"first:j", "second"}, order)
}
