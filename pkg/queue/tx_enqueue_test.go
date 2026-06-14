package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func TestQueue_EnqueueTx_NoCapabilityReturnsSentinel(t *testing.T) {
	q := New(newMockStorage())
	_, err := q.EnqueueTx(context.Background(), nil, "remote.job", "payload")
	require.ErrorIs(t, err, core.ErrStorageNoTxEnqueue)
}

func TestQueue_EnqueueTx_DoesNotRequireRegisteredHandlerAndUsesMiddleware(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	var calls atomic.Int32
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		calls.Add(1)
		job.Priority = 42
		return next(ctx, job)
	})

	tx := store.DB().Begin()
	require.NoError(t, tx.Error)
	id, err := q.EnqueueTx(ctx, tx, "remote.tx", map[string]string{"v": "1"})
	require.NoError(t, err)
	assert.False(t, q.HasHandler("remote.tx"))

	before, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, before)

	require.NoError(t, tx.Commit().Error)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, 42, job.Priority)
	assert.EqualValues(t, 1, calls.Load())
}

func TestQueue_EnqueueTx_ValidatesJobTypeName(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	tx := store.DB().Begin()
	require.NoError(t, tx.Error)
	_, err := q.EnqueueTx(ctx, tx, "has space", "payload")
	require.Error(t, err)
	require.NoError(t, tx.Rollback().Error)

	tx = store.DB().Begin()
	require.NoError(t, tx.Error)
	id, err := q.EnqueueTx(ctx, tx, "remote.valid", "payload")
	require.NoError(t, err)
	require.NoError(t, tx.Commit().Error)

	job, err := store.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "remote.valid", job.Type)
}

func TestQueue_EnqueueTx_UniqueDuplicateReturnsSentinel(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	tx1 := store.DB().Begin()
	require.NoError(t, tx1.Error)
	_, err := q.EnqueueTx(ctx, tx1, "remote.unique", "first", Unique("queue-tx-key"))
	require.NoError(t, err)
	require.NoError(t, tx1.Commit().Error)

	tx2 := store.DB().Begin()
	require.NoError(t, tx2.Error)
	_, err = q.EnqueueTx(ctx, tx2, "remote.unique", "second", Unique("queue-tx-key"))
	require.ErrorIs(t, err, core.ErrDuplicateJob)
	require.True(t, errors.Is(err, core.ErrDuplicateJob))
	require.NoError(t, tx2.Rollback().Error)
}

func TestQueue_EnqueueBatchTx_DedupesAndRunsMiddlewarePerEntry(t *testing.T) {
	ctx := context.Background()
	store := newQueueBatchTestStorage(t)
	q := New(store)

	var calls atomic.Int32
	q.UseEnqueueMiddleware(func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error {
		calls.Add(1)
		return next(ctx, job)
	})

	tx1 := store.DB().Begin()
	require.NoError(t, tx1.Error)
	_, err := q.EnqueueTx(ctx, tx1, "remote.seed", "seed", Unique("batch-existing"))
	require.NoError(t, err)
	require.NoError(t, tx1.Commit().Error)
	calls.Store(0)

	tx2 := store.DB().Begin()
	require.NoError(t, tx2.Error)
	ids, err := q.EnqueueBatchTx(ctx, tx2, []BatchEntry{
		Batch("batch.skip", "existing", Unique("batch-existing")),
		Batch("batch.new", "first", Unique("batch-new")),
		Batch("batch.new", "second", Unique("batch-new")),
	})
	require.NoError(t, err)
	require.Len(t, ids, 3)
	assert.Equal(t, ids[1], ids[2])
	assert.EqualValues(t, 3, calls.Load())
	require.NoError(t, tx2.Commit().Error)

	var count int64
	require.NoError(t, store.DB().Model(&core.Job{}).Where("unique_key IN ?", []string{"batch-existing", "batch-new"}).Count(&count).Error)
	assert.EqualValues(t, 2, count)
}
