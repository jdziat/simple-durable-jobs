package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/codec"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

func TestIdempotencyKeyDedupesAndExpires(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("charge", func(context.Context, string) error { return nil })

	first, err := q.Enqueue(ctx, "charge", "pay_123", IdempotencyKey("request-1", time.Hour))
	require.NoError(t, err)
	second, err := q.Enqueue(ctx, "charge", "pay_456", IdempotencyKey("request-1", time.Hour))
	require.NoError(t, err)
	assert.Equal(t, first, second)

	var count int64
	require.NoError(t, db.Model(&core.Job{}).Count(&count).Error)
	assert.EqualValues(t, 1, count)

	require.NoError(t, db.Model(&core.UniqueLock{}).Where("job_id = ?", first).
		Update("expires_at", time.Now().Add(-time.Hour).UTC()).Error)

	third, err := q.Enqueue(ctx, "charge", "pay_789", IdempotencyKey("request-1", time.Hour))
	require.NoError(t, err)
	assert.NotEqual(t, first, third)
	require.NoError(t, db.Model(&core.Job{}).Count(&count).Error)
	assert.EqualValues(t, 2, count)
}

func TestUniqueForHashesPlaintextBeforeSecretboxCodec(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	box, err := codec.NewSecretbox(key)
	require.NoError(t, err)

	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t, storage.WithCodec(box))
	q := New(store)
	q.Register("sync", func(context.Context, map[string]any) error { return nil })

	args := map[string]any{"account": "acct_1", "force": true}
	first, err := q.Enqueue(ctx, "sync", args, UniqueFor(time.Hour))
	require.NoError(t, err)
	second, err := q.Enqueue(ctx, "sync", map[string]any{"force": true, "account": "acct_1"}, UniqueFor(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, first, second)

	var jobs []core.Job
	require.NoError(t, db.Find(&jobs).Error)
	require.Len(t, jobs, 1)
	assert.NotEqual(t, []byte(`{"account":"acct_1","force":true}`), jobs[0].Args, "secretbox should store randomized ciphertext")
}

func TestWindowedDedupInvalidOptionsFailLoudly(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("sync", func(context.Context, string) error { return nil })

	for _, tc := range []struct {
		name string
		opt  Option
	}{
		{name: "empty idempotency key", opt: IdempotencyKey("", time.Hour)},
		{name: "zero idempotency ttl", opt: IdempotencyKey("request-1", 0)},
		{name: "zero unique-for ttl", opt: UniqueFor(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := q.Enqueue(ctx, "sync", "payload", tc.opt)
			require.ErrorIs(t, err, core.ErrInvalidWindowedDedup)
			require.True(t, errors.Is(err, core.ErrInvalidWindowedDedup))
		})
	}

	var count int64
	require.NoError(t, db.Model(&core.Job{}).Count(&count).Error)
	assert.EqualValues(t, 0, count)
}

func TestWindowedDedupValidOptionsStillDedup(t *testing.T) {
	ctx := context.Background()
	db, store := newQueueIdempotencyStore(t)
	q := New(store)
	q.Register("sync", func(context.Context, string) error { return nil })

	first, err := q.Enqueue(ctx, "sync", "payload-a", IdempotencyKey("request-1", time.Hour))
	require.NoError(t, err)
	second, err := q.Enqueue(ctx, "sync", "payload-b", IdempotencyKey("request-1", time.Hour))
	require.NoError(t, err)
	assert.Equal(t, first, second)

	third, err := q.Enqueue(ctx, "sync", "same-payload", UniqueFor(time.Hour))
	require.NoError(t, err)
	fourth, err := q.Enqueue(ctx, "sync", "same-payload", UniqueFor(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, third, fourth)

	var count int64
	require.NoError(t, db.Model(&core.Job{}).Count(&count).Error)
	assert.EqualValues(t, 2, count)
}

func newQueueIdempotencyStore(t *testing.T, opts ...storage.GormStorageOption) (*gorm.DB, *storage.GormStorage) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db, opts...)
	require.NoError(t, store.Migrate(context.Background()))
	return db, store
}
