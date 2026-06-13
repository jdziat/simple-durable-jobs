package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
)

type capabilityMockStorage struct {
	*mockStorage
}

func (s *capabilityMockStorage) TryAcquireConcurrencySlot(context.Context, string, string, string, int, time.Duration) (bool, error) {
	return true, nil
}

func (s *capabilityMockStorage) ReleaseConcurrencySlot(context.Context, string, string) error {
	return nil
}

func (s *capabilityMockStorage) TryConsumeRate(context.Context, string, float64, time.Duration, time.Time) (bool, error) {
	return true, nil
}

func TestNewWorkerTopLevelConcurrencyAppliesToDefaultQueue(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), Concurrency(50))

	require.NotNil(t, w.config.Queues)
	assert.Equal(t, 50, w.config.Queues["default"])
}

func TestNewWorkerWorkerQueueConcurrencyStillAppliesToNamedQueue(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), WorkerQueue("default", Concurrency(50)))

	require.NotNil(t, w.config.Queues)
	assert.Equal(t, 50, w.config.Queues["default"])
}

func TestNewWorkerBareUsesDefaultQueueConcurrency(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}))

	require.NotNil(t, w.config.Queues)
	assert.Equal(t, 10, w.config.Queues["default"])
}

func TestWorkerStartFailsWhenConcurrencyCapStorageCapabilityMissing(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), ConcurrencyCap("tenant", 1))

	err := w.Start(context.Background())

	require.Error(t, err)
	assert.ErrorContains(t, err, "worker has 1 ConcurrencyCap(s) configured")
	assert.ErrorContains(t, err, "does not support DB-backed concurrency slots")
}

func TestWorkerStartFailsWhenRateLimitStorageCapabilityMissing(t *testing.T) {
	w := NewWorker(queue.New(&mockStorage{}), RateLimit("llm", 1))

	err := w.Start(context.Background())

	require.Error(t, err)
	assert.ErrorContains(t, err, "worker has 1 RateLimit(s) configured")
	assert.ErrorContains(t, err, "does not support DB-backed rate limiting")
}

func TestWorkerStartAllowsCapableStorageForFleetLimiters(t *testing.T) {
	store := &capabilityMockStorage{mockStorage: &mockStorage{}}
	w := NewWorker(queue.New(store),
		ConcurrencyCap("tenant", 1),
		RateLimit("llm", 1),
		WithOwnershipAuditInterval(0),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.Start(ctx)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context cancellation, got %v", err)
	assert.NotContains(t, err.Error(), "does not support DB-backed")
}

var _ core.Storage = (*mockStorage)(nil)
var _ concurrencySlotStorage = (*capabilityMockStorage)(nil)
var _ rateLimiterStorage = (*capabilityMockStorage)(nil)
