package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// On graceful shutdown, a handler cancelled by the worker stopping must be
// RELEASED back to pending (lock cleared, attempt refunded) rather than
// failed/retried — so a rolling deploy hands work off cleanly. With
// DisableRetry() the distinction is crisp: the old fail path would terminally
// fail the cancelled job; the release path leaves it pending and runnable.
func TestWorker_GracefulShutdownReleasesRunningJob(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	started := make(chan struct{})
	var once sync.Once
	q.Register("blocked", func(ctx context.Context, _ struct{}) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})

	jobID, err := q.Enqueue(context.Background(), "blocked", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q,
		WithPollInterval(40*time.Millisecond),
		WithDrainTimeout(60*time.Millisecond),
		WithOwnershipAuditInterval(0),
		DisableRetry(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	startReturned := make(chan error, 1)
	go func() { startReturned <- w.Start(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	cancel() // begin graceful shutdown → drain timeout → cancel handler
	select {
	case <-startReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return")
	}

	job, err := q.Storage().GetJob(context.Background(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, core.StatusPending, job.Status, "shutdown-cancelled job is released to pending, not failed")
	assert.Empty(t, job.LockedBy, "lock cleared on release")
	assert.Equal(t, 0, job.Attempt, "attempt refunded on graceful release")
}
