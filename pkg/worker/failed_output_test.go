package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorker_FailedOutputSQLite(t *testing.T) {
	const checkout = "/tmp/checkouts/retained-edit"
	cause := errors.New("verification failed")
	for _, tc := range []struct {
		name    string
		err     error
		retries int
		status  core.JobStatus
	}{
		{"exhausted", cause, 0, core.StatusFailed},
		{"no_retry", core.NoRetry(cause), 3, core.StatusFailed},
		{"retry_after", core.RetryAfter(time.Hour, cause), 3, core.StatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, cleanup := newSQLiteQueue(t)
			defer cleanup()
			q.Register("edit", func(context.Context) (map[string]string, error) {
				return map[string]string{"checkout_path": checkout}, tc.err
			})
			var reported error
			q.OnJobFail(func(_ context.Context, _ *core.Job, err error) {
				reported = err
			})
			ctx := context.Background()
			id, err := q.Enqueue(ctx, "edit", nil, queue.Retries(tc.retries))
			require.NoError(t, err)
			w := NewWorker(q)
			job, err := q.Storage().Dequeue(ctx, []string{"default"}, w.config.WorkerID)
			require.NoError(t, err)
			require.NotNil(t, job)

			w.processJob(ctx, job)

			stored, err := q.Storage().GetJob(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, tc.status, stored.Status)
			assert.Contains(t, stored.LastError, cause.Error())
			assert.Contains(t, stored.LastError, checkout)
			assert.Empty(t, stored.Result, "failed output must not become a successful result")
			if tc.status == core.StatusFailed {
				require.ErrorIs(t, reported, cause)
				assert.Contains(t, reported.Error(), checkout)
			} else {
				require.NotNil(t, stored.RunAt)
				assert.WithinDuration(t, time.Now().Add(time.Hour), *stored.RunAt, time.Minute)
			}
		})
	}
}
