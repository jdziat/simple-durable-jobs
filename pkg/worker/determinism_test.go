package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// TestWorker_StrictDeterminism_FailsOnDroppedCall verifies that a job running in
// Strict determinism mode fails terminally when its replay does not reach every
// recorded Call checkpoint (the handler "dropped" calls that ran before). The
// default ExplicitCheckpoints mode tolerates this.
func TestWorker_StrictDeterminism_FailsOnDroppedCall(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      queue.DeterminismMode
		expectErr bool
	}{
		{"strict fails", queue.Strict, true},
		{"explicit tolerates", queue.ExplicitCheckpoints, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockStorage{
				checkpointFunc: func(_ context.Context, _ string) ([]core.Checkpoint, error) {
					// Two Call checkpoints recorded by a prior run.
					return []core.Checkpoint{
						{JobID: "j1", CallIndex: 0, CallType: "step-a", Result: []byte(`"ok"`)},
						{JobID: "j1", CallIndex: 1, CallType: "step-b", Result: []byte(`"ok"`)},
					}, nil
				},
				failFunc: func(_ context.Context, _, _, _ string, _ *time.Time) error { return nil },
			}
			q := queue.New(mock)

			done := make(chan error, 1)
			// Handler makes ZERO Calls — it dropped the two recorded calls.
			q.Register("strict-job", func(_ context.Context, _ struct{}) error { return nil })
			q.OnJobComplete(func(_ context.Context, _ *core.Job) { done <- nil })
			q.OnJobFail(func(_ context.Context, _ *core.Job, err error) { done <- err })

			mock.dequeueFunc = func(_ context.Context, _ []string, _ string) (*core.Job, error) {
				mock.dequeueFunc = nil
				return &core.Job{
					ID:          "j1",
					Type:        "strict-job",
					Queue:       "default",
					Args:        []byte(`{}`),
					MaxRetries:  3,
					Determinism: int(tc.mode),
				}, nil
			}

			w := NewWorker(q, WithStaleLockInterval(0))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			go func() { _ = w.Start(ctx) }()

			select {
			case err := <-done:
				if tc.expectErr {
					require.Error(t, err, "strict mode must fail on a dropped call")
					assert.Contains(t, err.Error(), "strict determinism")
				} else {
					require.NoError(t, err, "explicit mode tolerates unconsumed checkpoints")
				}
			case <-time.After(1500 * time.Millisecond):
				t.Fatal("job did not reach a terminal state")
			}
		})
	}
}

// TestWithMaxRetryBackoff covers the option and the calculateBackoff cap.
func TestWithMaxRetryBackoff(t *testing.T) {
	var c WorkerConfig
	WithMaxRetryBackoff(5 * time.Minute).ApplyWorker(&c)
	assert.Equal(t, 5*time.Minute, c.MaxRetryBackoff)

	// Non-positive is ignored (keeps the prior value).
	WithMaxRetryBackoff(0).ApplyWorker(&c)
	WithMaxRetryBackoff(-time.Second).ApplyWorker(&c)
	assert.Equal(t, 5*time.Minute, c.MaxRetryBackoff)

	// calculateBackoff honors the configured cap.
	w := &Worker{config: WorkerConfig{MaxRetryBackoff: 5 * time.Minute}}
	assert.Equal(t, 5*time.Minute, w.calculateBackoff(30), "large attempt clamps to the cap")
	assert.Equal(t, time.Second, w.calculateBackoff(0), "first retry is 1s")
}
