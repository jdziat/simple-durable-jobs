package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

func TestExponentialBackoff_NoJitterMatchesLegacyCalculateBackoff(t *testing.T) {
	w := &Worker{config: WorkerConfig{MaxRetryBackoff: time.Minute}}
	policy, ok := DefaultBackoffPolicy().(ExponentialBackoff)
	require.True(t, ok)
	policy.JitterFraction = 0

	for _, attempt := range []int{-1, 0, 1, 2, 3, 4, 5, 6, 10, 30, 99} {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			assert.Equal(t, w.calculateBackoff(attempt), policy.NextRetry(attempt, errors.New("boom")))
		})
	}
}

func TestDefaultBackoffPolicy_JitterBounds(t *testing.T) {
	legacy := &Worker{config: WorkerConfig{MaxRetryBackoff: time.Minute}}
	policy := DefaultBackoffPolicy()

	for attempt := 0; attempt <= 10; attempt++ {
		base := legacy.calculateBackoff(attempt)
		minDelay := time.Duration(float64(base) * 0.9)
		maxDelay := time.Duration(float64(base) * 1.1)

		for sample := 0; sample < 100; sample++ {
			delay := policy.NextRetry(attempt, errors.New("boom"))
			assert.GreaterOrEqual(t, delay, time.Duration(0))
			assert.GreaterOrEqual(t, delay, minDelay)
			assert.LessOrEqual(t, delay, maxDelay)
		}
	}
}

func TestWithBackoffStoresWorkerPolicy(t *testing.T) {
	var c WorkerConfig
	policy := BackoffFunc(func(int, error) time.Duration { return 42 * time.Second })

	WithBackoff(policy).ApplyWorker(&c)

	require.NotNil(t, c.JobBackoff)
	assert.Equal(t, 42*time.Second, c.JobBackoff.NextRetry(0, errors.New("boom")))
}

func TestWithHandlerBackoffStoresRegistrationPolicy(t *testing.T) {
	q := queue.New(&mockStorage{})
	policy := BackoffFunc(func(int, error) time.Duration { return 7 * time.Second })

	require.NoError(t, q.RegisterE("custom", func(context.Context, struct{}) error {
		return nil
	}, queue.WithHandlerBackoff(policy)))

	h, ok := q.GetHandler("custom")
	require.True(t, ok)
	require.NotNil(t, h.Backoff)
	assert.Equal(t, 7*time.Second, h.Backoff.NextRetry(0, errors.New("boom")))
}

func TestHandlerBackoffOverridesWorkerBackoff(t *testing.T) {
	q := queue.New(&mockStorage{})

	shortPolicy := ExponentialBackoff{
		InitialInterval: 20 * time.Second,
		Multiplier:      2,
		MaxInterval:     time.Hour,
		JitterFraction:  0,
	}
	longPolicy := ExponentialBackoff{
		InitialInterval: 40 * time.Second,
		Multiplier:      2,
		MaxInterval:     time.Hour,
		JitterFraction:  0,
	}
	q.Register("short", func(context.Context, struct{}) error {
		return nil
	}, queue.WithHandlerBackoff(shortPolicy))
	q.Register("long", func(context.Context, struct{}) error {
		return nil
	}, queue.WithHandlerBackoff(longPolicy))

	w := NewWorker(q,
		WithBackoff(BackoffFunc(func(int, error) time.Duration { return time.Minute })),
		WithMaxRetryBackoff(time.Hour),
	)

	err := errors.New("boom")
	shortDelay := w.retryBackoff(&core.Job{Type: "short", Attempt: 0}, err)
	longDelay := w.retryBackoff(&core.Job{Type: "long", Attempt: 0}, err)

	assert.Greater(t, longDelay, shortDelay)
	assert.Equal(t, shortPolicy.NextRetry(0, err), shortDelay)
	assert.Equal(t, longPolicy.NextRetry(0, err), longDelay)
}

func TestRetryBackoffClampsPolicyDelay(t *testing.T) {
	q := queue.New(&mockStorage{})
	w := NewWorker(q,
		WithBackoff(BackoffFunc(func(int, error) time.Duration { return 10 * time.Minute })),
		WithMaxRetryBackoff(5*time.Second),
	)

	delay := w.retryBackoff(&core.Job{Type: "missing"}, errors.New("boom"))

	assert.Equal(t, 5*time.Second, delay)
}

func TestPanickingBackoffPolicyBecomesATerminalFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		workerOpts []WorkerOption
		handlerOpt queue.Option
	}{
		{
			name: "worker policy",
			workerOpts: []WorkerOption{WithBackoff(BackoffFunc(func(int, error) time.Duration {
				panic("worker backoff boom")
			}))},
		},
		{
			name: "handler policy",
			handlerOpt: queue.WithHandlerBackoff(BackoffFunc(func(int, error) time.Duration {
				panic("handler backoff boom")
			})),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var failCalls atomic.Int32
			var retryAt *time.Time
			var storedError string
			store := &mockStorage{
				failFunc: func(_ context.Context, _ core.UUID, _ string, errMsg string, at *time.Time) error {
					failCalls.Add(1)
					storedError = errMsg
					retryAt = at
					return nil
				},
			}
			q := queue.New(store)
			registerOpts := []queue.Option{}
			if tc.handlerOpt != nil {
				registerOpts = append(registerOpts, tc.handlerOpt)
			}
			require.NoError(t, q.RegisterE("backoff-panic", func(context.Context, struct{}) error {
				return errors.New("handler failed")
			}, registerOpts...))

			var retries, failures atomic.Int32
			q.OnRetry(func(context.Context, *core.Job, int, error) { retries.Add(1) })
			q.OnJobFail(func(context.Context, *core.Job, error) { failures.Add(1) })

			w := NewWorker(q, append(tc.workerOpts, DisableRetry())...)
			var logs bytes.Buffer
			w.logger = slog.New(slog.NewTextHandler(&logs, nil))

			require.NotPanics(t, func() {
				w.processJob(context.Background(), &core.Job{
					ID: "backoff-panic-job", Type: "backoff-panic", Queue: "default",
					Args: []byte(`{}`), Attempt: 1, MaxRetries: 3,
				})
			})

			assert.Equal(t, int32(1), failCalls.Load(), "the panic must produce one durable disposition")
			assert.Nil(t, retryAt, "a broken retry policy cannot safely schedule a retry")
			assert.Contains(t, storedError, "backoff policy panicked")
			assert.Equal(t, int32(0), retries.Load())
			assert.Equal(t, int32(1), failures.Load())
			assert.True(t, strings.Contains(logs.String(), "backoff policy panicked"))
			assert.NotContains(t, logs.String(), "recovered panic in processJob",
				"a user callback panic must not be misreported as a library-internal panic")
		})
	}
}
