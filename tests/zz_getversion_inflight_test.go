package jobs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The documented Branch Pattern, exercised end to end through the public API on
// a run that was ALREADY IN FLIGHT when the branch was deployed — the population
// docs/content/docs/advanced/workflow-versioning.md exists to protect.
//
// Before the replay signal existed, GetVersion met an unrecorded changeID and
// took its first-execution path, so the replay was handed maxSupported, issued
// the NEW branch's Call at index 0 against a checkpoint holding the OLD call's
// type, and dead-lettered on a determinism violation on every attempt:
//
//	attempts=5 status=failed
//	last_error="jobs.Call determinism violation at index 0: checkpoint type
//	           \"quote-shipping\" does not match requested call \"quote-shipping-v2\""
//
// The `case jobs.DefaultVersion:` arm was unreachable for exactly the runs it
// was written for.

type legacyQuote struct{ Cents int }

type quoteV2 struct {
	Cents    int
	Currency string
}

// registerVersionedWorkflow registers the docs' Branch Pattern verbatim.
// deployedV2 flips from "the old code, which had no marker" to "the new deploy".
func registerVersionedWorkflow(q *jobs.Queue, deployedV2 *atomic.Bool,
	legacyRuns, v2Runs, receiptRuns *atomic.Int32, observed *[]int, mu *sync.Mutex,
) {
	q.Register("quote-shipping", func(_ context.Context, orderID string) (legacyQuote, error) {
		legacyRuns.Add(1)
		return legacyQuote{Cents: 500}, nil
	})
	q.Register("quote-shipping-v2", func(_ context.Context, orderID string) (quoteV2, error) {
		v2Runs.Add(1)
		return quoteV2{Cents: 700, Currency: "USD"}, nil
	})
	q.Register("send-receipt", func(_ context.Context, orderID string) (string, error) {
		receiptRuns.Add(1)
		return "receipt:" + orderID, nil
	})

	q.Register("process-order", func(ctx context.Context, orderID string) (string, error) {
		if !deployedV2.Load() {
			// The OLD code: no marker, one legacy quote, then an ordinary
			// retryable failure — the scenario the page opens with.
			if _, err := jobs.Call[legacyQuote](ctx, "quote-shipping", orderID); err != nil {
				return "", err
			}
			return "", errors.New("worker died before the receipt step")
		}

		// --- the shipped Branch Pattern, copied verbatim from the docs ---
		version, err := jobs.GetVersion(ctx, "shipping-v2", jobs.DefaultVersion, 1)
		if err != nil {
			if errors.Is(err, jobs.ErrUnsupportedWorkflowVersion) {
				return "", jobs.NoRetry(err)
			}
			return "", err
		}
		mu.Lock()
		*observed = append(*observed, version)
		mu.Unlock()

		switch version {
		case jobs.DefaultVersion:
			_, err = jobs.Call[legacyQuote](ctx, "quote-shipping", orderID)
		case 1:
			_, err = jobs.Call[quoteV2](ctx, "quote-shipping-v2", orderID)
		}
		if err != nil {
			return "", err
		}

		return jobs.Call[string](ctx, "send-receipt", orderID)
	})
}

// TestGetVersion_InFlightRunTakesTheDefaultBranch is the headline guard. It
// fails on the parent commit with status=failed and a determinism violation.
func TestGetVersion_InFlightRunTakesTheDefaultBranch(t *testing.T) {
	q, _ := openIntegrationQueue(t)

	var deployedV2 atomic.Bool
	var legacyRuns, v2Runs, receiptRuns atomic.Int32
	var observed []int
	var mu sync.Mutex
	registerVersionedWorkflow(q, &deployedV2, &legacyRuns, &v2Runs, &receiptRuns, &observed, &mu)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "process-order", "order-1", jobs.Retries(4))
	require.NoError(t, err)

	// Attempt 1 runs the old code: it checkpoints Call "quote-shipping" at index
	// 0 and then fails retryably. The deploy lands before the retry.
	runWorkerUntilFirstFailure(t, q, id)
	deployedV2.Store(true)

	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	t.Logf("status=%s last_error=%q attempt=%d observed=%v legacy=%d v2=%d receipt=%d",
		job.Status, job.LastError, job.Attempt, observed, legacyRuns.Load(), v2Runs.Load(), receiptRuns.Load())

	assert.Equal(t, core.StatusCompleted, job.Status,
		"an in-flight run must keep its originally recorded path, not dead-letter on a determinism violation")

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, observed, "the replay never reached GetVersion")
	for _, v := range observed {
		assert.Equal(t, jobs.DefaultVersion, v, "every replay of an in-flight run must observe DefaultVersion")
	}
	assert.Zero(t, v2Runs.Load(), "the new branch must never run for a run pinned to the old one")
	assert.Equal(t, int32(1), receiptRuns.Load(), "the workflow must finish its receipt step exactly once")
}

// TestGetVersion_FreshRunTakesTheNewBranch is the healthy-path guard: a job
// enqueued AFTER the deploy has no prior durable step to be pinned by, so it
// must take the new branch. Without it, a replay signal that over-fires would
// silently freeze every new run on the legacy path.
func TestGetVersion_FreshRunTakesTheNewBranch(t *testing.T) {
	q, _ := openIntegrationQueue(t)

	var deployedV2 atomic.Bool
	deployedV2.Store(true)
	var legacyRuns, v2Runs, receiptRuns atomic.Int32
	var observed []int
	var mu sync.Mutex
	registerVersionedWorkflow(q, &deployedV2, &legacyRuns, &v2Runs, &receiptRuns, &observed, &mu)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "process-order", "order-2", jobs.Retries(4))
	require.NoError(t, err)
	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	t.Logf("status=%s observed=%v legacy=%d v2=%d", job.Status, observed, legacyRuns.Load(), v2Runs.Load())

	assert.Equal(t, core.StatusCompleted, job.Status)
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, observed)
	for _, v := range observed {
		assert.Equal(t, 1, v, "a job enqueued after the deploy must record maxSupported")
	}
	assert.Equal(t, int32(1), v2Runs.Load(), "the new branch must run for a fresh job")
	assert.Zero(t, legacyRuns.Load())
}

// TestGetVersion_InFlightRunFailsLoudWhenOldBranchRemoved pins the second-deploy
// step the page describes: once minSupported is raised, a run still pinned to
// DefaultVersion must surface ErrUnsupportedWorkflowVersion rather than silently
// take a branch its checkpoints cannot support.
func TestGetVersion_InFlightRunFailsLoudWhenOldBranchRemoved(t *testing.T) {
	q, _ := openIntegrationQueue(t)

	var deployedV2 atomic.Bool
	var legacyRuns, v2Runs atomic.Int32

	q.Register("quote-shipping", func(_ context.Context, orderID string) (legacyQuote, error) {
		legacyRuns.Add(1)
		return legacyQuote{Cents: 500}, nil
	})
	q.Register("quote-shipping-v2", func(_ context.Context, orderID string) (quoteV2, error) {
		v2Runs.Add(1)
		return quoteV2{Cents: 700, Currency: "USD"}, nil
	})
	q.Register("process-order", func(ctx context.Context, orderID string) (string, error) {
		if !deployedV2.Load() {
			if _, err := jobs.Call[legacyQuote](ctx, "quote-shipping", orderID); err != nil {
				return "", err
			}
			return "", errors.New("worker died before the receipt step")
		}
		// The SECOND deploy: the old branch is gone and minSupported is 1.
		version, err := jobs.GetVersion(ctx, "shipping-v2", 1, 1)
		if err != nil {
			if errors.Is(err, jobs.ErrUnsupportedWorkflowVersion) {
				return "", jobs.NoRetry(err)
			}
			return "", err
		}
		_ = version
		_, err = jobs.Call[quoteV2](ctx, "quote-shipping-v2", orderID)
		if err != nil {
			return "", err
		}
		return "done", nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "process-order", "order-3", jobs.Retries(4))
	require.NoError(t, err)
	runWorkerUntilFirstFailure(t, q, id)
	deployedV2.Store(true)
	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	t.Logf("status=%s last_error=%q v2=%d", job.Status, job.LastError, v2Runs.Load())

	assert.Equal(t, core.StatusFailed, job.Status)
	assert.Contains(t, job.LastError, "unsupported workflow version",
		"the run must fail loud on the sentinel, not on a determinism violation")
	assert.Zero(t, v2Runs.Load(), "the removed-branch run must never issue the new Call")
}

// runWorkerUntilFirstFailure runs a worker only long enough for the job to record
// its first failed attempt, then stops it. This is how a test lands a deploy
// BETWEEN two attempts of one job.
func runWorkerUntilFirstFailure(t *testing.T, q *jobs.Queue, id core.UUID) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := q.NewWorker(jobs.WithPollInterval(20 * time.Millisecond))
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Start(ctx) }()

	require.Eventually(t, func() bool {
		job, err := q.Storage().GetJob(context.Background(), id)
		if err != nil {
			return false
		}
		return job.Attempt >= 1 && job.Status != core.StatusRunning
	}, 40*time.Second, 25*time.Millisecond, "job never recorded a first attempt")

	cancel()
	<-done
}
