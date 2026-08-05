package jobs_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file guards the Determinism table in
// docs/content/docs/api-reference/job-options.md.
//
// The shipped table said of Strict: "Replay panics if a new Call() invocation
// appears that was not present in the checkpoint history". Both halves were
// wrong, and in opposite directions:
//
//   - a replay that ADDS a Call not in the checkpoint history runs it fresh and
//     the job COMPLETES, in Strict mode, with no guard at all;
//   - the failure Strict really produces is the OPPOSITE trigger — a replay that
//     drops or reorders recorded Calls — and it is a terminal ERROR
//     (core.NoRetry), never a panic.
//
// An operator who enabled Strict to catch "a call appeared that wasn't there
// before" got nothing, and never learned it. These tests execute all three modes
// and then require the page's prose to match, so the two can only change
// together.

const determinismDocPath = "../docs/content/docs/api-reference/job-options.md"

func readDeterminismDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(determinismDocPath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", determinismDocPath)
	return string(b)
}

// registerAddCallWorkflow builds a handler whose replay issues an EXTRA Call
// ("extra") that the first attempt never made.
func registerAddCallWorkflow(q *jobs.Queue, runs *atomic.Int32, extraRuns *atomic.Int32) {
	q.Register("base", func(_ context.Context, n int) (int, error) { return n * 2, nil })
	q.Register("extra", func(_ context.Context, n int) (int, error) {
		extraRuns.Add(1)
		return n + 10, nil
	})
	q.Register("adds-a-call", func(ctx context.Context, _ struct{}) (int, error) {
		attempt := runs.Add(1)
		a, err := jobs.Call[int](ctx, "base", 2)
		if err != nil {
			return 0, err
		}
		if attempt == 1 {
			return 0, errors.New("forced retryable failure")
		}
		b, err := jobs.Call[int](ctx, "extra", a) // NOT in the checkpoint history
		if err != nil {
			return 0, err
		}
		return b, nil
	})
}

// TestDeterminism_StrictToleratesAnAddedCall pins the fact the table now states:
// in Strict mode an added Call executes and the job completes. No panic, no
// error.
func TestDeterminism_StrictToleratesAnAddedCall(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs, extraRuns atomic.Int32
	registerAddCallWorkflow(q, &runs, &extraRuns)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "adds-a-call", struct{}{}, jobs.Retries(3), jobs.Determinism(jobs.Strict))
	require.NoError(t, err)
	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	t.Logf("strict + added call: status=%s last_error=%q runs=%d extraRuns=%d result=%s",
		job.Status, job.LastError, runs.Load(), extraRuns.Load(), string(job.Result))

	assert.Equal(t, core.StatusCompleted, job.Status,
		"Strict does not guard against a Call that APPEARS on replay")
	assert.Equal(t, int32(1), extraRuns.Load(), "the added call must have executed")
}

// TestDeterminism_StrictFailsTerminallyOnADroppedCall pins the guard Strict
// really has, and that it is a terminal error rather than a panic.
func TestDeterminism_StrictFailsTerminallyOnADroppedCall(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs atomic.Int32

	q.Register("base", func(_ context.Context, n int) (int, error) { return n * 2, nil })
	q.Register("second", func(_ context.Context, n int) (int, error) { return n + 1, nil })
	q.Register("drops-a-call", func(ctx context.Context, _ struct{}) (int, error) {
		attempt := runs.Add(1)
		a, err := jobs.Call[int](ctx, "base", 2)
		if err != nil {
			return 0, err
		}
		if attempt == 1 {
			b, err := jobs.Call[int](ctx, "second", a)
			if err != nil {
				return 0, err
			}
			_ = b
			return 0, errors.New("forced retryable failure")
		}
		// The replay never issues "second".
		return a, nil
	})

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "drops-a-call", struct{}{}, jobs.Retries(3), jobs.Determinism(jobs.Strict))
	require.NoError(t, err)
	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	t.Logf("strict + dropped call: status=%s last_error=%q attempt=%d", job.Status, job.LastError, job.Attempt)

	assert.Equal(t, core.StatusFailed, job.Status)
	assert.Contains(t, job.LastError, "strict determinism violation")
	assert.Contains(t, job.LastError, "were not replayed")
	assert.Contains(t, job.LastError, "no retry:", "the Strict guard is terminal, not retryable")
}

// TestDeterminism_DefaultModeAlsoToleratesAnAddedCall shows the added-call
// behaviour is not special to Strict, which is why the page can no longer use it
// as the distinguishing property of any mode.
func TestDeterminism_DefaultModeAlsoToleratesAnAddedCall(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	var runs, extraRuns atomic.Int32
	registerAddCallWorkflow(q, &runs, &extraRuns)

	ctx := context.Background()
	id, err := q.Enqueue(ctx, "adds-a-call", struct{}{}, jobs.Retries(3), jobs.Determinism(jobs.ExplicitCheckpoints))
	require.NoError(t, err)
	runWorkerUntilTerminal(t, q, id)

	job, err := q.Storage().GetJob(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, core.StatusCompleted, job.Status)
	assert.Equal(t, int32(1), extraRuns.Load())
}

// TestDeterminismDocTableMatchesBehaviour requires the page to describe what the
// tests above just executed, and to have shed the two false claims.
func TestDeterminismDocTableMatchesBehaviour(t *testing.T) {
	doc := readDeterminismDoc(t)

	idx := strings.Index(doc, "### `Determinism(mode DeterminismMode) Option`")
	require.GreaterOrEqual(t, idx, 0, "Determinism section heading not found")
	section := doc[idx:]
	if end := strings.Index(section[1:], "\n### "); end >= 0 {
		section = section[:end+1]
	}

	assert.NotContains(t, section, "Replay panics",
		"no determinism mode panics; every outcome is a returned error")
	assert.NotContains(t, section, "panics if",
		"no determinism mode panics; every outcome is a returned error")

	// The section must state the guard Strict actually has, in the direction it
	// actually fires.
	assert.Contains(t, section, "fewer or reordered",
		"the page must name the real Strict trigger (dropped/reordered Calls)")
	assert.Contains(t, section, "terminally",
		"the page must say the Strict guard is terminal")
	assert.Contains(t, section, "extra",
		"the page must state that an extra Call on replay is tolerated by every mode")
}
