package fanout

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func literalTestCtx() *intctx.JobContext {
	return &intctx.JobContext{
		Job:           &core.Job{ID: core.UUID("p-lit"), Queue: "parent-q"},
		WorkerID:      "test-worker",
		HandlerLookup: func(string) (any, bool) { return nil, true },
	}
}

// SubJob is an exported struct with exported fields, and callers build it as a
// literal — the api-reference docs show the slice form and this package's own
// tests use bare literals. The *Set flags are written ONLY by the Sub() option
// path, so a fallback that tests the flag ALONE discards a literal's explicit
// non-zero value in favour of the fan-out default, silently: nothing reads the
// child's config back, so the sub-job simply runs with the wrong retry budget.
//
// FALSE-GREEN TRAP: a literal whose value happens to EQUAL the fan-out default
// passes either way. The value under test must differ from cfg.retries /
// cfg.priority, which is why WithFanOutRetries/WithPriority are set explicitly
// here rather than left at their defaults.
func TestBuildSubJobs_LiteralExplicitValuesSurvive(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 9, priority: 9}

	built, err := buildSubJobs([]SubJob{
		{Type: "work", Args: "x", Retries: 5, Priority: 3},
	}, cfg, literalTestCtx(), core.UUID("f-1"), time.Now())
	require.NoError(t, err)
	require.Len(t, built, 1)

	assert.Equal(t, 5, built[0].MaxRetries,
		"a literal's explicit Retries must survive: the *Set flag is only written by Sub(), "+
			"so testing it alone silently replaces the caller's value with the fan-out default")
	assert.Equal(t, 3, built[0].Priority,
		"same for an explicit Priority on a literal")
}

// TestBuildSubJobs_LiteralZeroStillTakesTheFanOutDefault pins the other half.
// A bare literal has no way to express "explicitly zero" — it has no option to
// set the flag — so zero must continue to mean "unset" for it, exactly as before.
func TestBuildSubJobs_LiteralZeroStillTakesTheFanOutDefault(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 9, priority: 9}

	built, err := buildSubJobs([]SubJob{{Type: "work", Args: "x"}}, cfg, literalTestCtx(), core.UUID("f-2"), time.Now())
	require.NoError(t, err)
	require.Len(t, built, 1)

	assert.Equal(t, 9, built[0].MaxRetries, "an unset literal still takes the fan-out default")
	assert.Equal(t, 9, built[0].Priority)
}

// TestBuildSubJobs_ExplicitZeroViaOptionStaysZero is the regression guard for the
// fix this wave shipped: queue.Retries(0) is a deliberate "run once", and the
// *Set flag is the only thing that can distinguish it from an absence. Widening
// the fallback to also test the value must NOT undo that.
func TestBuildSubJobs_ExplicitZeroViaOptionStaysZero(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 9, priority: 9}

	built, err := buildSubJobs([]SubJob{
		Sub("work", "x", queue.Retries(0), queue.Priority(0)),
	}, cfg, literalTestCtx(), core.UUID("f-3"), time.Now())
	require.NoError(t, err)
	require.Len(t, built, 1)

	assert.Equal(t, 0, built[0].MaxRetries,
		"queue.Retries(0) means run ONCE; the fan-out default must not override an explicit choice")
	assert.Equal(t, 0, built[0].Priority)
}

// A relative Delay must be measured from the fan-out's own creation, not from
// whenever the parent happens to be replayed.
//
// buildSubJobs runs on the REPLAY path too: after a crash between creating the
// fan-out record and persisting its children, the parent re-enters FanOut, sees a
// non-terminal fan-out, and rebuilds the child rows. Anchoring on time.Now() there
// would re-anchor every delayed child to the recovery instant, so a child asked to
// wait an hour waits an hour FROM THE REPLAY — drifting by the whole
// crash-to-recovery gap, on the one path where the caller cannot observe it.
//
// FALSE-GREEN TRAP: asserting only that RunAt is non-nil, or that it is "in the
// future", passes with the drift fully present. The assertion has to be against
// the ORIGINAL anchor.
func TestBuildSubJobs_DelayIsMeasuredFromTheFanOutAnchor(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 3}
	created := time.Now().Add(-30 * time.Minute) // the fan-out was made half an hour ago

	built, err := buildSubJobs([]SubJob{
		{Type: "work", Args: "x", Delay: time.Hour},
	}, cfg, literalTestCtx(), core.UUID("f-delay"), created)
	require.NoError(t, err)
	require.Len(t, built, 1)
	require.NotNil(t, built[0].RunAt)

	want := created.Add(time.Hour)
	assert.WithinDuration(t, want, *built[0].RunAt, time.Second,
		"a replay must reproduce the ORIGINAL schedule: anchoring on the replay instant would "+
			"make this child wait an extra 30 minutes, silently")
}

// TestBuildSubJobs_AbsoluteRunAtIgnoresTheAnchor is the control: only a relative
// Delay is anchor-sensitive. An absolute RunAt must be passed through untouched
// regardless of when the build happens.
func TestBuildSubJobs_AbsoluteRunAtIgnoresTheAnchor(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 3}
	at := time.Now().Add(2 * time.Hour).UTC()

	built, err := buildSubJobs([]SubJob{
		{Type: "work", Args: "x", RunAt: &at, Delay: time.Hour},
	}, cfg, literalTestCtx(), core.UUID("f-abs"), time.Now().Add(-90*time.Minute))
	require.NoError(t, err)
	require.NotNil(t, built[0].RunAt)

	assert.WithinDuration(t, at, *built[0].RunAt, time.Second,
		"an absolute RunAt wins over Delay and does not move with the anchor")
}

// TestBuildSubJobs_SchedulingAndDeterminismReachTheChild pins the EFFECT of the
// options, not their collection.
//
// FALSE-GREEN TRAP, confirmed by a reviewer: TestSub_CarriesSchedulingAndDeterminismOptions
// asserts the fields on the SubJob struct that Sub() returns, which proves the
// option was recorded and nothing more. Deleting `Determinism:` and `RunAt:` from
// the child core.Job that buildSubJobs constructs left the entire repo green — the
// options were collected and then dropped again, which is the exact bug the packet
// exists to fix.
func TestBuildSubJobs_SchedulingAndDeterminismReachTheChild(t *testing.T) {
	cfg := &config{queue: "fan-q", retries: 3}
	at := time.Now().Add(2 * time.Hour).UTC()

	built, err := buildSubJobs([]SubJob{
		Sub("work", "x",
			queue.At(at),
			queue.Determinism(queue.Strict),
			queue.Timeout(90*time.Second),
		),
	}, cfg, literalTestCtx(), core.UUID("f-opts"), time.Now())
	require.NoError(t, err)
	require.Len(t, built, 1)

	require.NotNil(t, built[0].RunAt, "RunAt must reach the child, not just the SubJob")
	assert.WithinDuration(t, at, *built[0].RunAt, time.Second)
	assert.Equal(t, int(queue.Strict), built[0].Determinism,
		"Determinism must reach the child — a sub-job asked to replay strictly must actually do so")
	assert.Equal(t, 90*time.Second, built[0].Timeout, "Timeout must reach the child")
}
