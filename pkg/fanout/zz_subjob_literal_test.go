package fanout

import (
	"testing"

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
	}, cfg, literalTestCtx(), core.UUID("f-1"))
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

	built, err := buildSubJobs([]SubJob{{Type: "work", Args: "x"}}, cfg, literalTestCtx(), core.UUID("f-2"))
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
	}, cfg, literalTestCtx(), core.UUID("f-3"))
	require.NoError(t, err)
	require.Len(t, built, 1)

	assert.Equal(t, 0, built[0].MaxRetries,
		"queue.Retries(0) means run ONCE; the fan-out default must not override an explicit choice")
	assert.Equal(t, 0, built[0].Priority)
}
