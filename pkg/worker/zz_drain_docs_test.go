package worker

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file guards the shutdown/drain contract as it is DESCRIBED to operators.
//
// Pages protected:
//
//	docs/content/docs/production-ops.md  ("## Graceful Drain")
//	docs/content/docs/getting-started.md ("For graceful shutdown")
//
// Round 40 found both pages describing a drain the worker does not implement.
// production-ops.md told operators to "use a drain timeout long enough for
// normal handlers to observe ctx.Done()" — but Start builds handler contexts
// with context.WithoutCancel, so NO handler is cancelled during the drain
// window; ctx.Done() fires only after the whole DrainTimeout has elapsed. The
// same page said that when the timeout expires "the worker cancels remaining
// handlers and waits for them to return", when in fact that second wait is
// bounded by a 5s forced-drain grace after which handlers are ABANDONED and
// Start returns while they are still executing. getting-started.md's "For
// graceful shutdown" snippet threw Start's return away with `go`, leaving the
// caller nothing to wait on at all.
//
// Reading never caught any of it. These tests execute the behaviour and then
// require the prose to match, so the two can only change together.

// drainDocPaths are resolved relative to this package's directory (go test runs
// with cwd = the package dir). docs/ is a nested Hugo module excluded from the
// published module zip, so this is a repo-checkout guard; CI runs it because
// test.Dockerfile COPYs the whole tree and the package list includes pkg/worker.
const (
	productionOpsDocPath   = "../../docs/content/docs/production-ops.md"
	gettingStartedDocPath  = "../../docs/content/docs/getting-started.md"
	drainSectionHeading    = "## Graceful Drain"
	gracefulShutdownAnchor = "For graceful shutdown"
)

func readDoc(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", path)
	return string(b)
}

// TestDrainWindowDoesNotCancelHandlerContexts executes the behaviour
// production-ops.md now describes: during phase 1 the handler context is NOT
// cancelled, so ctx.Done() fires only once the whole DrainTimeout has elapsed.
//
// If someone "fixes" Start to propagate cancellation into handlers during the
// drain, this fails and the page must be rewritten in the same commit.
func TestDrainWindowDoesNotCancelHandlerContexts(t *testing.T) {
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()

	const drain = 700 * time.Millisecond

	running := make(chan struct{})
	var once sync.Once
	observed := make(chan time.Duration, 1)
	var shutdownAt time.Time
	var mu sync.Mutex

	q.Register("ctx-aware", func(ctx context.Context, _ struct{}) error {
		once.Do(func() { close(running) })
		<-ctx.Done()
		mu.Lock()
		began := shutdownAt
		mu.Unlock()
		observed <- time.Since(began)
		return nil
	})
	_, err := q.Enqueue(context.Background(), "ctx-aware", struct{}{})
	require.NoError(t, err)

	w := NewWorker(q,
		WithPollInterval(20*time.Millisecond),
		WithDrainTimeout(drain),
		WithOwnershipAuditInterval(0),
	)
	w.forcedHandlerDrainGrace = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	ret := make(chan error, 1)
	go func() { ret <- w.Start(ctx) }()

	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	mu.Lock()
	shutdownAt = time.Now()
	mu.Unlock()
	cancel()

	var delay time.Duration
	select {
	case delay = <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never observed ctx.Done()")
	}
	<-ret

	// The whole point: cancellation reaches the handler only in phase 2, i.e.
	// after DrainTimeout. Half the window is a generous floor for "not early".
	require.Greaterf(t, delay, drain/2,
		"handler observed ctx.Done() after %s with DrainTimeout=%s; production-ops.md's "+
			"\"Graceful Drain\" section says handler contexts are NOT cancelled during phase 1 "+
			"and ctx.Done() fires only in phase 2 — rewrite that section if this is now false",
		delay, drain)
}

// TestForcedDrainGraceIsFiveSecondsAndDocumented pins the phase-2 bound that
// production-ops.md prints as a literal number. The grace is what makes
// "cancels remaining handlers and waits for them to return" false: after it
// elapses the handlers are abandoned and Start returns anyway.
func TestForcedDrainGraceIsFiveSecondsAndDocumented(t *testing.T) {
	require.Equal(t, 5*time.Second, defaultForcedHandlerDrainGrace,
		"the forced-drain grace is printed on production-ops.md; change both together")

	// NewWorker must actually install the constant, or the documented number
	// would describe a value nothing uses.
	q, cleanup := newSQLiteQueue(t)
	defer cleanup()
	require.Equal(t, defaultForcedHandlerDrainGrace, NewWorker(q).forcedHandlerDrainGrace)

	section := docSection(t, readDoc(t, productionOpsDocPath), drainSectionHeading)
	require.Contains(t, section, "5-second forced-drain grace",
		"production-ops.md's %q section must state the phase-2 bound", drainSectionHeading)
	require.Contains(t, section, "abandoning them",
		"the section must quote the log line an operator will actually see when handlers are abandoned")
}

// TestDrainDocsDoNotPromiseCancellationDuringTheDrainWindow re-asserts the two
// specific false sentences round 40 found, so neither can come back.
func TestDrainDocsDoNotPromiseCancellationDuringTheDrainWindow(t *testing.T) {
	section := docSection(t, readDoc(t, productionOpsDocPath), drainSectionHeading)

	require.NotContains(t, section, "long enough for normal handlers to observe",
		"handler contexts are not cancelled during the drain window; sizing advice must not "+
			"tell operators the window is time to notice cancellation")
	require.NotContains(t, section, "cancels remaining handlers and waits for them to return",
		"the second wait is bounded by the forced-drain grace; handlers are abandoned, not waited for")

	// And the correction must be present, not merely the falsehood absent.
	require.Contains(t, section, "Handler contexts are *not* cancelled here")
	require.Contains(t, section, "while they are still executing")
}

// TestGettingStartedShutdownSnippetWaitsOnStart guards the snippet a reader
// copies. It must not launch Start with `go` (which discards the drain) and it
// must wait on Start's return.
func TestGettingStartedShutdownSnippetWaitsOnStart(t *testing.T) {
	md := readDoc(t, gettingStartedDocPath)
	i := strings.Index(md, gracefulShutdownAnchor)
	require.GreaterOrEqualf(t, i, 0, "%s no longer contains %q", gettingStartedDocPath, gracefulShutdownAnchor)

	block := firstGoBlock(t, md[i:])
	require.NotContains(t, block, "go worker.Start(",
		"the graceful-shutdown snippet must not discard Start's return: `go worker.Start(ctx)` "+
			"leaves the caller nothing to wait on, so the process exits mid-drain")
	require.Contains(t, block, "worker.Start(ctx)")
	require.Contains(t, block, "if err := worker.Start(ctx)",
		"the snippet must wait on Start's return — that return is the only signal the drain finished")
}

// docSection returns the markdown between heading and the next heading of the
// same or higher level.
func docSection(t *testing.T, md, heading string) string {
	t.Helper()
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	require.Positivef(t, level, "heading %q must start with #", heading)

	lines := strings.Split(md, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) != heading {
			continue
		}
		require.Equalf(t, -1, start, "heading %q appears more than once; this guard needs it unique", heading)
		start = i
	}
	require.GreaterOrEqualf(t, start, 0, "heading %q not found", heading)

	for i := start + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		if hashes := len(trimmed) - len(strings.TrimLeft(trimmed, "#")); hashes <= level {
			return strings.Join(lines[start:i], "\n")
		}
	}
	return strings.Join(lines[start:], "\n")
}

// firstGoBlock returns the contents of the first ```go fence in md.
func firstGoBlock(t *testing.T, md string) string {
	t.Helper()
	const open = "```go\n"
	i := strings.Index(md, open)
	require.GreaterOrEqual(t, i, 0, "no ```go block found")
	rest := md[i+len(open):]
	j := strings.Index(rest, "```")
	require.GreaterOrEqual(t, j, 0, "unterminated ```go block")
	return rest[:j]
}
