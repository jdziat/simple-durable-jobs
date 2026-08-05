package worker

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/require"
)

// retentionDocPath is the operator-facing retention manual, resolved relative to
// this package's directory (go test runs with cwd = the package dir).
//
// Why a Go test parses English prose: the 2026-07-24 teardown found this page
// opening with "## Disabled by default" and telling operators that terminal jobs
// and consumed signal rows "are kept forever unless you configure retention on a
// worker" — the exact inverse of NewWorker, which has stamped 30/90/7-day
// windows on every unconfigured worker since the defaults landed. An operator
// who believed the manual and scheduled quarterly archival silently lost job
// history at 30 days and dead-letter triage rows at 90. Prose does not track a
// constant on its own, so the numbers on the page are pinned to the code here
// and the two can only change together.
//
// docs/ is a nested Hugo module and is excluded from the published module zip,
// so this is a repo-checkout guard. CI runs it: test.Dockerfile COPYs the whole
// tree into the image and scripts/docker-test-entrypoint.sh's package list
// includes pkg/worker.
const retentionDocPath = "../../docs/content/docs/advanced/retention-gc.md"

// retentionDocDays matches an "N days" / "N day" window value in a table row.
var retentionDocDays = regexp.MustCompile(`(\d+)\s+days?\b`)

func readRetentionDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(retentionDocPath)
	require.NoErrorf(t, err, "cannot read the retention manual at %s; if the page moved, move this guard with it rather than deleting it", retentionDocPath)
	return string(b)
}

// retentionDocSection returns the markdown between heading and the next heading
// of the same or higher level. Scoping by section is what lets the stock-window
// table and the DefaultRetention preset table both say "7 days" without this
// parser confusing one for the other.
func retentionDocSection(t *testing.T, md, heading string) string {
	t.Helper()
	level := len(heading) - len(strings.TrimLeft(heading, "#"))
	require.Positivef(t, level, "heading %q must start with #", heading)

	lines := strings.Split(md, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) != heading {
			continue
		}
		require.Equalf(t, -1, start, "heading %q appears more than once in %s; this guard needs it unique", heading, retentionDocPath)
		start = i
	}
	require.NotEqualf(t, -1, start, "heading %q not found in %s; the values under it are pinned to code, so keep the heading (or update this test with it)", heading, retentionDocPath)

	for i := start + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		if n := len(trimmed) - len(strings.TrimLeft(trimmed, "#")); n <= level {
			return strings.Join(lines[start+1:i], "\n")
		}
	}
	return strings.Join(lines[start+1:], "\n")
}

// retentionDocWindow returns the single "N days" value on the one line of
// section that contains marker.
//
// The exact-count requires are the load-bearing part. A matcher loose enough to
// match nothing would let this file stay green while the page said anything at
// all — the seed-only, never-fails guard this repo has shipped before. Exactly
// one row, exactly one number on it, or fail with both sides named.
func retentionDocWindow(t *testing.T, section, heading, marker string) time.Duration {
	t.Helper()
	var rows []string
	for _, line := range strings.Split(section, "\n") {
		if strings.Contains(line, marker) {
			rows = append(rows, line)
		}
	}
	require.Lenf(t, rows, 1, "expected exactly one table row containing %s under %q in %s; found %d: %q", marker, heading, retentionDocPath, len(rows), rows)

	nums := retentionDocDays.FindAllStringSubmatch(rows[0], -1)
	require.Lenf(t, nums, 1, "expected exactly one \"N days\" value on the %s row under %q in %s; found %d in %q", marker, heading, retentionDocPath, len(nums), rows[0])

	days, err := strconv.Atoi(nums[0][1])
	require.NoErrorf(t, err, "unparsable day count on the %s row under %q in %s: %q", marker, heading, retentionDocPath, rows[0])
	require.Positivef(t, days, "window on the %s row under %q in %s must be positive: %q", marker, heading, retentionDocPath, rows[0])
	return time.Duration(days) * 24 * time.Hour
}

func requireDocumentedWindow(t *testing.T, md, heading, marker string, want time.Duration, codeRef string) {
	t.Helper()
	require.Zerof(t, want%(24*time.Hour), "%s is %v, which is not a whole number of days; the docs table can only state whole days, so change the table's units too", codeRef, want)

	got := retentionDocWindow(t, retentionDocSection(t, md, heading), heading, marker)
	require.Equalf(t, want, got,
		"retention docs drifted from the code: %s documents %v (%d days) for %s, but %s is %v (%d days). Fix whichever is wrong — they ship to operators together.",
		retentionDocPath, got, int(got/(24*time.Hour)), marker, codeRef, want, int(want/(24*time.Hour)))
}

// TestRetentionDocMatchesStockWindows pins the published default windows to the
// constants NewWorker actually stamps on a worker built with no retention
// options.
func TestRetentionDocMatchesStockWindows(t *testing.T) {
	md := readRetentionDoc(t)
	const heading = "### Default windows"

	requireDocumentedWindow(t, md, heading, "`RetentionCompletedAfter`", defaultRetentionCompletedAfter, "defaultRetentionCompletedAfter")
	requireDocumentedWindow(t, md, heading, "`RetentionFailedAfter`", defaultRetentionFailedAfter, "defaultRetentionFailedAfter")
	requireDocumentedWindow(t, md, heading, "`RetentionConsumedSignalsAfter`", defaultRetentionConsumedSignalsAfter, "defaultRetentionConsumedSignalsAfter")

	// Pinning only the numbers would be a false green for the defect that
	// produced this file: the old page carried the right kind of table nowhere
	// near its headline, which said retention was DISABLED by default. Assert the
	// behaviour the headline now promises, and the headline itself.
	w := NewWorker(queue.New(&mockStorage{}), WithOwnershipAuditInterval(0))
	require.True(t, w.config.Retention.enabled(),
		"an unconfigured worker must have retention enabled; if this ever becomes opt-in again, %s has to stop saying it is on by default", retentionDocPath)
	require.Containsf(t, md, "## Enabled by default",
		"%s must lead with the fact that retention is on by default; an operator who reads the opposite loses terminal job history at %v", retentionDocPath, defaultRetentionCompletedAfter)
}

// TestRetentionDocMatchesDefaultRetentionPreset pins the documented
// DefaultRetention() preset to the option itself. The old page called the preset
// "not a silent default", written when there was no default at all; the preset is
// in fact TIGHTER than the stock windows, which is the opposite of what an
// operator would assume from the name.
func TestRetentionDocMatchesDefaultRetentionPreset(t *testing.T) {
	md := readRetentionDoc(t)
	const heading = "## The DefaultRetention preset"

	var cfg WorkerConfig
	DefaultRetention().ApplyWorker(&cfg)

	requireDocumentedWindow(t, md, heading, "| Completed jobs |", cfg.Retention.CompletedAfter, "DefaultRetention().CompletedAfter")
	requireDocumentedWindow(t, md, heading, "| Terminal failed and cancelled jobs |", cfg.Retention.FailedAfter, "DefaultRetention().FailedAfter")
	requireDocumentedWindow(t, md, heading, "| Consumed signal rows |", cfg.Retention.ConsumedSignalsAfter, "DefaultRetention().ConsumedSignalsAfter")

	require.Lessf(t, cfg.Retention.CompletedAfter, defaultRetentionCompletedAfter,
		"the page tells operators DefaultRetention() is tighter than the stock windows; make it true or reword %s", retentionDocPath)
	require.Lessf(t, cfg.Retention.FailedAfter, defaultRetentionFailedAfter,
		"the page tells operators DefaultRetention() is tighter than the stock windows; make it true or reword %s", retentionDocPath)
	require.LessOrEqualf(t, cfg.Retention.ConsumedSignalsAfter, defaultRetentionConsumedSignalsAfter,
		"the page tells operators DefaultRetention() is tighter than the stock windows; make it true or reword %s", retentionDocPath)
}

// readmePath is the OTHER page that describes retention to an operator, and the
// one most of them read first.
const readmePath = "../../README.md"

// TestReadmeRetentionBulletMatchesTheCode extends the retention-doc guard to the
// README. Round 40 found the README still carrying the pre-fix inversion long
// after retention-gc.md was corrected: it told operators that consumed-signal
// pruning is "opt-in" and that "a worker started with no retention configured
// logs a one-time WARN", implying the default keeps everything. Both are false —
// an unconfigured worker prunes on the stock windows and logs INFO "retention GC
// enabled"; the WARN fires only when retention is explicitly DISABLED. An
// operator who believed the README would schedule external archival that arrives
// after the rows are already gone.
//
// TestRetentionDocMatchesStockWindows guarded only retention-gc.md, which is why
// the README copy survived. Guard the pages together.
func TestReadmeRetentionBulletMatchesTheCode(t *testing.T) {
	b, err := os.ReadFile(readmePath)
	require.NoErrorf(t, err, "cannot read %s", readmePath)
	md := string(b)

	// The behaviour the bullet must not contradict.
	w := NewWorker(queue.New(&mockStorage{}), WithOwnershipAuditInterval(0))
	require.True(t, w.config.Retention.enabled(),
		"an unconfigured worker must have retention enabled, or the README bullet needs rewriting")

	require.NotContainsf(t, md, "consumed signals by opt-in age window",
		"%s calls consumed-signal pruning opt-in; ConsumedSignalsAfter defaults to %v on every worker",
		readmePath, defaultRetentionConsumedSignalsAfter)
	require.NotContainsf(t, md, "a worker started with no retention configured logs a one-time WARN",
		"%s says an unconfigured worker WARNs; it logs INFO and prunes. The WARN is the DISABLED case", readmePath)

	// And it must state the windows an operator plans archival around.
	for _, want := range []struct {
		days int
		what string
	}{
		{int(defaultRetentionCompletedAfter / (24 * time.Hour)), "completed"},
		{int(defaultRetentionFailedAfter / (24 * time.Hour)), "failed/cancelled"},
		{int(defaultRetentionConsumedSignalsAfter / (24 * time.Hour)), "consumed signals"},
	} {
		require.Containsf(t, md, strconv.Itoa(want.days),
			"%s must state the %s retention window (%d days) so archival is planned inside it",
			readmePath, want.what, want.days)
	}
}
