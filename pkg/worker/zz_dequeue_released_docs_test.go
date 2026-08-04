package worker

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This file guards the jobs.dequeue.released row of the metric catalog in
// docs/content/docs/advanced/metrics.md.
//
// That row enumerates the reason values, in the closed
// `reason=a|b|c` style the table uses elsewhere. It listed six.
// DequeueReleasedByReason returns SEVEN — the seventh being
// bouncePaused ("paused"), which the observable callback emits on EVERY scrape
// of an instrumented worker, not only after a pause. An operator who built
//
//	rate(jobs_dequeue_released_total{reason=~"queue_cap|queue_rate|..."}[5m])
//
// from the documented set silently dropped the pause-race churn signal — exactly
// the reason a claimed batch gets released without running.
//
// This test derives the emitted set from the real map and requires the page's
// enumeration to match it, so adding an eighth reason without touching the page
// fails here.

const metricsDocPath = "../../docs/content/docs/advanced/metrics.md"

var dequeueReleasedRowRE = regexp.MustCompile(`\|\s*` + "`jobs\\.dequeue\\.released`" + `\s*\|[^\n]*`)

func TestDequeueReleasedReasonEnumerationMatchesDoc(t *testing.T) {
	// The emitted set, straight off a Worker. A zero-value Worker is enough:
	// DequeueReleasedByReason enumerates every key unconditionally, which is
	// precisely why every scrape carries all of them.
	w := &Worker{}
	emitted := make([]string, 0, 8)
	for k := range w.DequeueReleasedByReason() {
		emitted = append(emitted, k)
	}
	sort.Strings(emitted)
	require.Contains(t, emitted, string(bouncePaused),
		"bouncePaused must be in the emitted map, or this guard is testing nothing")

	b, err := os.ReadFile(metricsDocPath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it rather than deleting it", metricsDocPath)

	row := dequeueReleasedRowRE.FindString(string(b))
	require.NotEmpty(t, row, "jobs.dequeue.released row not found in the metric catalog")

	// The row writes the enumeration as reason=a\|b\|c (pipes escaped for the
	// Markdown table).
	start := strings.Index(row, "`reason=")
	require.GreaterOrEqual(t, start, 0, "row does not enumerate reason values: %s", row)
	rest := row[start+len("`reason="):]
	end := strings.Index(rest, "`")
	require.GreaterOrEqual(t, end, 0, "unterminated reason enumeration: %s", row)

	documented := strings.Split(strings.ReplaceAll(rest[:end], `\|`, "|"), "|")
	for i := range documented {
		documented[i] = strings.TrimSpace(documented[i])
	}
	sort.Strings(documented)

	require.Equal(t, emitted, documented,
		"the metric catalog's reason enumeration must list exactly the reasons DequeueReleasedByReason emits")
}
