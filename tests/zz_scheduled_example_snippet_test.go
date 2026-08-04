package jobs_test

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// examples/scheduled/main.go ships three commented "in production, you'd
// use..." lines as copy-paste guidance. Daily and Weekly compiled; Cron did not:
//
//	// queue.Schedule("health-check", nil, jobs.Cron("0 * * * *"))
//	  -> multiple-value jobs.Cron("0 * * * *") (value of type (jobs.Schedule, error))
//	     in single-value context
//
// jobs.Cron returns (Schedule, error). A reader who uncommented the line the file
// told them to use got a build failure with no hint the comment was stale. It is
// a // comment inside a .go file, not a fenced snippet, so the docs-snippet
// compile gate never saw it — which is why this guard reads the file.

const scheduledExampleMainPath = "../examples/scheduled/main.go"

// scheduleWithCronArgRE matches a Schedule() call that passes a Cron(...) result
// straight through as its schedule argument — the single-value misuse.
var scheduleWithCronArgRE = regexp.MustCompile(`Schedule\([^)]*,\s*(jobs\.|schedule\.)?(Cron|CronIn)\(`)

// TestScheduledExampleCommentedGuidanceCompiles is the compiled half: this is
// the exact shape the file's comment now shows. If jobs.Cron's signature ever
// changes, this stops compiling and the comment must be updated with it.
func TestScheduledExampleCommentedGuidanceCompiles(t *testing.T) {
	q, _ := openIntegrationQueue(t)
	q.Register("health-check", func(_ context.Context, _ struct{}) error { return nil })
	q.Register("send-daily-report", func(_ context.Context, _ struct{}) error { return nil })
	q.Register("backup-database", func(_ context.Context, _ struct{}) error { return nil })

	// The corrected Cron guidance, verbatim in shape.
	hourly, err := jobs.Cron("0 * * * *")
	require.NoError(t, err)
	require.NoError(t, q.Schedule("health-check", nil, hourly))

	// The two adjacent lines, which always compiled — kept so a future signature
	// change to either is caught here rather than by a reader.
	require.NoError(t, q.Schedule("send-daily-report", nil, jobs.Daily(9, 0)))
	require.NoError(t, q.Schedule("backup-database", nil, jobs.Weekly(time.Sunday, 2, 0)))
}

// TestScheduledExampleHasNoSingleValueCronComment reads the shipped file and
// requires that no commented guidance line passes Cron's two-value result into a
// single-value context.
func TestScheduledExampleHasNoSingleValueCronComment(t *testing.T) {
	b, err := os.ReadFile(scheduledExampleMainPath)
	require.NoErrorf(t, err, "cannot read %s; if the example moved, move this guard with it rather than deleting it", scheduledExampleMainPath)

	var offenders []string
	for i, line := range strings.Split(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "//") {
			continue // real code is compiled by the build; only comments hide here
		}
		if scheduleWithCronArgRE.MatchString(trimmed) {
			offenders = append(offenders, strings.TrimSpace(line)+"  (line "+strconv.Itoa(i+1)+")")
		}
	}
	assert.Empty(t, offenders,
		"jobs.Cron returns (Schedule, error); a commented Schedule(..., jobs.Cron(...)) does not compile when uncommented")

	assert.Contains(t, string(b), `jobs.Cron("0 * * * *")`,
		"the Cron guidance should still be present, in its two-value form")
}
