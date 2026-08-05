package ui

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"github.com/stretchr/testify/require"
)

// A schedule with no future boundary — an unsatisfiable cron such as
// "0 0 30 2 *" — yields the zero time from Next(). The dashboard's catch-up loop
// then never runs, missed_fires stays 0 and overdue stays false, so the row
// rendered as "On track" for a schedule the WORKER has already logged once as
// "never fires: its schedule has no future boundary, so it is skipped".
//
// The two components disagreed about the same schedule, and the one an operator
// looks at was the one saying everything was fine.
//
// never_fires is a distinct state rather than a flavour of overdue on purpose:
// overdue is documented as "boundaries that SHOULD have fired have not", and a
// schedule that can never fire has no such boundaries. Reporting it as overdue
// would have contradicted that field's own meaning — the cheap fix, and the wrong
// one.
func TestListScheduledJobs_UnsatisfiableCronIsNotReportedHealthy(t *testing.T) {
	ctx := context.Background()
	svc, q := setupServiceWithQueue(t)

	// 30 February: parses fine, matches nothing, ever.
	dead, err := schedule.Cron("0 0 30 2 *")
	require.NoError(t, err)
	require.True(t, dead.Next(time.Now()).IsZero(),
		"premise: this cron must have no future boundary, or the test proves nothing")

	registerScheduledTestHandler(q, "dead")
	require.NoError(t, q.Schedule("dead", nil, dead))

	// A live schedule alongside it, so the assertion cannot pass by the handler
	// simply reporting nothing.
	registerScheduledTestHandler(q, "alive")
	require.NoError(t, q.Schedule("alive", nil, schedule.Every(time.Hour)))

	resp, err := svc.ListScheduledJobs(ctx, connect.NewRequest(&jobsv1.ListScheduledJobsRequest{}))
	require.NoError(t, err)

	byName := map[string]*jobsv1.ScheduledJobInfo{}
	for _, j := range resp.Msg.Jobs {
		byName[j.Name] = j
	}
	require.Contains(t, byName, "dead")
	require.Contains(t, byName, "alive")

	deadInfo := byName["dead"]
	require.True(t, deadInfo.NeverFires,
		"a schedule with no future boundary must be reported as never_fires; without it the dashboard renders 'On track' for a schedule the worker has permanently skipped")
	require.Nil(t, deadInfo.NextRun, "a schedule that can never fire has no next run")
	require.False(t, deadInfo.Overdue,
		"never_fires must NOT be reported as overdue: overdue means boundaries that should have fired did not, and there are none")

	aliveInfo := byName["alive"]
	require.False(t, aliveInfo.NeverFires,
		"an ordinary schedule must not be flagged; if this fires the check is matching something other than an absent boundary")
	require.NotNil(t, aliveInfo.NextRun, "premise: the live schedule must still report a next run")
}
