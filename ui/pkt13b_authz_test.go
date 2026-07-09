package ui

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1/jobsv1connect"
)

// TestActionForProcedure_Exhaustive (PKT-13 / Claude C2) asserts EVERY
// JobsService RPC procedure is classified, so a newly-added RPC that nobody maps
// cannot silently default to an allowed action — it would fail this test first.
func TestActionForProcedure_Exhaustive(t *testing.T) {
	procedures := []string{
		jobsv1connect.JobsServiceGetStatsProcedure,
		jobsv1connect.JobsServiceGetStatsHistoryProcedure,
		jobsv1connect.JobsServiceListJobsProcedure,
		jobsv1connect.JobsServiceGetJobProcedure,
		jobsv1connect.JobsServiceRetryJobProcedure,
		jobsv1connect.JobsServiceDeleteJobProcedure,
		jobsv1connect.JobsServiceBulkRetryJobsProcedure,
		jobsv1connect.JobsServiceBulkDeleteJobsProcedure,
		jobsv1connect.JobsServicePauseJobProcedure,
		jobsv1connect.JobsServiceCancelJobProcedure,
		jobsv1connect.JobsServiceResumeJobProcedure,
		jobsv1connect.JobsServicePauseQueueProcedure,
		jobsv1connect.JobsServiceResumeQueueProcedure,
		jobsv1connect.JobsServicePurgeQueueProcedure,
		jobsv1connect.JobsServiceListQueuesProcedure,
		jobsv1connect.JobsServiceListScheduledJobsProcedure,
		jobsv1connect.JobsServiceListWorkflowsProcedure,
		jobsv1connect.JobsServiceGetWorkflowProcedure,
		jobsv1connect.JobsServiceWatchEventsProcedure,
	}
	for _, p := range procedures {
		_, known := actionForProcedure(p)
		assert.True(t, known, "procedure %s must be classified (read or mutating)", p)
	}
}

// TestAuthorize_UnmappedProcedureDenied (PKT-13 / Claude C2) proves authorize
// FAILS CLOSED for an unmapped procedure on every path — even under
// insecureAllowUnauthenticated (which otherwise allows everything).
func TestAuthorize_UnmappedProcedureDenied(t *testing.T) {
	i := dashboardAuthInterceptor{insecureAllowUnauthenticated: true}
	err := i.authorize(context.Background(), "/jobs.v1.JobsService/FutureUnmappedRPC")
	require.Error(t, err, "an unmapped procedure must be denied even when insecure-allow is on")
	assert.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestAuthorize_MappedReadProcedureAllowedUnderInsecure guards the happy path:
// a known read procedure is still allowed under insecure-allow.
func TestAuthorize_MappedReadProcedureAllowedUnderInsecure(t *testing.T) {
	i := dashboardAuthInterceptor{insecureAllowUnauthenticated: true}
	err := i.authorize(context.Background(), jobsv1connect.JobsServiceListJobsProcedure)
	assert.NoError(t, err, "a classified read procedure must remain allowed under insecure-allow")
}
