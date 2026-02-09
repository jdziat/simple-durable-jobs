// Package jobctx provides public access to job context for handlers.
package jobctx

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
)

// JobFromContext returns the current Job from context, or nil if not in a job handler.
// Use this to get the job ID for logging or progress tracking.
func JobFromContext(ctx context.Context) *core.Job {
	jc := intctx.GetJobContext(ctx)
	if jc == nil {
		return nil
	}
	return jc.Job
}

// JobIDFromContext returns the current job ID from context, or empty string if not in a job handler.
func JobIDFromContext(ctx context.Context) string {
	job := JobFromContext(ctx)
	if job == nil {
		return ""
	}
	return job.ID
}
