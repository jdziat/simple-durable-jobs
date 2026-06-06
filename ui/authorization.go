package ui

import "context"

// Action identifies a mutating dashboard RPC operation.
type Action string

const (
	ActionRetryJob       Action = "retry_job"
	ActionDeleteJob      Action = "delete_job"
	ActionBulkRetryJobs  Action = "bulk_retry_jobs"
	ActionBulkDeleteJobs Action = "bulk_delete_jobs"
	ActionPauseJob       Action = "pause_job"
	ActionCancelJob      Action = "cancel_job"
	ActionResumeJob      Action = "resume_job"
	ActionPauseQueue     Action = "pause_queue"
	ActionResumeQueue    Action = "resume_queue"
	ActionPurgeQueue     Action = "purge_queue"
)

// Authorizer decides whether a dashboard action is allowed.
// Returning nil allows the action; returning a non-nil error denies it.
type Authorizer interface {
	Authorize(ctx context.Context, action Action) error
}

type principalContextKey struct{}

// WithPrincipal stores a caller-defined principal in ctx.
func WithPrincipal(ctx context.Context, principal any) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns a caller-defined principal from ctx, if present.
func PrincipalFromContext(ctx context.Context) (any, bool) {
	principal := ctx.Value(principalContextKey{})
	if principal == nil {
		return nil, false
	}
	return principal, true
}
