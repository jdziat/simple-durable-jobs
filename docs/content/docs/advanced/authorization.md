---
title: "Dashboard Authorization"
weight: 18
---

The dashboard protects mutating Connect RPCs with an optional per-action
`ui.Authorizer`. The dashboard does not impose an identity model. Your
middleware authenticates the request, extracts whatever principal your
application uses, and stores it on the request context. The authorizer receives
that context and the requested dashboard action.

```go
package main

import (
	"context"
	"errors"
	"net/http"

	jobs "github.com/jdziat/simple-durable-jobs/v2"
	"github.com/jdziat/simple-durable-jobs/v2/ui"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Principal struct {
	UserID string
	Role   string
}

type DashboardAuthorizer struct{}

func (DashboardAuthorizer) Authorize(ctx context.Context, action ui.Action) error {
	principal, ok := ui.PrincipalFromContext(ctx)
	if !ok {
		return errors.New("missing principal")
	}
	user, ok := principal.(Principal)
	if !ok {
		return errors.New("invalid principal")
	}

	switch action {
	case ui.ActionRetryJob, ui.ActionBulkRetryJobs:
		if user.Role == "operator" || user.Role == "admin" {
			return nil
		}
	case ui.ActionDeleteJob, ui.ActionBulkDeleteJobs, ui.ActionPurgeQueue:
		if user.Role == "admin" {
			return nil
		}
	case ui.ActionPauseJob, ui.ActionCancelJob, ui.ActionResumeJob,
		ui.ActionPauseQueue, ui.ActionResumeQueue:
		if user.Role == "operator" || user.Role == "admin" {
			return nil
		}
	}

	return errors.New("not allowed")
}

func main() {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	q := jobs.New(store)

	authMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Replace this with your real authentication and identity lookup.
			principal := Principal{UserID: "u_123", Role: "operator"}
			next.ServeHTTP(w, r.WithContext(ui.WithPrincipal(r.Context(), principal)))
		})
	}

	http.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(
		store,
		ui.WithQueue(q),
		ui.WithMiddleware(authMiddleware),
		ui.WithAuthorizer(DashboardAuthorizer{}),
	)))
	http.ListenAndServe(":8080", nil)
}
```

## Authorizer API

`ui.Authorizer` is:

```go
type Authorizer interface {
	Authorize(ctx context.Context, action Action) error
}
```

Return `nil` to allow the action. Return a non-nil error to deny it. Plain
errors become `PermissionDenied` Connect errors with the same message. If you
return a `*connect.Error`, its code is preserved, so policies can return
`Unauthenticated`, `PermissionDenied`, or another code deliberately.

## Actions

The dashboard passes one of these actions for mutating RPCs:

| Action | RPC |
|---|---|
| `ui.ActionRetryJob` | `RetryJob` |
| `ui.ActionDeleteJob` | `DeleteJob` |
| `ui.ActionBulkRetryJobs` | `BulkRetryJobs` |
| `ui.ActionBulkDeleteJobs` | `BulkDeleteJobs` |
| `ui.ActionPauseJob` | `PauseJob` |
| `ui.ActionCancelJob` | `CancelJob` |
| `ui.ActionResumeJob` | `ResumeJob` |
| `ui.ActionPauseQueue` | `PauseQueue` |
| `ui.ActionResumeQueue` | `ResumeQueue` |
| `ui.ActionPurgeQueue` | `PurgeQueue` |

Read-only RPCs are never passed to the authorizer.

## Existing Write Gate

Without `ui.WithAuthorizer`, dashboard write behavior is unchanged: mutating
RPCs require either `ui.WithMiddleware(...)` or
`ui.WithInsecureAllowUnauthenticatedWrites()`.

When `ui.WithAuthorizer` is set, it governs mutating dashboard RPCs directly.
You usually still pair it with middleware so the authorizer has a principal to
read, but the dashboard does not require a specific middleware or identity
type.

`ui.WithInsecureAllowUnauthenticatedWrites()` remains a development and test
escape hatch for deployments that do not configure an authorizer.

## Programmatic Reuse

The authorizer is only wired into the dashboard's Connect RPC interceptor.
Queue and storage APIs are unchanged, and no schema migration is involved.

If your application exposes programmatic admin operations, call the same
authorizer before invoking operations such as `Requeue`, `PauseJob`, or
`CancelJob` from your own handlers. That keeps the policy consistent without
adding authorization parameters to queue or core storage methods.
