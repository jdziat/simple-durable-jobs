---
title: "Dashboard Authorization"
weight: 18
---

The dashboard protects every Connect RPC with a per-action `ui.Authorizer` by
default. Without an authorizer, RPCs fail closed unless you explicitly opt in
with `ui.WithInsecureAllowUnauthenticated()` for local development or trusted
networks. The dashboard does not impose an identity model. Your
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
	case ui.ActionViewJobs, ui.ActionViewJob, ui.ActionViewStats, ui.ActionWatchEvents:
		return nil
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

The dashboard passes one of these actions for read RPCs:

| Action | RPC |
|---|---|
| `ui.ActionViewStats` | `GetStats`, `GetStatsHistory`, `ListQueues` |
| `ui.ActionViewJobs` | `ListJobs`, `ListScheduledJobs`, `ListWorkflows` |
| `ui.ActionViewJob` | `GetJob`, `GetWorkflow` |
| `ui.ActionWatchEvents` | `WatchEvents` |

Unknown read RPCs default to `ui.ActionViewJobs` so new procedures are not left
ungated.

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

## Default Gate

`ui.Handler` fails closed by default: without `ui.WithAuthorizer(...)` or
`ui.WithInsecureAllowUnauthenticated()`, all dashboard RPCs (reads and mutations) return
`PermissionDenied`. This is an authorization gate only — it does not provide
transport encryption, CSRF protection, or audit logging; operate the dashboard
behind your own TLS and network controls. Static frontend assets remain public
because they do not carry job payloads or metadata.

Use the insecure opt-in only for local development or a trusted network:

```go
http.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(
	store,
	ui.WithQueue(q),
	ui.WithInsecureAllowUnauthenticated(),
)))
```

`ui.WithInsecureAllowUnauthenticatedWrites()` remains available as a deprecated
alias for `ui.WithInsecureAllowUnauthenticated()`. The alias now permits both
reads and writes, matching the dashboard's unified explicit opt-in.

`ui.WithMiddleware(...)` does not grant access by itself. Middleware is useful
for authentication, logging, headers, and principal injection, but RPC access is
controlled only by `ui.WithAuthorizer(...)` or the explicit insecure opt-in.

## Origin Checks for Mutations

Mutating dashboard RPCs also check the browser `Origin` header. Requests with
no `Origin` header are allowed so CLI and server-to-server Connect clients keep
working. Browser requests with an `Origin` must be same-origin with the request
host or match an explicit allow-list:

```go
http.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(
	store,
	ui.WithMiddleware(authMiddleware),
	ui.WithAuthorizer(DashboardAuthorizer{}),
	ui.WithAllowedOrigins("https://ops.example.com"),
)))
```

The embedded SPA is served from the same origin as the API, so it does not need
this option. Cross-origin dashboard deployments do. If your authentication uses
cookies or any automatically attached browser credential, keep this Origin check
enabled and list only trusted dashboard origins.

## Programmatic Reuse

The authorizer is only wired into the dashboard's Connect RPC interceptor.
Queue and storage APIs are unchanged, and no schema migration is involved.

If your application exposes programmatic admin operations, call the same
authorizer before invoking operations such as `Requeue`, `PauseJob`, or
`CancelJob` from your own handlers. That keeps the policy consistent without
adding authorization parameters to queue or core storage methods.
