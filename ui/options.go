// Package ui provides an embeddable web dashboard for monitoring jobs.
package ui

import (
	"context"
	"net/http"
	"time"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
)

// Option configures the UI handler.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type config struct {
	ctx                          context.Context
	middleware                   func(http.Handler) http.Handler
	queue                        *queue.Queue
	statsRetention               time.Duration
	insecureAllowUnauthenticated bool
	authorizer                   Authorizer
	allowedOrigins               map[string]struct{}
	metadataRedaction            bool
}

// WithMiddleware wraps the handler with middleware (auth, logging, etc.).
func WithMiddleware(mw func(http.Handler) http.Handler) Option {
	return optionFunc(func(c *config) {
		c.middleware = mw
	})
}

// WithQueue provides access to the queue for scheduled jobs view and event streaming.
func WithQueue(q *queue.Queue) Option {
	return optionFunc(func(c *config) {
		c.queue = q
	})
}

// WithStatsRetention sets how long stats rows are kept. Default: 31 days, so the
// dashboard's longest (30d) throughput window always has data. Lower it to reduce
// stats-table growth if you do not use the longer windows.
func WithStatsRetention(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.statsRetention = d
	})
}

// WithInsecureAllowUnauthenticated permits all dashboard RPCs without an authorizer.
// This is intended for local development and trusted networks only.
func WithInsecureAllowUnauthenticated() Option {
	return optionFunc(func(c *config) {
		c.insecureAllowUnauthenticated = true
	})
}

// WithInsecureAllowUnauthenticatedWrites permits all dashboard RPCs without an
// authorizer. This is intended for local development and trusted networks only.
//
// Deprecated: use [WithInsecureAllowUnauthenticated], which makes explicit that
// the unauthenticated surface includes reads (job payloads), not only writes.
func WithInsecureAllowUnauthenticatedWrites() Option {
	return WithInsecureAllowUnauthenticated()
}

// WithAuthorizer configures per-action authorization for dashboard RPCs.
func WithAuthorizer(a Authorizer) Option {
	return optionFunc(func(c *config) {
		c.authorizer = a
	})
}

// WithAllowedOrigins permits browser mutating RPCs from the given origins in
// addition to same-origin requests. Values must be full origins such as
// "https://ops.example.com".
func WithAllowedOrigins(origins ...string) Option {
	return optionFunc(func(c *config) {
		if c.allowedOrigins == nil {
			c.allowedOrigins = make(map[string]struct{}, len(origins))
		}
		for _, origin := range origins {
			c.allowedOrigins[origin] = struct{}{}
		}
	})
}

// WithMetadataRedaction controls best-effort secret redaction for job metadata
// values returned by the dashboard. Metadata redaction is enabled by default.
func WithMetadataRedaction(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.metadataRedaction = enabled
	})
}

// WithContext provides a lifecycle context for background goroutines (e.g. stats collector).
// When cancelled, background workers flush and exit gracefully.
// If not provided, context.Background() is used (goroutines run until process exit).
func WithContext(ctx context.Context) Option {
	return optionFunc(func(c *config) {
		c.ctx = ctx
	})
}
