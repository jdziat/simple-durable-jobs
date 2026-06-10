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
	ctx                                context.Context
	middleware                         func(http.Handler) http.Handler
	queue                              *queue.Queue
	statsRetention                     time.Duration
	insecureAllowUnauthenticatedWrites bool
	authorizer                         Authorizer
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

// WithInsecureAllowUnauthenticatedWrites permits mutating RPCs without auth middleware.
// This is intended for local development and tests only.
func WithInsecureAllowUnauthenticatedWrites() Option {
	return optionFunc(func(c *config) {
		c.insecureAllowUnauthenticatedWrites = true
	})
}

// WithAuthorizer configures per-action authorization for mutating dashboard RPCs.
func WithAuthorizer(a Authorizer) Option {
	return optionFunc(func(c *config) {
		c.authorizer = a
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
