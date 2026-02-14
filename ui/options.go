// Package ui provides an embeddable web dashboard for monitoring jobs.
package ui

import (
	"context"
	"net/http"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// Option configures the UI handler.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type config struct {
	ctx            context.Context
	middleware     func(http.Handler) http.Handler
	queue          *queue.Queue
	statsRetention time.Duration
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

// WithStatsRetention sets how long stats rows are kept. Default: 7 days.
func WithStatsRetention(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.statsRetention = d
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
