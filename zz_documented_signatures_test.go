package jobs

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/worker"
)

// TestDocumentedWorkerSurfaceStillCompiles compile-asserts the worker API in the
// exact shapes these pages print:
//
//	README.md, docs/content/docs/getting-started.md, docs/content/docs/examples.md
//	docs/content/docs/api-reference/worker.md, .../queue.md
//
// Round 40 found that 37 documented call sites did not compile at all:
// (*Queue).NewWorker returns core.Starter (Start only), so every documented
// worker.Pause/Resume/CancelJob/Health call was unreachable as written, and
// CancelJob was documented as taking a string when it takes core.UUID. Reading
// the pages never caught it across many releases; compiling them caught it
// immediately. If you change one of these signatures, this stops compiling —
// update the pages in the same commit.
//
// The body is unreachable on purpose: this is a TYPE check, not a run.
func TestDocumentedWorkerSurfaceStillCompiles(t *testing.T) {
	if true {
		t.Skip("compile-time assertion only")
	}
	var q *Queue
	var w *Worker

	// README.md, getting-started.md, examples.md — the corrected constructor shapes.
	w = NewWorker(q)
	w = NewWorker(q, WorkerQueue("default", Concurrency(10)))
	w = NewWorker(q, WithScheduler(true))
	// api-reference/worker.md:161 — a pkg/worker option passed through the facade.
	w = NewWorker(q, worker.WithLockDuration(2*time.Hour))

	// The *Worker-only surface the pages call after constructing.
	var _ func(PauseMode) = w.Pause
	var _ func() = w.Resume
	var _ func(core.UUID) bool = w.CancelJob
	var _ func() http.Handler = w.HealthHandler
	var _ func(context.Context) error = w.Start

	// api-reference/worker.md:196 — PauseJob takes the *Queue; the others take Storage.
	var _ func(context.Context, *Queue, UUID, ...PauseOption) error = PauseJob
	var _ func(context.Context, Storage, UUID) error = ResumeJob
	var _ func(context.Context, Storage, UUID) (bool, error) = IsJobPaused
	var _ func(context.Context, Storage, string) ([]*Job, error) = GetPausedJobs
	var _ func(context.Context, Storage, string) error = PauseQueue
	_ = w
}
