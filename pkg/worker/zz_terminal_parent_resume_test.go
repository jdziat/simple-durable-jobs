package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/require"
)

// completeFanOut resumes the waiting parent, and when the parent is not resumable
// it retries on a background goroutine before warning that it is "relying on the
// stalled-parent backstop".
//
// A parent that is already TERMINAL is not "not yet resumable", it is NEVER
// resumable. That is the ordinary steady state with CancelOnFail=false (the
// default): the fan-out settles early on a failure, the parent runs on to a
// terminal status, and every sibling that then finishes NATURALLY arrives here.
// Each one used to drive one inline plus four background ResumeJob writes that
// cannot succeed, and then log a WARN pointing an operator at a stall that does
// not exist and a backstop that will never touch that parent.
//
// Both legs are asserted together so the fix cannot be "never retry": a parent
// that is merely mid-transition must still get its retries.
func TestCompleteFanOut_DoesNotRetryOrWarnForATerminalParent(t *testing.T) {
	newWorkerFor := func(t *testing.T, parentStatus core.JobStatus) (*Worker, *atomic.Int64, *bytes.Buffer) {
		t.Helper()
		var resumeCalls atomic.Int64
		store := &mockStorage{
			resumeJobFunc: func(context.Context, core.UUID) (bool, error) {
				resumeCalls.Add(1)
				return false, nil // never resumable, which is what drives the retries
			},
			getJobFunc: func(_ context.Context, id core.UUID) (*core.Job, error) {
				return &core.Job{ID: id, Type: "parent", Queue: "default", Status: parentStatus}, nil
			},
		}
		w := NewWorker(queue.New(store))
		var buf bytes.Buffer
		w.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		return w, &resumeCalls, &buf
	}

	t.Run("terminal parent: one inline attempt, no retries, no warning", func(t *testing.T) {
		w, calls, buf := newWorkerFor(t, core.StatusCompleted)
		fo := &core.FanOut{ID: core.NewID(), ParentJobID: core.NewID(), TotalCount: 1}

		require.NoError(t, w.completeFanOut(context.Background(), fo, core.FanOutCompleted))
		w.wg.Wait() // any tracked retry goroutine would be joined here

		require.Equal(t, int64(1), calls.Load(),
			"a terminal parent cannot be resumed, so the single inline attempt is all that should ever happen; %d calls means the doomed background retries still run for every late-settling sibling", calls.Load())
		require.NotContains(t, buf.String(), "stalled-parent backstop",
			"warning about a stalled parent for a job that is already terminal sends an operator looking for a stall that does not exist")
	})

	t.Run("non-terminal parent: retries still engage", func(t *testing.T) {
		w, calls, buf := newWorkerFor(t, core.StatusWaiting)
		fo := &core.FanOut{ID: core.NewID(), ParentJobID: core.NewID(), TotalCount: 1}

		require.NoError(t, w.completeFanOut(context.Background(), fo, core.FanOutCompleted))
		w.wg.Wait()

		require.Greater(t, calls.Load(), int64(1),
			"a parent that is merely not-yet-waiting must still get its bounded retries; if this drops to a single attempt the fix has disabled the retry path it was only supposed to narrow")
		require.Contains(t, buf.String(), "stalled-parent backstop",
			"when every retry genuinely fails against a non-terminal parent, the warning is the correct and useful outcome")
	})
}

// A read failure must fall back to retrying rather than skipping the resume: four
// wasted writes are cheap, whereas wrongly skipping strands a waiting parent until
// the stalled-parent backstop notices it.
func TestCompleteFanOut_ParentReadFailureStillRetries(t *testing.T) {
	var resumeCalls atomic.Int64
	store := &mockStorage{
		resumeJobFunc: func(context.Context, core.UUID) (bool, error) {
			resumeCalls.Add(1)
			return false, nil
		},
		getJobFunc: func(context.Context, core.UUID) (*core.Job, error) {
			return nil, context.DeadlineExceeded
		},
	}
	w := NewWorker(queue.New(store))
	var buf bytes.Buffer
	w.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fo := &core.FanOut{ID: core.NewID(), ParentJobID: core.NewID(), TotalCount: 1}
	require.NoError(t, w.completeFanOut(context.Background(), fo, core.FanOutCompleted))
	w.wg.Wait()

	require.Greater(t, resumeCalls.Load(), int64(1),
		"an unreadable parent must be treated as possibly-resumable and retried")
	require.True(t, strings.Contains(buf.String(), "stalled-parent backstop"),
		"and it must still hand off to the backstop rather than dropping the parent silently")
}
