package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSlotSweeper is a minimal concurrencySlotSweepStorage for unit-testing
// deleteExpiredConcurrencySlots without a full storage backend.
type fakeSlotSweeper struct {
	called bool
	cutoff time.Time
	ret    int64
	err    error
}

func (f *fakeSlotSweeper) DeleteExpiredConcurrencySlots(ctx context.Context, cutoff time.Time) (int64, error) {
	f.called = true
	f.cutoff = cutoff
	return f.ret, f.err
}

func newLogCapture() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})), &buf
}

func TestDeleteExpiredConcurrencySlots_Success(t *testing.T) {
	logger, buf := newLogCapture()
	w := &Worker{logger: logger}
	sweeper := &fakeSlotSweeper{ret: 3}

	w.deleteExpiredConcurrencySlots(context.Background(), sweeper)

	require.True(t, sweeper.called, "the sweep must call storage")
	assert.False(t, sweeper.cutoff.IsZero(), "a cutoff time is passed")
	assert.Empty(t, buf.String(), "a successful sweep logs nothing")
}

func TestDeleteExpiredConcurrencySlots_GenericErrorIsLogged(t *testing.T) {
	logger, buf := newLogCapture()
	w := &Worker{logger: logger}
	sweeper := &fakeSlotSweeper{err: errors.New("db exploded")}

	w.deleteExpiredConcurrencySlots(context.Background(), sweeper)

	assert.Contains(t, buf.String(), "concurrency slot GC pass failed", "a real error is warn-logged")
}

func TestDeleteExpiredConcurrencySlots_ContextErrorsAreSwallowed(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, buf := newLogCapture()
			w := &Worker{logger: logger}
			sweeper := &fakeSlotSweeper{err: tc.err}

			w.deleteExpiredConcurrencySlots(context.Background(), sweeper)

			assert.Empty(t, strings.TrimSpace(buf.String()),
				"shutdown-driven context errors must not be logged as failures")
		})
	}
}
