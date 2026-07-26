package jobs_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

// Queue.NewWorker takes ...any because the facade cannot name
// worker.WorkerOption without an import cycle. Every argument that was not a
// WorkerOption — a queue.Option, an option from the wrong constructor, a bare
// value — was therefore dropped in silence, leaving the worker on defaults with
// no indication anything had been ignored.
//
// FALSE-GREEN TRAP: asserting that NewWorker returns non-nil passes with the bug
// fully present, because dropping the argument is exactly what it used to do and
// the worker is still constructed. The discriminating observation is the log
// record: it must name the offending argument's POSITION and CONCRETE TYPE, so
// an operator can find it among several.
func TestNewWorker_LogsDiscardedNonOptionArguments(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN(t.TempDir()+"/argcheck.db")),
		&gorm.Config{Logger: glogger.Default.LogMode(glogger.Silent)})
	require.NoError(t, err)
	store := jobs.NewGormStorage(db)
	q := jobs.New(store)

	// A real option, then two things that are not worker options at all.
	w := q.NewWorker(
		jobs.WithPollInterval(50*time.Millisecond),
		"not-an-option",
		42,
	)
	require.NotNil(t, w, "a bad argument must not prevent construction — this is a warning, not a panic")

	out := buf.String()
	assert.Equal(t, 2, strings.Count(out, "ignoring argument to NewWorker"),
		"one record per discarded argument, so a caller passing several sees all of them")
	assert.Contains(t, out, "type=string", "the record must name the concrete type that was ignored")
	assert.Contains(t, out, "type=int")
	assert.Contains(t, out, "index=1", "and its position, so it can be found among several arguments")
	assert.Contains(t, out, "index=2")
}

// TestNewWorker_SilentWhenAllArgumentsAreOptions is the negative control: the
// warning must not fire for correct usage, or it becomes noise operators filter
// out — which would make it worthless for the case it exists to catch.
func TestNewWorker_SilentWhenAllArgumentsAreOptions(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN(t.TempDir()+"/argcheck2.db")),
		&gorm.Config{Logger: glogger.Default.LogMode(glogger.Silent)})
	require.NoError(t, err)
	q := jobs.New(jobs.NewGormStorage(db))

	w := q.NewWorker(jobs.WithPollInterval(50 * time.Millisecond))
	require.NotNil(t, w)

	assert.NotContains(t, buf.String(), "ignoring argument to NewWorker",
		"correct usage must stay silent")
}
