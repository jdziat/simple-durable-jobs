package benchmarks

import (
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestMain silences the default logger. A worker captures slog.Default() when it
// is constructed and logs a "retention GC enabled" line on every start; the default
// handler writes through the standard log package to the same stream `go test`
// prints results on. Go prints a benchmark's name before the run starts, so a log
// line landing in between splits the record and benchstat drops the sample. Every
// worker is built after TestMain returns control, so the discard handler is in
// place before any of them captures the logger.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}
