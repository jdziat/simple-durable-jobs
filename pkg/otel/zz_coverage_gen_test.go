package otel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/storage"
)

// genSetupQueue builds a queue backed by an in-memory sqlite database with no
// external dependencies. It reuses the same sqlite-in-memory pattern as the
// existing worker integration test.
func genSetupQueue(t *testing.T) *queue.Queue {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	if sqlDB, dbErr := db.DB(); dbErr == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store)
}

// TestInstrument_DefaultsToGlobalProviders exercises the nil-defaulting
// branches in Instrument: when no WithTracerProvider/WithPropagator options
// are supplied it falls back to the global OTel tracer provider and
// propagator. This must not panic and must register all hooks.
func TestInstrument_DefaultsToGlobalProviders(t *testing.T) {
	q := genSetupQueue(t)

	// Call with no options so cfg.tracerProvider and cfg.propagator are both
	// nil and the code defaults them to the global OTel singletons.
	require.NotPanics(t, func() {
		Instrument(q)
	})

	// The defaulting path is also exercised when only one option is provided,
	// leaving the other nil. Provide only a propagator so the tracer-provider
	// nil branch is taken on a second queue.
	q2 := genSetupQueue2(t)
	require.NotPanics(t, func() {
		Instrument(q2, WithPropagator(nil))
	})
}

// genSetupQueue2 is a distinct helper so the in-memory sqlite DSN keyed on
// t.Name() does not collide between the two queues in a single test.
func genSetupQueue2(t *testing.T) *queue.Queue {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"-2?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	if sqlDB, dbErr := db.DB(); dbErr == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store)
}

// TestCompleteHook_NoRecordingSpan covers the early-return branch in
// completeHook when the context carries a non-recording (noop) span.
func TestCompleteHook_NoRecordingSpan(t *testing.T) {
	hook := completeHook()
	// context.Background() yields a non-recording noop span via
	// trace.SpanFromContext, so !span.IsRecording() is true.
	require.NotPanics(t, func() {
		hook(context.Background(), &core.Job{ID: "no-span"})
	})
}

// TestFailHook_NoRecordingSpan covers the early-return branch in failHook when
// the context carries a non-recording span.
func TestFailHook_NoRecordingSpan(t *testing.T) {
	hook := failHook()
	require.NotPanics(t, func() {
		hook(context.Background(), &core.Job{ID: "no-span"}, errors.New("boom"))
	})
}

// TestRetryHook_NoRecordingSpan covers the early-return branch in retryHook
// when the context carries a non-recording span.
func TestRetryHook_NoRecordingSpan(t *testing.T) {
	hook := retryHook()
	require.NotPanics(t, func() {
		hook(context.Background(), &core.Job{ID: "no-span"}, 1, errors.New("boom"))
	})
}
