package otel

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

// TestRecordSanitizedError_RedactsSecrets (PKT-13 / Claude E2) proves error text
// recorded on a span is run through security.SanitizeErrorMessage, so secrets in
// raw error messages are redacted before reaching any exporter.
func TestRecordSanitizedError_RedactsSecrets(t *testing.T) {
	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer(instrumentationName)

	_, span := tracer.Start(context.Background(), "test.span")
	recordSanitizedError(span, errors.New("connect failed pwd=topsecret retrying"))
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	s := spans[0]
	assert.Equal(t, codes.Error, s.Status.Code)
	assert.Contains(t, s.Status.Description, "[REDACTED]")
	assert.NotContains(t, s.Status.Description, "topsecret", "span status must not leak the secret")
	for _, ev := range s.Events {
		for _, a := range ev.Attributes {
			assert.NotContains(t, a.Value.AsString(), "topsecret", "recorded exception must not leak the secret")
		}
	}
}

// TestInstrument_SecondCallIsNoop (PKT-13 / C2) proves calling Instrument twice on
// the same Queue does not double-register the enqueue middleware (which would leak
// duplicate enqueue spans).
func TestInstrument_SecondCallIsNoop(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	q := queue.New(store)
	q.Register("x", func(context.Context, struct{}) error { return nil })

	tp, exporter := setupTracer()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	Instrument(q, WithTracerProvider(tp))
	Instrument(q, WithTracerProvider(tp)) // second call must be a no-op

	_, err = q.Enqueue(context.Background(), "x", struct{}{})
	require.NoError(t, err)

	enqueueSpans := 0
	for _, s := range exporter.GetSpans() {
		if strings.Contains(strings.ToLower(s.Name), "enqueue") {
			enqueueSpans++
		}
	}
	assert.Equal(t, 1, enqueueSpans, "a second Instrument must not double-register enqueue middleware")
}
