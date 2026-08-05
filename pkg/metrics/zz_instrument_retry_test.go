package metrics

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// failingMeter cannot construct instruments. WithMeterProvider accepts the public
// metric.MeterProvider interface, so any custom or bridging provider can behave
// this way; the stock SDK provider happens not to.
type failingMeter struct{ metric.Meter }

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errors.New("meter unavailable")
}

type failingProvider struct{ embedded.MeterProvider }

func (failingProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingMeter{Meter: noop.NewMeterProvider().Meter("")}
}

// A first Instrument whose instrument construction FAILS registers no hooks and no
// gauges, so it must not consume the once-guard.
//
// Before the fix, LoadOrStore claimed the queue before the fallible step and the
// error path returned without releasing it. The operator's retry with a working
// provider was then swallowed as a duplicate — and the warning said "called more
// than once", pointing away from the real cause. The queue emitted nothing for the
// life of the process, unrecoverable short of building a new Queue.
func TestInstrument_RetryAfterAFailedProviderStillInstruments(t *testing.T) {
	ctx := context.Background()
	q := queue.New(noDepthStorage{})

	Instrument(q, WithMeterProvider(failingProvider{}))

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()
	Instrument(q, WithMeterProvider(mp))

	q.CallStartHooks(ctx, &core.Job{
		ID: core.NewID(), Type: "email", Queue: "critical", Attempt: 1,
	})

	rm := collectMetrics(t, reader)
	assertCounterPoint(t, rm, metricJobsStarted, 1, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeStarted,
	})
}
