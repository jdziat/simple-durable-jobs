package metrics

import (
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// TestDroppedEventsCounter (PKT-13 / Claude C2) proves jobs.events.dropped is
// registered and observes the queue's cumulative dropped-event total (a single
// scalar with no attributes), so silently-dropped lifecycle events are now
// observable.
func TestDroppedEventsCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter(instrumentationName)

	registerDroppedEventsCounter(meter, func() uint64 { return 7 })

	rm := collectMetrics(t, reader)
	assertCounterPoint(t, rm, metricEventsDropped, 7, nil)
}
