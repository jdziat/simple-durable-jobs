package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

func TestInstrument_RecordsLifecycleMetrics(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(noDepthStorage{})
	Instrument(q, WithMeterProvider(mp))

	startedAt := time.Now().Add(-2 * time.Second)
	job := &core.Job{
		ID:        core.NewID(),
		Type:      "email",
		Queue:     "critical",
		CreatedAt: startedAt.Add(-3 * time.Second),
		StartedAt: &startedAt,
		Attempt:   1,
	}

	q.CallStartHooks(ctx, job)
	q.CallRetryHooks(ctx, job, 1, errors.New("temporary failure"))
	q.CallCompleteHooks(ctx, job)
	q.CallFailHooks(ctx, job, errors.New("terminal failure"))

	rm := collectMetrics(t, reader)

	assertCounterPoint(t, rm, metricJobsStarted, 1, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeStarted,
	})
	assertCounterPoint(t, rm, metricJobsRetried, 1, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeRetried,
	})
	assertCounterPoint(t, rm, metricJobsCompleted, 1, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeCompleted,
	})
	assertCounterPoint(t, rm, metricJobsFailed, 1, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeFailed,
	})
	assertHistogramPoint(t, rm, metricJobWaitDuration, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeStarted,
	})
	assertHistogramPoint(t, rm, metricJobRunDuration, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeCompleted,
	})
	assertHistogramPoint(t, rm, metricJobRunDuration, map[string]string{
		attrQueue: "critical", attrJobType: "email", attrOutcome: outcomeFailed,
	})
}

func TestInstrument_RecordsLeaseReclaimed(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(noDepthStorage{})
	Instrument(q, WithMeterProvider(mp))

	q.CallJobReclaimedHooks(ctx, "job-1", "stale_lock")
	q.CallJobReclaimedHooks(ctx, "job-2", "ownership_audit")

	rm := collectMetrics(t, reader)

	// Each reason lands on its own data point — operators must not sum across
	// reasons (stale_lock = actor/crash indicator, ownership_audit = victim).
	assertCounterPoint(t, rm, metricLeasesReclaimed, 1, map[string]string{
		attrReason: "stale_lock",
	})
	assertCounterPoint(t, rm, metricLeasesReclaimed, 1, map[string]string{
		attrReason: "ownership_audit",
	})
}

func TestInstrument_RecordsQueueDepthGauge(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStore(t)

	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "email", Queue: "alpha"}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "report", Queue: "beta"}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "email", Queue: "alpha"}))
	running, err := store.Dequeue(ctx, []string{"alpha"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, running)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(store)
	Instrument(q, WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertGaugePoint(t, rm, metricQueueDepth, 1, map[string]string{
		attrQueue: "alpha", attrOutcome: outcomePending,
	})
	assertGaugePoint(t, rm, metricQueueDepth, 1, map[string]string{
		attrQueue: "beta", attrOutcome: outcomePending,
	})
	assertGaugePoint(t, rm, metricQueueDepth, 1, map[string]string{
		attrQueue: "alpha", attrOutcome: outcomeRunning,
	})
}

func TestInstrument_RecordsBacklogOldestAgeGauge(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStore(t)
	createdAt := time.Now().Add(-5 * time.Minute)

	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "email", Queue: "alpha", CreatedAt: createdAt}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.NewID(), Type: "email", Queue: "alpha", Status: core.StatusRunning, CreatedAt: createdAt.Add(-time.Hour)}))

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(store)
	Instrument(q, WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertFloatGaugePoint(t, rm, metricBacklogOldestAge, func(value float64) bool {
		return value > 0
	}, map[string]string{
		attrQueue: "alpha",
	})
}

func TestInstrument_RecordsDeadLetterDepthGauge(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStore(t)

	seedDeadLetteredJob(t, store, string(core.NewID()), "alpha")
	seedDeadLetteredJob(t, store, string(core.NewID()), "alpha")
	seedDeadLetteredJob(t, store, string(core.NewID()), "beta")

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(store)
	Instrument(q, WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertGaugePoint(t, rm, metricDeadLetterDepth, 2, map[string]string{
		attrQueue: "alpha",
	})
	assertGaugePoint(t, rm, metricDeadLetterDepth, 1, map[string]string{
		attrQueue: "beta",
	})
}

func TestInstrumentQueueSaturation_RecordsWorkerGauge(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	InstrumentQueueSaturation("worker-1", map[string]int{
		"alpha": 4,
		"beta":  2,
		"zero":  0,
	}, func(context.Context) (map[string]int, error) {
		return map[string]int{
			"alpha": 3,
			"beta":  1,
			"zero":  10,
		}, nil
	}, WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertFloatGaugePoint(t, rm, metricQueueSaturation, func(value float64) bool {
		return value == 0.75
	}, map[string]string{
		attrWorkerID: "worker-1",
		attrQueue:    "alpha",
	})
	assertFloatGaugePoint(t, rm, metricQueueSaturation, func(value float64) bool {
		return value == 0.5
	}, map[string]string{
		attrWorkerID: "worker-1",
		attrQueue:    "beta",
	})
	assertMissingFloatGaugePoint(t, rm, metricQueueSaturation, map[string]string{
		attrWorkerID: "worker-1",
		attrQueue:    "zero",
	})
}

func TestInstrumentWorkerDequeue_RecordsChurnCounters(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	InstrumentWorkerDequeue("worker-1",
		func() map[string]int64 {
			return map[string]int64{"fleet_rate": 8, "fleet_rate_cached": 191, "queue_cap": 0}
		},
		func() int64 { return 29 },
		WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertCounterPoint(t, rm, metricDequeueReleased, 8, map[string]string{
		attrWorkerID: "worker-1",
		attrReason:   "fleet_rate",
	})
	assertCounterPoint(t, rm, metricDequeueReleased, 191, map[string]string{
		attrWorkerID: "worker-1",
		attrReason:   "fleet_rate_cached",
	})
	assertCounterPoint(t, rm, metricDequeueSuppressedTicks, 29, map[string]string{
		attrWorkerID: "worker-1",
		attrReason:   "fleet_rate_saturated",
	})
}

func TestInstrumentWorkerRateSaturation_RecordsGauge(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	InstrumentWorkerRateSaturation("worker-1", func() int64 { return 7 }, WithMeterProvider(mp))

	rm := collectMetrics(t, reader)
	assertGaugePoint(t, rm, metricRateSaturationCacheSize, 7, map[string]string{
		attrWorkerID: "worker-1",
	})
}

func TestInstrument_SkipsQueueDepthGaugeWithoutCapability(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	q := queue.New(noDepthStorage{})
	require.NotPanics(t, func() {
		Instrument(q, WithMeterProvider(mp))
	})

	rm := collectMetrics(t, reader)
	assert.Nil(t, findMetric(rm, metricQueueDepth))
}

func TestNewPrometheusHandler(t *testing.T) {
	handler, mp, err := NewPrometheusHandler()
	require.NoError(t, err)
	require.NotNil(t, handler)
	require.NotNil(t, mp)
	defer func() { _ = mp.Shutdown(context.Background()) }()
}

func newSQLiteStore(t *testing.T) *storage.GormStorage {
	t.Helper()
	dsn := t.TempDir() + "/jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(context.Background()))
	return store
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, scope := range rm.ScopeMetrics {
		for i := range scope.Metrics {
			if scope.Metrics[i].Name == name {
				return &scope.Metrics[i]
			}
		}
	}
	return nil
}

func assertCounterPoint(t *testing.T, rm metricdata.ResourceMetrics, name string, value int64, attrs map[string]string) {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %s not found", name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %s has type %T", name, m.Data)
	for _, point := range sum.DataPoints {
		if attributesMatch(point.Attributes, attrs) {
			assert.Equal(t, value, point.Value)
			return
		}
	}
	t.Fatalf("metric %s missing data point with attributes %v", name, attrs)
}

func assertGaugePoint(t *testing.T, rm metricdata.ResourceMetrics, name string, value int64, attrs map[string]string) {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %s not found", name)
	gauge, ok := m.Data.(metricdata.Gauge[int64])
	require.True(t, ok, "metric %s has type %T", name, m.Data)
	for _, point := range gauge.DataPoints {
		if attributesMatch(point.Attributes, attrs) {
			assert.Equal(t, value, point.Value)
			return
		}
	}
	t.Fatalf("metric %s missing data point with attributes %v", name, attrs)
}

func assertFloatGaugePoint(t *testing.T, rm metricdata.ResourceMetrics, name string, match func(float64) bool, attrs map[string]string) {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %s not found", name)
	gauge, ok := m.Data.(metricdata.Gauge[float64])
	require.True(t, ok, "metric %s has type %T", name, m.Data)
	for _, point := range gauge.DataPoints {
		if attributesMatch(point.Attributes, attrs) {
			assert.True(t, match(point.Value), "metric %s value %v did not match expectation", name, point.Value)
			return
		}
	}
	t.Fatalf("metric %s missing data point with attributes %v", name, attrs)
}

func assertMissingFloatGaugePoint(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs map[string]string) {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %s not found", name)
	gauge, ok := m.Data.(metricdata.Gauge[float64])
	require.True(t, ok, "metric %s has type %T", name, m.Data)
	for _, point := range gauge.DataPoints {
		if attributesMatch(point.Attributes, attrs) {
			t.Fatalf("metric %s had unexpected data point with attributes %v", name, attrs)
		}
	}
}

func assertHistogramPoint(t *testing.T, rm metricdata.ResourceMetrics, name string, attrs map[string]string) {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %s not found", name)
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "metric %s has type %T", name, m.Data)
	for _, point := range hist.DataPoints {
		if attributesMatch(point.Attributes, attrs) {
			assert.Equal(t, uint64(1), point.Count)
			assert.Greater(t, point.Sum, 0.0)
			return
		}
	}
	t.Fatalf("metric %s missing histogram point with attributes %v", name, attrs)
}

func attributesMatch(set attribute.Set, expected map[string]string) bool {
	for key, expectedValue := range expected {
		actual, ok := set.Value(attribute.Key(key))
		if !ok || actual.AsString() != expectedValue {
			return false
		}
	}
	return true
}

type noDepthStorage struct {
	core.Storage
}

func seedDeadLetteredJob(t *testing.T, store *storage.GormStorage, id, queueName string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: core.UUID(id), Type: "work", Queue: queueName, MaxRetries: 1}))
	job, err := store.Dequeue(ctx, []string{queueName}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, job)
	require.NoError(t, store.Fail(ctx, job.ID, "worker-1", "boom", nil))
}
