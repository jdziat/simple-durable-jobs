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

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
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
		ID:        "job-1",
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

	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: "pending-alpha", Type: "email", Queue: "alpha"}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: "pending-beta", Type: "report", Queue: "beta"}))
	require.NoError(t, store.Enqueue(ctx, &core.Job{ID: "running-alpha", Type: "email", Queue: "alpha"}))
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
