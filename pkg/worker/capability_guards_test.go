package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

var _ scheduledFireReader = (*storage.GormStorage)(nil)
var _ scheduledFireSeeder = (*storage.GormStorage)(nil)
var _ completeWithResultStorage = (*storage.GormStorage)(nil)
var _ completablePendingFanOutStorage = (*storage.GormStorage)(nil)
var _ failTerminalWithResultStorage = (*storage.GormStorage)(nil)
var _ batchDequeuer = (*storage.GormStorage)(nil)
var _ perQueueDequeuer = (*storage.GormStorage)(nil)
var _ concurrencySlotStorage = (*storage.GormStorage)(nil)
var _ rateLimiterStorage = (*storage.GormStorage)(nil)
var _ retentionStorage = (*storage.GormStorage)(nil)
var _ uniqueLockSweepStorage = (*storage.GormStorage)(nil)
var _ concurrencySlotSweepStorage = (*storage.GormStorage)(nil)
var _ consumedSignalRetentionStorage = (*storage.GormStorage)(nil)
var _ signalResumeStorage = (*storage.GormStorage)(nil)
var _ signalResumePager = (*storage.GormStorage)(nil)
var _ pendingSignalNameReader = (*storage.GormStorage)(nil)
var _ recoveryLeaser = (*storage.GormStorage)(nil)
var _ fanOutSuspendStorage = (*storage.GormStorage)(nil)

func TestGormStorageSatisfiesAllGuardedInterfaces(t *testing.T) {
	var s core.Storage = (*storage.GormStorage)(nil)

	checks := []struct {
		name string
		ok   bool
	}{
		{"scheduledFireReader", implements[scheduledFireReader](s)},
		{"scheduledFireSeeder", implements[scheduledFireSeeder](s)},
		{"completeWithResultStorage", implements[completeWithResultStorage](s)},
		{"completablePendingFanOutStorage", implements[completablePendingFanOutStorage](s)},
		{"failTerminalWithResultStorage", implements[failTerminalWithResultStorage](s)},
		{"batchDequeuer", implements[batchDequeuer](s)},
		{"perQueueDequeuer", implements[perQueueDequeuer](s)},
		{"concurrencySlotStorage", implements[concurrencySlotStorage](s)},
		{"rateLimiterStorage", implements[rateLimiterStorage](s)},
		{"retentionStorage", implements[retentionStorage](s)},
		{"uniqueLockSweepStorage", implements[uniqueLockSweepStorage](s)},
		{"concurrencySlotSweepStorage", implements[concurrencySlotSweepStorage](s)},
		{"consumedSignalRetentionStorage", implements[consumedSignalRetentionStorage](s)},
		{"signalResumeStorage", implements[signalResumeStorage](s)},
		{"signalResumePager", implements[signalResumePager](s)},
		{"pendingSignalNameReader", implements[pendingSignalNameReader](s)},
		{"recoveryLeaser", implements[recoveryLeaser](s)},
	}

	for _, check := range checks {
		require.True(t, check.ok, "GormStorage should implement %s", check.name)
	}
}

func implements[T any](s core.Storage) bool {
	_, ok := s.(T)
	return ok
}

func TestLogStorageCapabilitiesLogsInactiveOnFakeStorage(t *testing.T) {
	var buf lockedLogBuffer
	q := queue.New(&mockStorage{})
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.logStorageCapabilities()

	out := buf.String()
	assert.Contains(t, out, "storage backend missing optional capabilities; some features inactive")
	assert.Contains(t, out, "batch-dequeue")
	assert.Contains(t, out, "recovery-lease")
	assert.Contains(t, out, "atomic-fanout-suspend")
}

// TestWarnDegradedStorageDurability_WarnsForNonAtomicStorage (PKT-12) proves the
// worker loudly warns at startup when a custom storage lacks a durability-critical
// atomic capability: the fan-out warning fires unconditionally, and the
// scheduled-fire (data-loss) warning fires when schedules are configured.
func TestWarnDegradedStorageDurability_WarnsForNonAtomicStorage(t *testing.T) {
	var buf lockedLogBuffer
	q := queue.New(&mockStorage{}) // lacks SuspendForFanOut and TxEnqueuer
	q.Register("sched-task", func(_ context.Context, _ struct{}) error { return nil })
	require.NoError(t, q.Schedule("sched-task", nil, fixedSchedule{}))
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.warnDegradedStorageDurability(q.Storage())

	out := buf.String()
	assert.Contains(t, out, "DEGRADED DURABILITY")
	assert.Contains(t, out, "SuspendForFanOut", "must warn that fan-out suspend is non-atomic")
	assert.Contains(t, out, "scheduled-fire", "must warn about the lossy scheduled-fire fallback when schedules are configured")
}

// TestWarnDegradedStorageDurability_ScheduledFireWarnGatedOnSchedules proves the
// scheduled-fire (data-loss) warning is SUPPRESSED when no schedules are
// configured, even on a non-atomic storage — the fan-out warning still fires
// (it is unconditional). Guards the len(GetScheduledJobs())>0 gate.
func TestWarnDegradedStorageDurability_ScheduledFireWarnGatedOnSchedules(t *testing.T) {
	var buf lockedLogBuffer
	q := queue.New(&mockStorage{}) // non-atomic, but NO schedules registered
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.warnDegradedStorageDurability(q.Storage())

	out := buf.String()
	assert.NotContains(t, out, "scheduled-fire", "no schedules configured → no scheduled-fire data-loss warning")
	assert.Contains(t, out, "SuspendForFanOut", "the fan-out warning is unconditional and must still fire")
}

// TestWarnDegradedStorageDurability_SilentOnGormStorage proves the default
// GormStorage (both atomic paths) triggers no durability warning.
func TestWarnDegradedStorageDurability_SilentOnGormStorage(t *testing.T) {
	_, store := newWorkerRetentionStore(t)
	var buf lockedLogBuffer
	q := queue.New(store)
	q.Register("sched-task", func(_ context.Context, _ struct{}) error { return nil })
	require.NoError(t, q.Schedule("sched-task", nil, fixedSchedule{}))
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.warnDegradedStorageDurability(q.Storage())

	assert.NotContains(t, buf.String(), "DEGRADED DURABILITY")
}

// fixedSchedule is a minimal schedule.Schedule for the tests above.
type fixedSchedule struct{}

func (fixedSchedule) Next(time.Time) time.Time { return time.Now().Add(time.Hour) }

func TestLogStorageCapabilitiesSilentOnGormStorage(t *testing.T) {
	_, store := newWorkerRetentionStore(t)
	var buf lockedLogBuffer
	q := queue.New(store)
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.logStorageCapabilities()

	assert.NotContains(t, buf.String(), "storage backend missing optional capabilities; some features inactive")
}
