package worker

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
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
}

func TestLogStorageCapabilitiesSilentOnGormStorage(t *testing.T) {
	_, store := newWorkerRetentionStore(t)
	var buf lockedLogBuffer
	q := queue.New(store)
	w := NewWorker(q, WithOwnershipAuditInterval(0))
	w.logger = slog.New(slog.NewTextHandler(&buf, nil))

	w.logStorageCapabilities()

	assert.NotContains(t, buf.String(), "storage backend missing optional capabilities; some features inactive")
}
