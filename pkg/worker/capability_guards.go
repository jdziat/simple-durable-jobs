package worker

import (
	"fmt"
	"strings"

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
var _ concurrencySlotRenewer = (*storage.GormStorage)(nil)
var _ rateLimiterStorage = (*storage.GormStorage)(nil)
var _ rateReleaser = (*storage.GormStorage)(nil)
var _ windowedRateLimiter = (*storage.GormStorage)(nil)
var _ retentionStorage = (*storage.GormStorage)(nil)
var _ uniqueLockSweepStorage = (*storage.GormStorage)(nil)
var _ concurrencySlotSweepStorage = (*storage.GormStorage)(nil)
var _ consumedSignalRetentionStorage = (*storage.GormStorage)(nil)
var _ signalResumeStorage = (*storage.GormStorage)(nil)
var _ signalResumePager = (*storage.GormStorage)(nil)
var _ pendingSignalNameReader = (*storage.GormStorage)(nil)
var _ recoveryLeaser = (*storage.GormStorage)(nil)

func (w *Worker) logStorageCapabilities() {
	s := w.queue.Storage()
	inactive := make([]string, 0, 18)

	if _, ok := s.(scheduledFireReader); !ok {
		inactive = append(inactive, "durable-timers/scheduled-fire-read")
	}
	if _, ok := s.(scheduledFireSeeder); !ok {
		inactive = append(inactive, "durable-timers/scheduled-fire-seed")
	}
	if _, ok := s.(completeWithResultStorage); !ok {
		inactive = append(inactive, "result-on-complete")
	}
	if _, ok := s.(completablePendingFanOutStorage); !ok {
		inactive = append(inactive, "fanout-pending-recovery")
	}
	if _, ok := s.(fanOutSuspendStorage); !ok {
		// FanOut() falls back to the legacy non-atomic 4-write suspend; see the
		// DEGRADED DURABILITY warning from validateConfiguredStorageCapabilities.
		inactive = append(inactive, "atomic-fanout-suspend")
	}
	if _, ok := s.(failTerminalWithResultStorage); !ok {
		inactive = append(inactive, "result-on-terminal-fail")
	}
	if _, ok := s.(batchDequeuer); !ok {
		inactive = append(inactive, "batch-dequeue")
	}
	if _, ok := s.(perQueueDequeuer); !ok {
		inactive = append(inactive, "per-queue-dequeue")
	}
	if _, ok := s.(concurrencySlotStorage); !ok {
		inactive = append(inactive, "fleet-concurrency-caps")
	}
	if _, ok := s.(concurrencySlotRenewer); !ok {
		inactive = append(inactive, "concurrency-slot-renew")
	}
	if _, ok := s.(rateLimiterStorage); !ok {
		inactive = append(inactive, "rate-limits")
	}
	if _, ok := s.(retentionStorage); !ok {
		inactive = append(inactive, "retention")
	}
	if _, ok := s.(uniqueLockSweepStorage); !ok {
		inactive = append(inactive, "unique-lock-sweep")
	}
	if _, ok := s.(concurrencySlotSweepStorage); !ok {
		inactive = append(inactive, "concurrency-slot-sweep")
	}
	if _, ok := s.(consumedSignalRetentionStorage); !ok {
		inactive = append(inactive, "consumed-signal-retention")
	}
	if _, ok := s.(signalResumeStorage); !ok {
		inactive = append(inactive, "signal-resume")
	}
	if _, ok := s.(signalResumePager); !ok {
		inactive = append(inactive, "signal-resume-paging")
	}
	if _, ok := s.(pendingSignalNameReader); !ok {
		inactive = append(inactive, "pending-signal-name-read")
	}
	if _, ok := s.(recoveryLeaser); !ok {
		inactive = append(inactive, "recovery-lease")
	}

	if len(inactive) == 0 {
		return
	}
	w.logger.Info("storage backend missing optional capabilities; some features inactive",
		"storage", fmt.Sprintf("%T", s),
		"inactive", strings.Join(inactive, ","))
}
