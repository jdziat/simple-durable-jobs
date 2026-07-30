package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

// Worker processes jobs from the queue.
type Worker struct {
	queue  *queue.Queue
	config WorkerConfig
	logger *slog.Logger
	// wg covers background goroutines (scheduler, reaper, sweeps, retention, ...).
	wg sync.WaitGroup
	// handlerWG covers ONLY in-flight job-handler goroutines (processLoop), so
	// shutdown can drain handlers on a bounded timeout without waiting unbounded on
	// a ctx-ignoring handler, and without conflating them with background loops.
	handlerWG sync.WaitGroup
	// forcedHandlerDrainGrace bounds the post-force-cancel drain wait; set from
	// defaultForcedHandlerDrainGrace in NewWorker (tests may shrink it per-instance).
	forcedHandlerDrainGrace time.Duration

	batchCompleter *batchCompleter

	// Pause state
	started   atomic.Bool
	paused    atomic.Bool
	pauseMode atomic.Value // stores core.PauseMode
	// shuttingDown is set once the worker begins draining on ctx cancellation, so
	// a handler cancelled by shutdown is released back to pending (clean handoff)
	// rather than charged a retry attempt.
	shuttingDown atomic.Bool

	// Running job cancellation (for aggressive pause)
	// runningJobs maps a job id to the run currently executing it. The value
	// carries a per-run token because a job can legitimately be running under a
	// LATER run while an earlier one is still unwinding: the aggressive-pause path
	// releases to `pending` while this worker is still polling, so the same job can
	// be re-dequeued before the first run's deferred cleanup fires.
	runningJobs map[core.UUID]runningJobEntry
	// nextRunToken issues the per-run identity above.
	nextRunToken atomic.Uint64
	// pauseCancelled marks jobs whose handler context was cancelled by
	// Pause(PauseModeAggressive) rather than by a genuine failure or shutdown.
	// Guarded by runningJobsMu, which already guards the cancel funcs the pause
	// invokes, so the mark and the cancel cannot be observed out of order.
	// pauseCancelled holds the RUN TOKENS whose handler an aggressive pause
	// cancelled — keyed by RUN, not by job id.
	//
	// Two runs of the same job id can be alive at once now that the pause path
	// releases to `pending` while this worker still polls, and a map keyed by job
	// id is a single shared slot between them — whichever run reads or writes it
	// last wins, so one run steals or clobbers the other's mark and that run's
	// cancellation falls through to the ordinary failure path and burns an
	// attempt. Both directions of that were reproduced before this was keyed by
	// run. A token is unique to one run, so there is nothing to share.
	pauseCancelled map[uint64]struct{}
	runningJobsMu  sync.Mutex

	// Per-queue concurrency tracking
	queueRunning map[string]*atomic.Int32 // queue name -> active count
	// queueJobID and slotJobID are keyed by RUN TOKEN, not job id, for the same
	// reason pauseCancelled is: the pause path releases to `pending` while this
	// worker still polls, so two runs of ONE job id can be alive at once. Keyed by
	// job id they are a single shared slot — run #2 overwrites run #1's entry, and
	// whichever cleans up first deletes it, so the other's decrement or release
	// never happens. Measured: a permanent +1 leak on the per-queue counter (which
	// eventually bounces 100% of that queue's work while the worker looks healthy)
	// and a fleet concurrency slot released out from under a still-running handler,
	// which under-counts the cap and admits an extra concurrent job.
	queueJobID   map[uint64]string // run token -> queue name (for decrement on completion)
	queueJobIDMu sync.Mutex

	// DB-backed concurrency slots acquired for dequeued jobs. Only populated
	// when the storage backend implements concurrencySlotStorage.
	//
	// The value carries the job id as well as the slot names, because the DATABASE
	// row is keyed (slot_name, job_id) while this map is keyed by run token: two
	// runs of one job id SHARE a row, so deciding whether a release may delete it
	// requires asking "does any OTHER token still hold this job id?", which needs
	// the job id on the value side. See releaseConcurrencySlots.
	slotJobIDMu sync.Mutex
	slotJobID   map[uint64]slotHold // run token -> the row that run holds

	// Per-worker queue rate-limit buckets. Only populated for queues configured
	// with WithQueueRateLimit.
	queueRateBuckets map[string]*tokenBucket

	// cto-F2 saturation-feedback throttle. When a fleet RateLimit denies a job
	// (tryConsumeRateLimits), the dispatch loop would otherwise keep CLAIMING
	// jobs every poll tick only to bounce+Release them — write amplification with
	// no progress. markRateSaturated records, per effective limit name, the time
	// the limit's current window rolls over; dequeueSlots consults it and claims
	// nothing while a binding limit is saturated, collapsing the churn to one
	// probe batch per window. This is a CLAIM-RATE damper only: the unchanged
	// TryConsumeRate DB gate remains the sole admission authority, so it can only
	// ever cause FEWER claims, never an extra admit.
	//
	// allRateLimitsUnkeyed is precomputed: suppression fires ONLY when every
	// configured RateLimit is unkeyed, because a keyed limit's effective name
	// needs the held job (RateLimitKey), so it cannot be pre-gated before a claim.
	// With any keyed limit, behavior is identical to before this throttle.
	rateSaturationMu     sync.Mutex
	rateSaturatedUntil   map[string]time.Time
	allRateLimitsUnkeyed bool

	// dequeueChurn counts dispatch bounces by reason and rate-saturation
	// suppressed ticks — the observability signals for the cto-F2 claim->release
	// churn (a healthy throttled worker shows suppressedTicks rising while
	// released{fleet_rate} stays low: one probe batch per window). Exposed via
	// DequeueReleasedByReason/DequeueSuppressedTicks and wired into OTel by
	// pkg/metrics.InstrumentWorkerDequeue.
	dequeueChurn dequeueChurnCounters

	rateLimitStorageMissingLogged       atomic.Bool
	retentionStorageMissingLogged       atomic.Bool
	retentionUnconfiguredLogged         atomic.Bool
	retentionConfiguredLogged           atomic.Bool
	uniqueLockStorageMissingLogged      atomic.Bool
	slotSweepStorageMissingLogged       atomic.Bool
	readyPromoterStorageMissingLogged   atomic.Bool
	batchCompletionStorageMissingLogged atomic.Bool

	// futureSleepSuppressions memoizes (jobID -> run_at) for sleeping jobs the
	// signal-resume backstop has already inspected, capping checkpoint lookups
	// at one per sleeper per sleep. Entries are pruned once their run_at
	// passes and cleared on resume; worst case, a job that leaves waiting
	// out-of-band (e.g. cancelled mid-sleep) holds its ~tens-of-bytes entry
	// until the original deadline expires.
	futureSleepMu           sync.Mutex
	futureSleepSuppressions map[core.UUID]int64

	// heartbeatInterval is the tick rate for runHeartbeat. Defaults to
	// 2 minutes; tests override with a sub-second value. Not exposed via
	// WorkerConfig because changing it in production would change lock
	// contention semantics — the 2-minute default is paired with the
	// 45-minute lock expiry assumed elsewhere.
	heartbeatInterval time.Duration
}

type scheduledFireReader interface {
	GetScheduledFireTime(context.Context, string) (time.Time, bool, error)
}

// scheduledFireSeeder is implemented by storage backends that can establish a
// shared fire-boundary anchor for a fresh schedule (insert-if-absent). It lets
// every worker in a fleet derive the same first boundary, so skewed wall clocks
// can't double-fire the first tick. Optional.
type scheduledFireSeeder interface {
	SeedScheduledFire(ctx context.Context, name string, anchor time.Time) (time.Time, error)
}

type completeWithResultStorage interface {
	CompleteWithResult(ctx context.Context, jobID core.UUID, workerID string, result []byte) (*core.FanOut, error)
}

type batchCompleteStorage interface {
	BatchComplete(ctx context.Context, workerID string, items []storage.BatchCompleteItem) ([]core.UUID, error)
}

// completablePendingFanOutStorage is implemented by backends that can find
// fan-outs left status='pending' with terminal counts and a waiting parent —
// the post-crash strand the recovery poll heals by routing each row through the
// same completeFanOut path the live worker uses. Optional: backends without it
// simply skip the backstop (they still benefit from the in-tx status advance).
type completablePendingFanOutStorage interface {
	GetCompletablePendingFanOuts(ctx context.Context, olderThan time.Time) ([]*core.FanOut, error)
}

// abandonedFanOutStorage is implemented by backends that can reconcile pending
// fan_outs abandoned mid-creation under an already-terminal parent.
type abandonedFanOutStorage interface {
	CleanAbandonedFanOuts(ctx context.Context, olderThan time.Time) (int64, error)
}

type failTerminalWithResultStorage interface {
	FailTerminalWithResult(ctx context.Context, jobID core.UUID, workerID, errMsg string) (*core.FanOut, error)
}

type batchDequeuer interface {
	DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error)
}

type perQueueDequeuer interface {
	DequeueBatchPerQueue(ctx context.Context, workerID string, budgets map[string]int) ([]*core.Job, error)
}

type concurrencySlotStorage interface {
	TryAcquireConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, workerID string, limit int, ttl time.Duration) (bool, error)
	ReleaseConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID) error
}

// concurrencySlotOwnedReleaser is the ownership-fenced release. It is a SEPARATE
// optional capability rather than a change to concurrencySlotStorage because
// external storage implementations satisfy these interfaces structurally, and
// widening an existing method's signature would break them at compile time —
// impossible inside v4. Storages that provide it get the fence; those that do
// not keep the old unfenced behaviour.
type concurrencySlotOwnedReleaser interface {
	ReleaseConcurrencySlotOwned(ctx context.Context, slotName string, jobID core.UUID, workerID string) error
}

type concurrencySlotRenewer interface {
	RenewConcurrencySlot(ctx context.Context, slotName string, jobID core.UUID, ttl time.Duration) (bool, error)
}

type rateLimiterStorage interface {
	TryConsumeRate(ctx context.Context, limitName string, perSecond float64, window time.Duration, now time.Time) (bool, error)
}

type retentionStorage interface {
	DeleteTerminalJobsOlderThan(ctx context.Context, status core.JobStatus, age time.Duration, limit int) (int64, error)
}

type uniqueLockSweepStorage interface {
	DeleteExpiredUniqueLocks(ctx context.Context, limit int) (int64, error)
}

type concurrencySlotSweepStorage interface {
	DeleteExpiredConcurrencySlots(ctx context.Context, cutoff time.Time) (int64, error)
}

type readyPromoterStorage interface {
	PromoteReadyJobs(ctx context.Context) (int64, error)
}

// consumedSignalRetentionStorage is implemented by backends that can prune
// consumed signal rows; the retention sweep uses it when the opt-in
// consumed-signal window is set. Optional — absent backends are skipped.
type consumedSignalRetentionStorage interface {
	DeleteConsumedSignalsOlderThan(ctx context.Context, age time.Duration, limit int) (int64, error)
}

// signalResumeStorage is implemented by backends that buffer signals; the
// recovery poll uses it to wake jobs whose awaited signal has arrived or whose
// wait deadline has passed. Optional — backends without it simply don't poll.
type signalResumeStorage interface {
	GetSignalWaitingJobsToResume(ctx context.Context) ([]*core.Job, error)
	// ResumeSignalWaitingJob resumes a waiting (never paused) job and clears its
	// timeout wake deadline. Distinct from ResumeJob so the signal backstop can't
	// un-pause an operator-paused job or strip a delayed job's schedule.
	ResumeSignalWaitingJob(ctx context.Context, jobID core.UUID) (bool, error)
}

type signalResumePager interface {
	GetSignalWaitingJobsToResumeAfter(ctx context.Context, afterJobID core.UUID, limit int) ([]*core.Job, error)
}

// pendingSignalNameReader lets the resume backstop distinguish a genuine
// signal wake (unconsumed signal present) from a durable-timer deadline wake,
// and best-effort name the signal for the JobResumedBySignal event. Optional.
type pendingSignalNameReader interface {
	GetPendingSignalName(ctx context.Context, jobID core.UUID) (name string, ok bool, err error)
}

// recoveryLeaser is implemented by storage backends that can elect a single
// worker to run the fleet-wide fan-out recovery scan. Optional: backends that
// don't implement it fall back to every worker polling.
type recoveryLeaser interface {
	TryAcquireRecoveryLease(ctx context.Context, name, owner string, ttl time.Duration) (bool, error)
}

const (
	// recoveryLeaseName is the lease key for the fan-out recovery poll.
	recoveryLeaseName = "fanout-recovery"
	// recoveryLeaseTTL must exceed the recovery poll interval so the current
	// holder keeps renewing across ticks; if the holder dies, the lease fails
	// over to another worker within one TTL.
	recoveryLeaseTTL          = 15 * time.Second
	defaultConcurrencySlotTTL = 45 * time.Minute
	// checkpointWriteTimeout bounds a cancellation-immune checkpoint write. The
	// handler activity has already run by the time we persist the checkpoint, so
	// the write must land even if the per-job deadline/cancellation just fired —
	// otherwise the side effect re-runs on replay. Mirrors the 5s detached
	// budget used by releaseDequeuedJobOnShutdown (worker.go:447).
	checkpointWriteTimeout = 5 * time.Second
	healthCheckTimeout     = 5 * time.Second
	maxDrainIterations     = 1000
)

// defaultForcedHandlerDrainGrace bounds the wait AFTER handlers are force-cancelled
// at shutdown, so a handler that ignores its context cannot hang shutdown forever.
// The live value is a per-Worker field (Worker.forcedHandlerDrainGrace) so tests
// can shrink it on their own instance without racing other workers.
const defaultForcedHandlerDrainGrace = 5 * time.Second

// ErrWorkerAlreadyStarted is returned by Start when a worker is already running.
// Start admits exactly one live run; call it again only after the previous run's
// context has been cancelled and Start has returned.
var ErrWorkerAlreadyStarted = errors.New("jobs: worker already started")

var signalResumePollBatchSize = 100

// NewWorker creates a new worker for the given queue.
func NewWorker(q *queue.Queue, opts ...WorkerOption) *Worker {
	config := WorkerConfig{
		Queues:       nil, // Will be set to default if no queue options provided
		PollInterval: 100 * time.Millisecond,
		WorkerID:     string(core.NewID()),
		DrainTimeout: 30 * time.Second,
		// Claim up to this many jobs per dequeue round-trip. The claim is a single
		// statement per queue (UPDATE ... RETURNING on PG/SQLite), so a larger batch
		// amortizes round-trips without extra per-row cost. It never widens the
		// claimed-not-running window: dequeueSlots caps the claim at the worker's
		// free concurrency slots, so a worker at the default concurrency (10) is
		// unaffected — only deployments that raise Concurrency() claim larger batches.
		DequeueBatchSize: 50,
	}

	for _, opt := range opts {
		opt.ApplyWorker(&config)
	}

	// If no queues configured, use default
	if config.Queues == nil {
		n := 10
		if config.topLevelConcurrency != nil {
			n = *config.topLevelConcurrency
		}
		config.Queues = map[string]int{"default": n}
	}

	// Set default retry configs if not specified
	if config.StorageRetry == nil {
		defaultCfg := DefaultRetryConfig()
		config.StorageRetry = &defaultCfg
	}
	if config.DequeueRetry == nil {
		// Use longer backoff for dequeue to avoid hammering DB during outages
		dequeueCfg := RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    500 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
			JitterFraction:    0.2,
		}
		config.DequeueRetry = &dequeueCfg
	}
	// Clamp a non-positive attempt count to a single try. MaxAttempts COUNTS the
	// initial attempt, so 0 — the zero value of a hand-built RetryConfig, and what
	// WithRetryAttempts(0) used to install verbatim — meant "never call the
	// operation": retryWithBackoff returned nil without writing anything and every
	// caller read that as a successful write, turning the worker into a simulator
	// that fires completion hooks for jobs still 'running' in the DB. The nil
	// checks above only substitute a default for an ABSENT config, never for a
	// present-but-nonsensical one. Clamp onto a COPY: the pointer may be shared
	// with an option value the caller reuses across workers, and mutating it in
	// place would be a cross-worker write. 1 preserves DisableRetry()'s meaning.
	if config.StorageRetry.MaxAttempts < 1 {
		clamped := *config.StorageRetry
		clamped.MaxAttempts = 1
		config.StorageRetry = &clamped
	}
	if config.DequeueRetry.MaxAttempts < 1 {
		clamped := *config.DequeueRetry
		clamped.MaxAttempts = 1
		config.DequeueRetry = &clamped
	}

	// Set default stale lock reaper cadence. The reaper always runs (it
	// recovers jobs from crashed workers and cannot be disabled), so a
	// non-positive interval simply falls back to the 5m default.
	if config.StaleLockInterval <= 0 {
		config.StaleLockInterval = 5 * time.Minute
	}
	// Default the ready-promoter cadence to the poll interval. Dequeue requires
	// dq_ready=true and the promoter is the only path that flips a future-dated
	// job true, so a promoter slower than the poll would add dequeue latency to
	// delayed/scheduled jobs and short RetryAfter backoffs; matching the poll
	// keeps their visibility latency at ~one poll, as before dq_ready existed.
	if config.ReadyPromoteInterval <= 0 {
		config.ReadyPromoteInterval = config.PollInterval
	}
	if config.ReadyPromoteInterval <= 0 {
		config.ReadyPromoteInterval = 100 * time.Millisecond
	}
	// Guard <= 0, not just == 0: a NEGATIVE StaleLockAge would invert the reaper
	// cutoff into the future (now - negative = future), so the reaper would
	// reclaim every running job each tick — wholesale double-execution from one
	// config typo (teardown g9). Mirror FanOutRecoveryStaleAge's <= 0 guard.
	if config.StaleLockAge <= 0 {
		config.StaleLockAge = 45 * time.Minute
	}
	if config.FanOutRecoveryStaleAge <= 0 {
		config.FanOutRecoveryStaleAge = 2 * time.Minute
	}
	if !config.retentionSet && !config.Retention.enabled() {
		config.Retention = RetentionConfig{
			CompletedAfter:       defaultRetentionCompletedAfter,
			FailedAfter:          defaultRetentionFailedAfter,
			ConsumedSignalsAfter: defaultRetentionConsumedSignalsAfter,
			Interval:             defaultRetentionInterval,
			BatchSize:            defaultRetentionBatchSize,
		}
	}

	// Clamp the heartbeat interval below StaleLockAge. The reaper now reclaims a
	// job from its LAST CONTACT — COALESCE(last_heartbeat_at, started_at,
	// locked_until) < now-StaleLockAge — instead of from the (stacked) lease, so
	// last_heartbeat_at must be refreshed several times within a StaleLockAge
	// window or a live, not-yet-heartbeated job would anchor on started_at and
	// could be falsely reclaimed and double-run. runHeartbeat ticks once per
	// interval with NO immediate first beat, so the unprotected window is one
	// full interval; keeping interval <= StaleLockAge/3 guarantees ~3 beats land
	// before the stale window elapses. The default StaleLockAge (45m → 15m) leaves
	// the 2m default untouched; the chaos harness's 2s StaleLockAge drives it to
	// ~667ms, comfortably below 2s, and the 200ms floor keeps a sub-second
	// (documented-unsupported) StaleLockAge from hammering the DB.
	hbInterval := 2 * time.Minute
	if maxHB := config.StaleLockAge / 3; maxHB > 0 && maxHB < hbInterval {
		hbInterval = maxHB
	}
	if hbInterval < 200*time.Millisecond {
		hbInterval = 200 * time.Millisecond
	}

	if config.MaxRetryBackoff <= 0 {
		config.MaxRetryBackoff = time.Minute
	}

	// Propagate lock duration to the storage backend if supported.
	// The storage must implement SetLockDuration(time.Duration) for this to take effect.
	if config.LockDuration > 0 {
		type lockDurationSetter interface {
			SetLockDuration(time.Duration)
		}
		if setter, ok := q.Storage().(lockDurationSetter); ok {
			setter.SetLockDuration(config.LockDuration)
		}
	}

	// Propagate the opt-in checkpoint-on-complete GC to the storage backend if
	// supported. Default off: the dashboard reads completed jobs' checkpoints, so
	// only an explicit RetentionDeleteCheckpointsOnComplete() flips it on. Backends
	// without the setter (no GormStorage) silently ignore the opt-in.
	if config.Retention.DeleteCheckpointsOnComplete {
		type checkpointGCSetter interface {
			SetDeleteCheckpointsOnComplete(bool)
		}
		if setter, ok := q.Storage().(checkpointGCSetter); ok {
			setter.SetDeleteCheckpointsOnComplete(true)
		}
	}

	// Initialize per-queue concurrency counters
	queueRunning := make(map[string]*atomic.Int32, len(config.Queues))
	for name := range config.Queues {
		queueRunning[name] = &atomic.Int32{}
	}
	queueRateBuckets := make(map[string]*tokenBucket, len(config.QueueRateLimits))
	now := time.Now()
	for name, limit := range config.QueueRateLimits {
		if bucket := newTokenBucket(limit.PerSecond, limit.Burst, now); bucket != nil {
			queueRateBuckets[name] = bucket
		}
	}

	// Default OwnershipAuditInterval to 5s only when it was never set. An
	// explicit WithOwnershipAuditInterval(0) is honored as "disable" (the
	// Start guard below skips the goroutine); without the ownershipAuditSet
	// flag we couldn't tell that apart from "unset".
	if !config.ownershipAuditSet && config.OwnershipAuditInterval == 0 {
		config.OwnershipAuditInterval = 5 * time.Second
	}

	// The cto-F2 whole-fleet dequeue suppression only applies when EVERY
	// configured fleet RateLimit is unkeyed — a keyed limit's effective name needs
	// the held job, so it can't be pre-gated before a claim (see dequeueSlots).
	// The per-key cooldown (markRateSaturated/keyedRateSaturated) applies to keyed
	// limits too; it only removes the DB rate-check, never suppresses the claim.
	allRateLimitsUnkeyed := len(config.RateLimits) > 0
	for _, limit := range config.RateLimits {
		if limit.Key != nil {
			allRateLimitsUnkeyed = false
			break
		}
	}
	if config.rateSaturationCap <= 0 {
		config.rateSaturationCap = defaultRateSaturationCap
	}

	return &Worker{
		queue:                   q,
		config:                  config,
		logger:                  slog.Default(),
		forcedHandlerDrainGrace: defaultForcedHandlerDrainGrace,
		runningJobs:             make(map[core.UUID]runningJobEntry),
		pauseCancelled:          make(map[uint64]struct{}),
		queueRunning:            queueRunning,
		queueJobID:              make(map[uint64]string),
		slotJobID:               make(map[uint64]slotHold),
		queueRateBuckets:        queueRateBuckets,
		rateSaturatedUntil:      make(map[string]time.Time),
		allRateLimitsUnkeyed:    allRateLimitsUnkeyed,
		futureSleepSuppressions: make(map[core.UUID]int64),
		heartbeatInterval:       hbInterval,
	}
}

// Start begins processing jobs. Blocks until context is cancelled.
// Per-queue concurrency is enforced: each queue only dequeues up to its
// configured concurrency limit.
// The dispatcher drains available work within each poll interval; setting
// DequeueBatchSize to 1 with a slow PollInterval approximates the legacy
// once-per-tick dispatch behavior.
func (w *Worker) Start(ctx context.Context) error {
	// Real re-entry guard: reject a concurrent/overlapping Start so two run loops
	// cannot race this worker's shared maps/counters. A CAS (not a plain Store)
	// makes the check-and-set atomic.
	if !w.started.CompareAndSwap(false, true) {
		return ErrWorkerAlreadyStarted
	}
	defer w.started.Store(false)
	// Reset the drain flag AFTER the CAS (so a rejected double-Start can't clear a
	// live run's flag) so a restarted worker (Start → cancel → Start on a fresh
	// ctx) begins clean — otherwise a stale shuttingDown=true would misroute a
	// later non-shutdown context.Canceled (e.g. aggressive pause) to release.
	w.shuttingDown.Store(false)

	if err := w.validateConfiguredStorageCapabilities(); err != nil {
		return err
	}
	w.logStorageCapabilities()

	totalConcurrency := 0
	for _, c := range w.config.Queues {
		totalConcurrency += c
	}

	jobsChan := make(chan dispatchedJob, totalConcurrency)
	handlerBase, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHandlers()

	// Start scheduler if enabled
	if w.config.EnableScheduler {
		w.goTracked(func() { w.runScheduler(ctx) })
	}

	// Start polling for waiting jobs (fan-out fallback)
	w.goTracked(func() { w.pollWaitingJobs(ctx) })

	// Start the stale-lock reaper to reclaim jobs whose owning worker died.
	// This always runs — it's the only recovery path for crashed workers, so
	// it cannot be disabled (NewWorker guarantees a positive interval).
	w.goTracked(func() { w.reapStaleLocks(ctx) })

	// Start ownership audit to cancel local handlers for jobs cancelled
	// or reclaimed by other workers. Same-worker cancellation is handled
	// directly by completeFanOut/reapStaleLocks; this is the cross-worker
	// counterpart.
	if w.config.OwnershipAuditInterval > 0 {
		w.goTracked(func() { w.runOwnershipAudit(ctx) })
	}

	if w.config.UniqueLockSweep.enabled() {
		w.goTracked(func() { w.runUniqueLockSweep(ctx) })
	}

	w.goTracked(func() { w.runConcurrencySlotSweep(ctx) })

	// Ready-promoter backstop: makes delayed/scheduled jobs dequeue-visible once
	// their run_at passes (and heals any missed dq_ready flag). Always runs.
	w.goTracked(func() { w.runReadyPromoter(ctx) })

	if w.config.Retention.enabled() {
		w.logRetentionConfigured()
		w.goTracked(func() { w.runRetention(ctx) })
	} else {
		w.warnIfRetentionUnconfigured()
	}

	if w.config.BatchCompletion.Enabled {
		if batchStorage, ok := w.queue.Storage().(batchCompleteStorage); ok {
			cfg := w.config.BatchCompletion
			w.batchCompleter = newBatchCompleter(context.WithoutCancel(ctx), batchStorage, w.config.WorkerID, cfg.MaxBatch, cfg.MaxDelay, *w.config.StorageRetry, w.logger)
			w.goTracked(func() { w.batchCompleter.run() })
		} else if w.batchCompletionStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support batch completion; falling back to per-job completion",
				"storage", fmt.Sprintf("%T", w.queue.Storage()))
		}
	}

	for i := 0; i < totalConcurrency; i++ {
		w.handlerWG.Add(1)
		go w.processLoop(handlerBase, jobsChan)
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Mark shutdown so any handler we cancel below is released back to
			// pending for immediate reclaim, not failed-with-retry.
			w.shuttingDown.Store(true)
			close(jobsChan)
			if w.batchCompleter != nil {
				w.batchCompleter.Close()
			}
			// Phase 1: graceful drain of in-flight handlers (skipped when
			// DrainTimeout<=0). A clean drain returns immediately.
			if w.config.DrainTimeout > 0 && !w.waitHandlers(w.config.DrainTimeout) {
				w.logger.Warn("worker drain timeout reached; cancelling in-flight handlers",
					"in_flight", w.RunningJobCount(),
					"drain_timeout", w.config.DrainTimeout)
			}
			// Phase 2: force-cancel any still-running handlers, then BOUND the wait
			// so a handler that ignores its context cannot hang shutdown forever.
			cancelHandlers()
			if !w.waitHandlers(w.forcedHandlerDrainGrace) {
				w.logger.Error("in-flight handlers did not exit within the forced-drain grace; abandoning them",
					"in_flight", w.RunningJobCount(),
					"grace", w.forcedHandlerDrainGrace)
			}
			// Background goroutines all select on ctx.Done() (already fired); wait
			// for them unbounded so none is abandoned mid-DB-write.
			w.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			w.drainDequeuedJobs(ctx, jobsChan, totalConcurrency)
		}
	}
}

func (w *Worker) validateConfiguredStorageCapabilities() error {
	storage := w.queue.Storage()
	if len(w.config.rateLimitOptionErrors) > 0 {
		// A RateLimit(...) was configured with invalid args (empty name / non-positive
		// rate). Fail loudly at Start rather than run with a silently-absent limit.
		return fmt.Errorf("invalid RateLimit option(s): %w", errors.Join(w.config.rateLimitOptionErrors...))
	}
	if count := len(w.config.ConcurrencyCaps); count > 0 {
		if _, ok := storage.(concurrencySlotStorage); !ok {
			return fmt.Errorf("worker has %d ConcurrencyCap(s) configured but storage %T does not support DB-backed concurrency slots; the cap would be silently ignored", count, storage)
		}
	}
	if count := len(w.config.RateLimits); count > 0 {
		if _, ok := storage.(rateLimiterStorage); !ok {
			return fmt.Errorf("worker has %d RateLimit(s) configured but storage %T does not support DB-backed rate limiting; the rate limit would be silently ignored", count, storage)
		}
	}
	w.warnDegradedStorageDurability(storage)
	return nil
}

// fanOutSuspendStorage is the atomic fan-out suspend capability (mirrors
// pkg/fanout's suspender). When a storage lacks it, FanOut() uses the legacy
// non-atomic 4-write fallback with a wider crash window.
type fanOutSuspendStorage interface {
	SuspendForFanOut(ctx context.Context, parentID core.UUID, workerID string, fanOut *core.FanOut, checkpoint *core.Checkpoint, subJobs []*core.Job) error
}

// warnDegradedStorageDurability loudly warns at startup when a CUSTOM storage
// lacks a durability-critical atomic capability that a used/usable feature
// depends on. Unlike concurrency/rate caps (silently IGNORED → hard fail), these
// fallbacks still function, just with a crash-durability penalty — so warn rather
// than fail. GormStorage implements both atomic paths, so neither warning fires
// for the default storage. See docs/content/docs/storage-durability.md.
func (w *Worker) warnDegradedStorageDurability(storage core.Storage) {
	// Scheduled fires: the non-atomic fallback can LOSE a fire on a crash between
	// claiming the boundary and enqueuing the job. Only relevant when schedules
	// are actually configured.
	if len(w.queue.GetScheduledJobs()) > 0 && !w.queue.SupportsAtomicScheduledFire() {
		w.logger.Warn("DEGRADED DURABILITY: scheduled jobs are configured but storage lacks atomic "+
			"scheduled-fire enqueue (ScheduledFireTxClaimer + TxEnqueuer); a fire can be LOST if this "+
			"worker crashes between claiming the fire boundary and enqueuing the job. Use GormStorage "+
			"(or a TxEnqueuer-capable storage) for at-least-once scheduled fires.",
			"storage", fmt.Sprintf("%T", storage))
	}
	// Fan-out: without atomic suspend, a crash mid-suspend leaves a running+locked
	// parent until the stale-lock reaper reclaims it — recoverable, but a wider
	// window than the atomic path. Fan-out is a runtime feature, so warn whenever
	// the capability is absent (any handler may call FanOut).
	if _, ok := storage.(fanOutSuspendStorage); !ok {
		w.logger.Warn("DEGRADED DURABILITY: storage lacks atomic fan-out suspend (SuspendForFanOut); "+
			"FanOut() uses the legacy non-atomic fallback with a wider crash window (a crash mid-suspend "+
			"leaves a running+locked parent until the stale-lock reaper reclaims it — recoverable, but "+
			"wider than the atomic path). Use GormStorage for atomic fan-out suspend.",
			"storage", fmt.Sprintf("%T", storage))
	}
}

func (w *Worker) drainDequeuedJobs(ctx context.Context, jobsChan chan<- dispatchedJob, totalConcurrency int) {
	deadline := time.Now().Add(w.config.PollInterval)
	initialQueues := w.queuesWithCapacity()
	releaseBudget := w.dequeueSlots(initialQueues, totalConcurrency)
	if releaseBudget <= 0 {
		return
	}

	releasedThisTick := 0
	for iteration := 0; iteration < maxDrainIterations; iteration++ {
		if ctx.Err() != nil {
			return
		}
		if w.IsPaused() {
			return
		}

		availableQueues := w.queuesWithCapacity()
		if len(availableQueues) == 0 {
			return
		}

		slots := w.dequeueSlots(availableQueues, totalConcurrency)
		if slots <= 0 {
			return
		}

		jobs, err := w.dequeueAvailableJobs(ctx, availableQueues, totalConcurrency)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to dequeue after retries", "error", err)
			}
			return
		}

		// Re-check pause AFTER the dequeue round-trip. The top-of-iteration check
		// (above) only sees a pause that landed BEFORE the iteration; a Pause() that
		// lands DURING dequeueAvailableJobs — a window widened by network latency on
		// Postgres/MySQL — would otherwise let this worker DISPATCH jobs it claimed
		// after being told to pause (e.g. a job enqueued immediately after Pause()
		// slips through). A graceful pause must stop new dispatch, so release the
		// just-claimed batch back to pending (for resume, or another worker) and end
		// the tick rather than dispatch it.
		if w.IsPaused() {
			w.releaseClaimedJobs(ctx, jobs)
			return
		}

		dispatched, released := w.dispatchDequeuedJobs(ctx, jobsChan, jobs)
		releasedThisTick += released
		if dispatched == 0 {
			return
		}
		if releasedThisTick >= releaseBudget {
			return
		}
		if !time.Now().Before(deadline) {
			return
		}
	}
}

func (w *Worker) dequeueAvailableJobs(ctx context.Context, availableQueues []string, totalConcurrency int) ([]*core.Job, error) {
	slots := w.dequeueSlots(availableQueues, totalConcurrency)
	if slots <= 0 {
		return nil, nil
	}
	if slots > 1 {
		if pqd, ok := w.queue.Storage().(perQueueDequeuer); ok {
			return w.dequeueBatchPerQueueWithRetry(ctx, pqd, w.dequeueQueueBudgets(availableQueues, slots), slots)
		}
		if bd, ok := w.queue.Storage().(batchDequeuer); ok {
			return w.dequeueBatchWithRetry(ctx, bd, availableQueues, slots)
		}
	}
	job, err := w.dequeueWithRetry(ctx, availableQueues)
	if err != nil || job == nil {
		return nil, err
	}
	return []*core.Job{job}, nil
}

// Dispatch accounting invariant: totalConcurrency is the sum of per-queue caps
// and also the jobsChan buffer size. dequeueSlots is only an upper-bound
// estimate and may over-claim because RunningJobCount lags jobs already
// buffered in jobsChan. The real caps are enforced downstream: per-queue by the
// queueRunning CAS in tryTrackQueueJob, named limits by
// tryAcquireConcurrencySlots, and total concurrency by the bounded channel. Any
// job that fails admission is released back to storage, and releaseBudget bounds
// per-tick claim/release churn.
func (w *Worker) dequeueSlots(availableQueues []string, totalConcurrency int) int {
	if len(availableQueues) == 0 {
		return 0
	}
	slots := totalConcurrency - w.RunningJobCount()
	if slots <= 0 {
		return 0
	}
	if w.config.DequeueBatchSize > 0 && slots > w.config.DequeueBatchSize {
		slots = w.config.DequeueBatchSize
	}
	queueCapacity := w.totalCapacityAcrossQueues(availableQueues)
	if slots > queueCapacity {
		slots = queueCapacity
	}
	if slots < 0 {
		return 0
	}
	// cto-F2: if every configured fleet RateLimit is unkeyed and at least one is
	// currently saturated, claiming is futile — a claimed job would pass the
	// cheap in-memory gates, be denied by the DB rate gate, and be Released
	// straight back to pending (write amplification, no progress). Skip the claim
	// this tick; markRateSaturated set the suppress-until to the window rollover,
	// so a probe batch re-fires precisely when headroom can return. This only
	// ever REDUCES claims (the unchanged TryConsumeRate gate is still the sole
	// admission authority), and never fires for keyed/mixed configs.
	if w.allRateLimitsUnkeyed && w.unkeyedRateLimitsSaturated(time.Now()) {
		// Count one suppressed tick. dequeueSlots runs up to three times per
		// drainDequeuedJobs (top-of-drain budget, in-loop, and via
		// dequeueAvailableJobs), but a fully-saturated tick increments this
		// exactly once: the top-of-drain call returns a 0 budget and
		// drainDequeuedJobs returns before the loop's calls run. (A tick that
		// only turns saturated mid-loop is armed by tryConsumeRateLimits after
		// those calls, so it still counts at most once.) The drain loop is
		// single-goroutine, so there is no cross-tick double count.
		w.dequeueChurn.suppressedTicks.Add(1)
		return 0
	}
	return slots
}

func (w *Worker) totalCapacityAcrossQueues(queues []string) int {
	total := 0
	for _, name := range queues {
		maxConcurrency, ok := w.config.Queues[name]
		if !ok {
			continue
		}
		used := 0
		if counter, ok := w.queueRunning[name]; ok {
			used = int(counter.Load())
		}
		if remaining := maxConcurrency - used; remaining > 0 {
			total += remaining
		}
	}
	return total
}

func (w *Worker) dequeueQueueBudgets(queues []string, limit int) map[string]int {
	budgets := make(map[string]int, len(queues))
	if limit <= 0 {
		return budgets
	}

	type queueCapacity struct {
		name      string
		remaining int
	}
	capacities := make([]queueCapacity, 0, len(queues))
	for _, name := range queues {
		maxConcurrency, ok := w.config.Queues[name]
		if !ok {
			continue
		}
		used := 0
		if counter, ok := w.queueRunning[name]; ok {
			used = int(counter.Load())
		}
		if remaining := maxConcurrency - used; remaining > 0 {
			capacities = append(capacities, queueCapacity{name: name, remaining: remaining})
		}
	}
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].name < capacities[j].name
	})

	remainingLimit := limit
	if remainingLimit >= len(capacities) {
		for i := range capacities {
			budgets[capacities[i].name] = 1
			capacities[i].remaining--
			remainingLimit--
		}
	}
	for remainingLimit > 0 {
		progressed := false
		for i := range capacities {
			if remainingLimit <= 0 {
				break
			}
			if capacities[i].remaining <= 0 {
				continue
			}
			budgets[capacities[i].name]++
			capacities[i].remaining--
			remainingLimit--
			progressed = true
		}
		if !progressed {
			break
		}
	}
	return budgets
}

func (w *Worker) dispatchDequeuedJobs(ctx context.Context, jobsChan chan<- dispatchedJob, jobs []*core.Job) (dispatched int, released int) {
	for _, job := range jobs {
		if job == nil {
			continue
		}
		// The run token is allocated HERE, not in processJob, because admission
		// state (the per-queue counter and the concurrency slots) is registered on
		// this side of the channel and has to be keyed by the same run that will
		// later release it.
		runToken := w.nextRunToken.Add(1)
		if !w.tryTrackQueueJob(runToken, job.Queue) {
			w.recordBounce(bounceQueueCap)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
			continue
		}
		if !w.tryConsumeQueueRateLimit(job.Queue) {
			w.recordBounce(bounceQueueRate)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
			continue
		}
		if ok := w.tryAcquireConcurrencySlots(ctx, job, runToken); !ok {
			w.recordBounce(bounceConcurrency)
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
			continue
		}
		if ctx.Err() != nil {
			w.recordBounce(bounceShutdown)
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
			continue
		}
		if ok, reason := w.tryConsumeRateLimits(ctx, job); !ok {
			w.recordBounce(reason) // fleet_rate (paid the DB tx) or fleet_rate_cached (cooldown skip)
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
			continue
		}

		select {
		case jobsChan <- dispatchedJob{job: job, token: runToken}:
			dispatched++
		case <-ctx.Done():
			w.recordBounce(bounceShutdown)
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			released++
		}
	}
	return dispatched, released
}

// releaseClaimedJobs releases a batch of jobs that were dequeued (claimed +
// locked) but NOT yet admitted/dispatched — e.g. when a pause lands during the
// dequeue round-trip. Unlike releaseDequeuedJobOnShutdown these jobs hold no
// queue/concurrency tracking yet (that happens in dispatchDequeuedJobs), so a
// plain storage Release back to pending is sufficient. Best-effort: a failed
// release leaves the job locked until the stale-lock reaper reclaims it.
func (w *Worker) releaseClaimedJobs(ctx context.Context, jobs []*core.Job) {
	for _, job := range jobs {
		if job == nil {
			continue
		}
		w.recordBounce(bouncePaused)
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := w.queue.Storage().Release(releaseCtx, job.ID, w.config.WorkerID); err != nil && !errors.Is(err, core.ErrJobNotOwned) {
			w.logger.Warn("failed to release job claimed while pausing",
				"job_id", job.ID, "error", err)
		}
		cancel()
	}
}

func (w *Worker) releaseDequeuedJobOnShutdown(ctx context.Context, job *core.Job, runToken uint64) {
	// TWO budgets, not one shared between them. These are independent pieces of
	// cleanup — the job row and the fleet-cap rows — and neither should be able to
	// starve the other.
	//
	// Sharing one context meant a slow job Release consumed the whole 5s, after
	// which every slot DELETE ran on an already-Done parent. releaseSlotNames
	// divides slotReleaseTimeout among the slots, but it divides a CONSTANT rather
	// than the parent's REMAINING budget, so `WithTimeout(donePparent, perSlot)`
	// returns children that are already expired and NOT ONE row is deleted. That is
	// the same defect the per-slot division was written to fix, moved one layer up,
	// and strictly worse: the earlier version leaked slots 2..N, this leaked all of
	// them.
	//
	// The path that matters is graceful shutdown, which releases every in-flight
	// job at once — so the connection pool is at its most contended exactly when
	// Release is asked to finish inside 5s, and database/sql charges pool wait to
	// the same context.
	jobCtx, cancelJob := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelJob()

	if err := w.queue.Storage().Release(jobCtx, job.ID, w.config.WorkerID); err != nil && !errors.Is(err, core.ErrJobNotOwned) {
		w.logger.Warn("failed to release dequeued job during shutdown",
			"job_id", job.ID,
			"error", err)
	}

	slotCtx, cancelSlots := context.WithTimeout(context.WithoutCancel(ctx), w.slotReleaseBudget())
	defer cancelSlots()
	w.releaseConcurrencySlots(slotCtx, job.ID, runToken)
	w.untrackQueueJob(runToken)
}

// waitHandlers waits up to timeout for all in-flight job handlers to finish,
// returning true if they drained and false on timeout. It waits ONLY on
// handlerWG (job handlers), never on background goroutines.
func (w *Worker) waitHandlers(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		w.handlerWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (w *Worker) goTracked(fn func()) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}

// warnIfRetentionUnconfigured emits exactly one WARN per worker process when an
// operator explicitly disables the retention sweep.
func (w *Worker) warnIfRetentionUnconfigured() {
	if w.retentionUnconfiguredLogged.CompareAndSwap(false, true) {
		w.logger.Warn("retention is disabled; completed/failed/cancelled job rows and consumed signals accumulate forever")
	}
}

func (w *Worker) logRetentionConfigured() {
	if w.retentionConfiguredLogged.CompareAndSwap(false, true) {
		cfg := w.config.Retention
		w.logger.Info("retention GC enabled",
			"completed_after", cfg.CompletedAfter,
			"failed_after", cfg.FailedAfter,
			"consumed_signals_after", cfg.ConsumedSignalsAfter,
			"disable_hint", "disable with jobs.RetentionDisabled()")
	}
}

func (w *Worker) runRetention(ctx context.Context) {
	jobStorage, jobOK := w.queue.Storage().(retentionStorage)
	signalStorage, signalOK := w.queue.Storage().(consumedSignalRetentionStorage)
	if !jobOK && !signalOK {
		if w.retentionStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support retention GC; retention disabled")
		}
		return
	}

	cfg := w.config.Retention
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultRetentionInterval
	}
	if interval < minRetentionInterval {
		interval = minRetentionInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultRetentionBatchSize
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runRetentionOnce(ctx, jobStorage, signalStorage, cfg, batchSize)
		}
	}
}

func (w *Worker) runRetentionOnce(ctx context.Context, jobStorage retentionStorage, signalStorage consumedSignalRetentionStorage, cfg RetentionConfig, batchSize int) {
	if jobStorage != nil && cfg.CompletedAfter > 0 {
		w.deleteTerminalStatus(ctx, jobStorage, core.StatusCompleted, cfg.CompletedAfter, batchSize)
	}
	if jobStorage != nil && cfg.FailedAfter > 0 {
		w.deleteTerminalStatus(ctx, jobStorage, core.StatusFailed, cfg.FailedAfter, batchSize)
		w.deleteTerminalStatus(ctx, jobStorage, core.StatusCancelled, cfg.FailedAfter, batchSize)
	}
	if signalStorage != nil && cfg.ConsumedSignalsAfter > 0 {
		w.deleteConsumedSignals(ctx, signalStorage, cfg.ConsumedSignalsAfter, batchSize)
	}
}

func (w *Worker) deleteTerminalStatus(ctx context.Context, storage retentionStorage, status core.JobStatus, age time.Duration, batchSize int) {
	for ctx.Err() == nil {
		deleted, err := storage.DeleteTerminalJobsOlderThan(ctx, status, age, batchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Warn("retention GC pass failed", "status", status, "error", err)
			}
			return
		}
		if deleted < int64(batchSize) {
			return
		}
	}
}

func (w *Worker) deleteConsumedSignals(ctx context.Context, storage consumedSignalRetentionStorage, age time.Duration, batchSize int) {
	for ctx.Err() == nil {
		deleted, err := storage.DeleteConsumedSignalsOlderThan(ctx, age, batchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Warn("retention GC consumed-signal pass failed", "error", err)
			}
			return
		}
		if deleted < int64(batchSize) {
			return
		}
	}
}

func (w *Worker) runUniqueLockSweep(ctx context.Context) {
	storage, ok := w.queue.Storage().(uniqueLockSweepStorage)
	if !ok {
		if w.uniqueLockStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support unique lock GC; windowed enqueue deduplication lock sweep disabled")
		}
		return
	}

	cfg := w.config.UniqueLockSweep
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultUniqueLockSweepInterval
	}
	if interval < minUniqueLockSweepInterval {
		interval = minUniqueLockSweepInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultUniqueLockSweepBatch
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.deleteExpiredUniqueLocks(ctx, storage, batchSize)
		}
	}
}

func (w *Worker) deleteExpiredUniqueLocks(ctx context.Context, storage uniqueLockSweepStorage, batchSize int) {
	for ctx.Err() == nil {
		deleted, err := storage.DeleteExpiredUniqueLocks(ctx, batchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Warn("unique lock GC pass failed", "error", err)
			}
			return
		}
		if deleted < int64(batchSize) {
			return
		}
	}
}

func (w *Worker) runConcurrencySlotSweep(ctx context.Context) {
	storage, ok := w.queue.Storage().(concurrencySlotSweepStorage)
	if !ok {
		if w.slotSweepStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support concurrency slot GC; expired concurrency-cap slot sweep disabled")
		}
		return
	}

	ticker := time.NewTicker(defaultUniqueLockSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.deleteExpiredConcurrencySlots(ctx, storage)
		}
	}
}

func (w *Worker) deleteExpiredConcurrencySlots(ctx context.Context, storage concurrencySlotSweepStorage) {
	_, err := storage.DeleteExpiredConcurrencySlots(ctx, time.Now().UTC())
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.Warn("concurrency slot GC pass failed", "error", err)
	}
}

// runReadyPromoter is the wedge-backstop for the dq_ready dequeue hint: it
// periodically flips dq_ready=true for pending jobs that have become eligible
// (run_at passed) but are still flagged not-ready — delayed/scheduled jobs
// reaching their run_at, or any job a write path failed to flag. dq_ready is a
// pure performance hint (Dequeue still gates on dq_eligible_at <= now), so this
// loop only ever affects dequeue LATENCY, never correctness; running it
// per-worker is safe and idempotent. It cannot be disabled because it is the
// only mechanism that makes a delayed job dequeue-visible.
func (w *Worker) runReadyPromoter(ctx context.Context) {
	storage, ok := w.queue.Storage().(readyPromoterStorage)
	if !ok {
		if w.readyPromoterStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support ready promotion; delayed jobs rely on per-write dq_ready flagging only")
		}
		return
	}

	base := w.config.ReadyPromoteInterval
	for {
		// ±10% per-worker jitter so a fleet of workers doesn't fire the promotion
		// UPDATE in lockstep and contend on the same newly-eligible rows every
		// tick. The promoter is already capped + SKIP LOCKED, but staggering
		// further reduces herd contention under a thundering-herd schedule.
		d := base
		if base > 0 {
			d = base + readyPromoteJitter(base)
			if d <= 0 {
				d = base
			}
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			w.promoteReadyJobsOnce(ctx, storage)
		}
	}
}

// readyPromoteJitter returns a uniform offset in [-base/10, +base/10].
func readyPromoteJitter(base time.Duration) time.Duration {
	span := int64(base / 5) // 20% of base, centered → ±10%
	if span <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(span+1) - span/2)
}

func (w *Worker) promoteReadyJobsOnce(ctx context.Context, storage readyPromoterStorage) {
	if _, err := storage.PromoteReadyJobs(ctx); err != nil &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		w.logger.Warn("ready promotion pass failed", "error", err)
	}
}

// queuesWithCapacity returns queue names that haven't reached their concurrency limit.
func (w *Worker) queuesWithCapacity() []string {
	available := make([]string, 0, len(w.config.Queues))
	for name, maxConcurrency := range w.config.Queues {
		if !w.queueRateLimitHasToken(name) {
			continue
		}
		counter, ok := w.queueRunning[name]
		if !ok {
			available = append(available, name)
			continue
		}
		if int(counter.Load()) < maxConcurrency {
			available = append(available, name)
		}
	}
	return available
}

func (w *Worker) queueRateLimitHasToken(queueName string) bool {
	bucket, ok := w.queueRateBuckets[queueName]
	if !ok {
		return true
	}
	return bucket.hasToken(time.Now())
}

func (w *Worker) tryConsumeQueueRateLimit(queueName string) bool {
	bucket, ok := w.queueRateBuckets[queueName]
	if !ok {
		return true
	}
	return bucket.tryConsume(time.Now())
}

func (w *Worker) refundQueueRateLimit(queueName string) {
	bucket, ok := w.queueRateBuckets[queueName]
	if !ok {
		return
	}
	bucket.refund(time.Now())
}

// trackQueueJob increments the running counter for a queue and records the
// run→queue mapping.
func (w *Worker) trackQueueJob(runToken uint64, queueName string) {
	if counter, ok := w.queueRunning[queueName]; ok {
		counter.Add(1)
	}
	w.queueJobIDMu.Lock()
	w.queueJobID[runToken] = queueName
	w.queueJobIDMu.Unlock()
}

// tryTrackQueueJob is the authoritative per-queue admission gate. Its atomic
// CAS is not advisory: once a queue is at its configured cap, dispatch must
// release the dequeued job instead of letting it borrow capacity from another
// queue.
func (w *Worker) tryTrackQueueJob(runToken uint64, queueName string) bool {
	counter, ok := w.queueRunning[queueName]
	if !ok {
		w.trackQueueJob(runToken, queueName)
		return true
	}
	maxConcurrency, ok := w.config.Queues[queueName]
	if !ok {
		w.trackQueueJob(runToken, queueName)
		return true
	}
	for {
		current := counter.Load()
		if int(current) >= maxConcurrency {
			return false
		}
		if counter.CompareAndSwap(current, current+1) {
			w.queueJobIDMu.Lock()
			w.queueJobID[runToken] = queueName
			w.queueJobIDMu.Unlock()
			return true
		}
	}
}

// untrackQueueJob decrements the running counter for THIS RUN's queue. Keyed by
// run token so a second run of the same job id cannot consume this one's entry.
func (w *Worker) untrackQueueJob(runToken uint64) {
	w.queueJobIDMu.Lock()
	queueName, ok := w.queueJobID[runToken]
	if ok {
		delete(w.queueJobID, runToken)
	}
	w.queueJobIDMu.Unlock()

	if ok {
		if counter, exists := w.queueRunning[queueName]; exists {
			counter.Add(-1)
		}
	}
}

func (w *Worker) concurrencySlotTTL() time.Duration {
	base := defaultConcurrencySlotTTL
	if w.config.LockDuration > 0 {
		base = w.config.LockDuration
	}
	// Slot leases are renewed only on heartbeat ticks. Keep roughly three
	// renewal opportunities inside the slot TTL, mirroring the stale-lock
	// heartbeat reasoning without changing the job lock lease itself.
	if w.heartbeatInterval > 0 {
		minTTL := 3 * w.heartbeatInterval
		if base < minTTL {
			return minTTL
		}
	}
	return base
}

func (w *Worker) capSlotName(cap ConcurrencyCapConfig, job *core.Job) (name string, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("concurrency cap key panicked; releasing dequeued job",
				"job_id", job.ID,
				"cap", cap.Name,
				"panic", r,
				"stack", string(debug.Stack()))
			name = ""
			ok = false
		}
	}()
	if cap.Key == nil {
		return cap.Name, true
	}
	return cap.Name + ":" + cap.Key(job), true
}

func (w *Worker) rateLimitName(limit RateLimitConfig, job *core.Job) (name string, ok bool) {
	ok = true
	// Recover a panicking user RateLimitKey (parity with capSlotName): a panic
	// here would otherwise crash the worker on the dispatch goroutine. Treat it as
	// a derivation failure so the caller bounces the job instead.
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("rate-limit key panicked; releasing dequeued job",
				"job_id", job.ID,
				"limit", limit.Name,
				"panic", r,
				"stack", string(debug.Stack()))
			name = ""
			ok = false
		}
	}()
	n := limit.Name
	if limit.Key != nil {
		n = limit.Name + ":" + limit.Key(job)
	}
	// Bound the effective name to the limit_name column width: an unbounded
	// RateLimitKey would otherwise overflow it and hot-loop the job (teardown g4).
	return boundRateLimitName(n), true
}

// tryConsumeRateLimits returns (allowed, reason). reason is meaningful only when
// allowed is false: bounceFleetRateCached when the per-key cooldown short-circuited
// the DB transaction, bounceFleetRate when the DB itself denied (or errored).
func (w *Worker) tryConsumeRateLimits(ctx context.Context, job *core.Job) (bool, bounceReason) {
	if len(w.config.RateLimits) == 0 {
		return true, ""
	}
	storage, ok := w.queue.Storage().(rateLimiterStorage)
	if !ok {
		if w.rateLimitStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support fleet-wide rate limits; continuing without RateLimit enforcement")
		}
		return true, ""
	}
	// Refund support: when a LATER fleet limit denies, the units already consumed
	// from EARLIER limits must be returned, or every multi-limit bounce permanently
	// drains those windows for a job that never runs (teardown g4). Backends without
	// ReleaseRate degrade to the prior no-refund behavior.
	releaser, canRelease := w.queue.Storage().(rateReleaser)
	// Precise consume+refund: when available (GormStorage, all dialects), refund the
	// EXACT window the consume committed to, so a refund that races a window rollover
	// can no longer decrement the wrong window and strand the consume's own. Falls
	// back to the now-relative rateReleaser path for backends without it.
	windowed, useWindowed := w.queue.Storage().(windowedRateLimiter)
	type consumedRate struct {
		name        string
		window      time.Duration
		windowStart time.Time
	}
	var consumed []consumedRate
	refund := func() {
		if !useWindowed && !canRelease {
			return
		}
		for _, c := range consumed {
			var err error
			if useWindowed {
				err = windowed.ReleaseRateAt(ctx, c.name, c.windowStart)
			} else {
				err = releaser.ReleaseRate(ctx, c.name, c.window)
			}
			if err != nil {
				w.logger.Warn("failed to refund consumed rate limit after a later limit denied",
					"job_id", job.ID, "limit", c.name, "error", err)
			}
		}
	}
	for _, limit := range w.config.RateLimits {
		if limit.Name == "" || limit.PerSecond <= 0 {
			continue
		}
		window := w.resolveRateLimitWindow(limit)
		limitName, nameOK := w.rateLimitName(limit, job)
		if !nameOK {
			refund()
			return false, bounceFleetRate
		}
		// cto-F2 per-key cooldown (KEYED limits only — the gap v1 left): if this
		// exact bucket was denied by the DB this window, skip the locked
		// TryConsumeRate transaction and bounce — the DB fixed window would only
		// deny again (count only rises within a window). Correctness-neutral: the
		// cache is written only from a real DB denial, so a hit means the DB
		// already said no this window; after the window rolls keyedRateSaturated
		// prunes the entry and the DB gate decides again.
		//
		// Scoped to keyed limits deliberately: for an all-unkeyed config dequeueSlots
		// already suppresses CLAIMING during saturation, so a job only reaches here
		// on the probe tick, which MUST consult the DB to detect headroom returning
		// — short-circuiting it would change v1's proven unkeyed behavior.
		if limit.Key != nil && w.keyedRateSaturated(limitName, time.Now()) {
			refund()
			return false, bounceFleetRateCached
		}
		var allowed bool
		var windowStart time.Time
		var err error
		if useWindowed {
			allowed, windowStart, err = windowed.TryConsumeRateWindow(ctx, limitName, limit.PerSecond, window, time.Time{})
		} else {
			allowed, err = storage.TryConsumeRate(ctx, limitName, limit.PerSecond, window, time.Time{})
		}
		if err != nil {
			w.logger.Warn("failed to consume rate limit; releasing dequeued job",
				"job_id", job.ID,
				"limit", limitName,
				"error", err)
			refund()
			return false, bounceFleetRate
		}
		if !allowed {
			// cto-F2: remember this bucket has no headroom until its window rolls.
			// For an all-unkeyed config dequeueSlots reads this to stop CLAIMING
			// (removes both churns); for a keyed bucket the pre-tx fast path above
			// reads it to skip the next DB rate tx (removes the denied-tx churn).
			w.markRateSaturated(limitName, window)
			refund()
			return false, bounceFleetRate
		}
		consumed = append(consumed, consumedRate{name: limitName, window: window, windowStart: windowStart})
	}
	return true, ""
}

// markRateSaturated records that limitName has no headroom until the END of its
// current fixed window (now.Truncate(window).Add(window), matching
// TryConsumeRate's own windowStart math), so the probe re-fires exactly when the
// gated window can roll. It is a worker-local heuristic damper, never an
// admission authority — the unchanged TryConsumeRate DB gate still decides every
// admission — so bounded worker/DB clock skew only shifts the next probe by the
// skew, never over- or under-admits.
// defaultRateSaturationCap bounds the saturated-bucket cooldown cache when the
// caller does not set WithRateSaturationCacheSize.
const defaultRateSaturationCap = 4096

func (w *Worker) markRateSaturated(limitName string, window time.Duration) {
	if window <= 0 {
		window = defaultRateLimitWindow
	}
	until := time.Now().Truncate(window).Add(window)
	w.rateSaturationMu.Lock()
	defer w.rateSaturationMu.Unlock()
	// Refreshing an existing bucket never grows the map. A NEW bucket at the cap
	// triggers a prune of expired entries; if still full, refuse to insert — that
	// bucket simply pays the DB rate tx, exactly as before the cooldown (graceful
	// degrade). We never evict a still-live entry, so a hot key is never dropped.
	// This bounds memory at cap entries regardless of RateLimitKey cardinality.
	if _, exists := w.rateSaturatedUntil[limitName]; !exists && len(w.rateSaturatedUntil) >= w.config.rateSaturationCap {
		now := time.Now()
		for name, u := range w.rateSaturatedUntil {
			if !now.Before(u) {
				delete(w.rateSaturatedUntil, name)
			}
		}
		if len(w.rateSaturatedUntil) >= w.config.rateSaturationCap {
			return
		}
	}
	w.rateSaturatedUntil[limitName] = until
}

// keyedRateSaturated reports whether the specific effective limit bucket is
// known saturated within its current window, pruning the entry on read once its
// window has rolled. Used by the pre-tx fast path in tryConsumeRateLimits to skip
// the DB rate transaction for a bucket the DB already denied this window.
func (w *Worker) keyedRateSaturated(limitName string, now time.Time) bool {
	w.rateSaturationMu.Lock()
	defer w.rateSaturationMu.Unlock()
	until, ok := w.rateSaturatedUntil[limitName]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(w.rateSaturatedUntil, limitName)
	return false
}

// unkeyedRateLimitsSaturated reports whether any fleet RateLimit was last seen
// saturated within a window that has not yet rolled. Callers must guard on
// w.allRateLimitsUnkeyed: a keyed limit's effective name is per-job, so its
// saturation can't gate a pre-claim decision. Because every job is subject to
// every configured fleet limit, any one saturated unkeyed limit means a freshly
// claimed job would bounce — so claiming is futile until the window rolls.
// Expired entries are pruned opportunistically.
func (w *Worker) unkeyedRateLimitsSaturated(now time.Time) bool {
	w.rateSaturationMu.Lock()
	defer w.rateSaturationMu.Unlock()
	saturated := false
	for name, until := range w.rateSaturatedUntil {
		if now.Before(until) {
			saturated = true
		} else {
			delete(w.rateSaturatedUntil, name)
		}
	}
	return saturated
}

// bounceReason labels why dispatchDequeuedJobs released a just-claimed job back
// to pending instead of running it. Each value is a low-cardinality metric label
// (jobs.dequeue.released{reason}).
type bounceReason string

const (
	bounceQueueCap        bounceReason = "queue_cap"         // per-queue concurrency cap full
	bounceQueueRate       bounceReason = "queue_rate"        // in-memory queue rate limiter
	bounceConcurrency     bounceReason = "concurrency"       // fleet concurrency slot unavailable
	bounceFleetRate       bounceReason = "fleet_rate"        // fleet (DB) rate limit saturated — paid the locked tx
	bounceFleetRateCached bounceReason = "fleet_rate_cached" // keyed bucket known saturated — skipped the locked tx (per-key cooldown)
	bounceShutdown        bounceReason = "shutdown"          // ctx cancelled mid-dispatch
	bouncePaused          bounceReason = "paused"            // pause landed during the dequeue round-trip; claimed batch released
)

// dequeueChurnCounters holds cumulative dispatch-churn counts. All fields are
// atomic; the struct is embedded by value in the pointer-only Worker.
type dequeueChurnCounters struct {
	queueCap        atomic.Int64
	queueRate       atomic.Int64
	concurrency     atomic.Int64
	fleetRate       atomic.Int64
	fleetRateCached atomic.Int64
	shutdown        atomic.Int64
	paused          atomic.Int64
	suppressedTicks atomic.Int64
}

func (w *Worker) recordBounce(r bounceReason) {
	switch r {
	case bounceQueueCap:
		w.dequeueChurn.queueCap.Add(1)
	case bounceQueueRate:
		w.dequeueChurn.queueRate.Add(1)
	case bounceConcurrency:
		w.dequeueChurn.concurrency.Add(1)
	case bounceFleetRate:
		w.dequeueChurn.fleetRate.Add(1)
	case bounceFleetRateCached:
		w.dequeueChurn.fleetRateCached.Add(1)
	case bounceShutdown:
		w.dequeueChurn.shutdown.Add(1)
	case bouncePaused:
		w.dequeueChurn.paused.Add(1)
	}
}

// DequeueReleasedByReason returns the cumulative count of dequeued jobs the
// dispatcher released back to pending (a "bounce"), keyed by reason. Under the
// cto-F2 saturation throttle a steady-state-saturated fleet limit shows only a
// small, slowly-growing fleet_rate count (one probe batch per rate window)
// rather than a count that climbs every poll tick. Safe to call concurrently.
func (w *Worker) DequeueReleasedByReason() map[string]int64 {
	return map[string]int64{
		string(bounceQueueCap):        w.dequeueChurn.queueCap.Load(),
		string(bounceQueueRate):       w.dequeueChurn.queueRate.Load(),
		string(bounceConcurrency):     w.dequeueChurn.concurrency.Load(),
		string(bounceFleetRate):       w.dequeueChurn.fleetRate.Load(),
		string(bounceFleetRateCached): w.dequeueChurn.fleetRateCached.Load(),
		string(bounceShutdown):        w.dequeueChurn.shutdown.Load(),
		string(bouncePaused):          w.dequeueChurn.paused.Load(),
	}
}

// DequeueSuppressedTicks returns the cumulative number of poll ticks on which
// the cto-F2 throttle skipped claiming because an all-unkeyed fleet rate limit
// was saturated. A rising value alongside a flat DequeueReleasedByReason
// fleet_rate count is the throttle working as intended (not a stuck worker).
// Safe to call concurrently.
func (w *Worker) DequeueSuppressedTicks() int64 {
	return w.dequeueChurn.suppressedTicks.Load()
}

// DequeueRateSaturationCacheSize returns the current number of saturated rate
// buckets cached by the per-key cooldown (see WithRateSaturationCacheSize). A
// value at the configured cap means new saturated buckets are degrading to
// paying the DB rate transaction — the high-cardinality-key signal. Returns
// int64 so it passes directly as the metrics.InstrumentWorkerRateSaturation
// snapshot (matching DequeueSuppressedTicks). Safe to call concurrently.
func (w *Worker) DequeueRateSaturationCacheSize() int64 {
	w.rateSaturationMu.Lock()
	defer w.rateSaturationMu.Unlock()
	return int64(len(w.rateSaturatedUntil))
}

func (w *Worker) tryAcquireConcurrencySlots(ctx context.Context, job *core.Job, runToken uint64) bool {
	if len(w.config.ConcurrencyCaps) == 0 {
		return true
	}
	storage, ok := w.queue.Storage().(concurrencySlotStorage)
	if !ok {
		return true
	}
	ttl := w.concurrencySlotTTL()
	// Ownership is recorded INCREMENTALLY, one slot at a time, rather than once at
	// the end. Two reasons, and the second is a bug this cost us:
	//
	//  1. Ownership of a row transfers at the instant this run JOINS it — not when
	//     the run later registers in runningJobs. Those are different moments: this
	//     function runs in dispatchDequeuedJobs, the job then crosses jobsChan
	//     (which BLOCKS while every processLoop goroutine is busy), and only then
	//     does processJobRun register. A release that inferred ownership from
	//     runningJobs was blind for that whole window.
	//
	//  2. THE ROLLBACK PATHS BELOW ARE RELEASES TOO. They used to call
	//     releaseSlotNames directly, which has no run-token awareness, so a partial
	//     acquire deleted rows an EARLIER run of the same job id was still holding
	//     — the exact over-admission the ownership fence exists to prevent, on the
	//     one path that did not consult it. Reachable with two or more caps: run #2
	//     renews cap A idempotently (SHARING run #1's row), then errors or is
	//     refused on cap B, and its rollback deletes A out from under run #1. The
	//     ownership fence cannot refuse it — same job id, same worker id.
	//
	// Recording as we go means every bail-out can route through
	// releaseConcurrencySlots, which consults the fence and cleans up this run's
	// entry. Within this package there is now exactly ONE place that deletes these
	// rows: releaseSlotNames, called only from releaseConcurrencySlots. (pkg/storage
	// deletes them too — on batch completion, terminal transitions and aggressive
	// pause — but every one of those sits in the transaction that also makes the job
	// unclaimable, so none can race a later run of the same job id.)
	//
	// THE RECORD HAPPENS BEFORE THE STORAGE CALL, NOT AFTER. Recording after it
	// returns left a real window, just a much smaller one than the version before
	// it: between TryAcquireConcurrencySlot committing the row and this run
	// publishing that it holds it, a concurrent release for an EARLIER run of the
	// same job id scans the map, sees no other holder, and deletes the row this run
	// has already joined. Microseconds instead of a blocking channel send, but the
	// same defect and the same consequence — reproduced, with a second job then
	// admitted past a cap of 1.
	//
	// Publishing INTENT early is safe in the other direction. If the acquire then
	// fails, the rollback names a row this run never got — and either another token
	// holds it, in which case the fence skips it, or nobody does, in which case
	// deleting it harms no one (and ReleaseConcurrencySlotOwned still fences a peer
	// worker by worker_id). Claiming too much briefly costs nothing; claiming too
	// little loses a live row.
	//
	// Stores a COPY: the local slice is appended to on the next iteration, and
	// append reuses the backing array while there is capacity.
	// MERGES, it does not replace. A departing run hands its names to this one
	// (see releaseConcurrencySlots), and that can land at ANY point while this loop
	// is still walking the caps. A wholesale assignment here would discard
	// everything handed over on the very next iteration — so the handover survived
	// exactly one loop step, and the names it rescued leaked to the slot TTL after
	// all. Reproduced two ways: three caps with a transient error on the second,
	// and two caps with a CapKey that changed between the runs, the latter on the
	// PLAIN SUCCESS PATH with no error, refusal or shutdown involved.
	record := func(names []string) {
		w.slotJobIDMu.Lock()
		prev := w.slotJobID[runToken]
		w.slotJobID[runToken] = slotHold{jobID: job.ID, names: unionSlotNames(names, prev.names)}
		w.slotJobIDMu.Unlock()
	}
	// A rollback must not release on the context that just failed: on shutdown ctx
	// is already Done, so the DELETE would be refused too and a slot acquired
	// moments earlier would survive to its TTL.
	//
	// HONESTY, because an earlier commit message claimed more than this earns:
	// tryAcquireConcurrencySlots has exactly ONE production caller, and on a
	// refusal that caller immediately runs releaseDequeuedJobOnShutdown, whose
	// release context has ALWAYS been detached. So this closes no leak that was
	// reachable through dispatch — the row is released a few lines later either
	// way. It is defence-in-depth for the helper itself, and it means the function
	// no longer depends on its caller cleaning up after a bail-out it already
	// reported. Worth keeping; not worth claiming as a fix.
	rollback := func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		w.releaseConcurrencySlots(releaseCtx, job.ID, runToken)
	}
	acquiredSlots := make([]string, 0, len(w.config.ConcurrencyCaps))
	for _, cap := range w.config.ConcurrencyCaps {
		slotName, ok := w.capSlotName(cap, job)
		if !ok {
			rollback()
			return false
		}
		acquiredSlots = append(acquiredSlots, slotName)
		record(acquiredSlots)
		acquired, err := storage.TryAcquireConcurrencySlot(ctx, slotName, job.ID, w.config.WorkerID, cap.Limit, ttl)
		if err != nil {
			w.logger.Warn("failed to acquire concurrency slot; releasing dequeued job",
				"job_id", job.ID,
				"slot", slotName,
				"error", err)
			rollback()
			return false
		}
		if !acquired {
			rollback()
			return false
		}
	}
	return true
}

// releaseConcurrencySlots releases the slots THIS RUN acquired. Keyed by run
// token: releasing by job id alone let an earlier run delete the slot row a later
// run of the same job is still relying on, and the ownership fence cannot catch
// that because both runs share the job id AND the worker id.
func (w *Worker) releaseConcurrencySlots(ctx context.Context, jobID core.UUID, runToken uint64) {
	// Keying the in-memory list by run is not enough, because the DATABASE row is
	// not keyed by run: concurrency_slots is (slot_name, job_id) and capSlotName is
	// deterministic per (cap, job), so two runs of one job id derive the SAME row.
	// Run #2 re-acquiring lands in TryAcquireConcurrencySlot's idempotent-renewal
	// branch and SHARES run #1's row rather than creating its own.
	//
	// Deleting that row while another run still holds it drops the fleet cap out
	// from under a handler that is still executing — which the ownership fence
	// cannot refuse (both runs share the job id AND the worker id) and which
	// RenewConcurrencySlot cannot repair (it is UPDATE-only, so it silently
	// resurrects nothing).
	//
	// The invariant is therefore: THE ROW SURVIVES WHILE ANY TOKEN HOLDS IT, and
	// the last holder to finish is the one that deletes it. That is decided here,
	// under a single mutex, by asking whether any OTHER token still holds this job
	// id — which is why slotHold carries the job id.
	//
	// An earlier attempt asked runningJobs instead. That is the wrong map: a run
	// joins the row in tryAcquireConcurrencySlots (dispatch side) but only appears
	// in runningJobs later, in processJobRun, with a blocking channel send in
	// between. Between those two moments the later run was invisible and its row
	// was deleted — the exact over-admission this exists to prevent. Keying on the
	// map written at ACQUIRE time has no such window, and it covers the reverse
	// direction too: a later run that finishes FIRST still sees the earlier holder
	// and leaves the row for it.
	//
	// The scan is linear in the number of runs currently HOLDING slots, which is
	// bounded by this worker's total configured concurrency (a slot is only ever
	// recorded for a dispatched, not-yet-finished run) — tens to low hundreds, not
	// job throughput. A jobID-keyed refcount would make it O(1), at the cost of a
	// second map to keep in sync with this one; that is one more invariant to get
	// wrong, and getting this one wrong is what produced the bug twice already.
	// On handover the departing run's names are MERGED INTO the surviving holders,
	// not discarded. The successor can legitimately hold a strict SUBSET: it is
	// still walking the cap loop, or it bailed out before reaching the later caps.
	// Dropping the difference left rows named by nobody — held by no run, released
	// by no run, and surviving to the slot TTL (45 minutes by default), denying one
	// slot of a FLEET-WIDE cap to every worker in the deployment. Reproduced with
	// two caps: run #1 holds customer+region, hands over to a run #2 that has only
	// recorded customer, and region leaks.
	//
	// Merging keeps the invariant exact rather than approximate: the union of every
	// name any live run of this job id holds is always covered, so whoever is last
	// out releases all of them.
	// THE LOCK IS HELD ACROSS THE DELETE. That is deliberate, and it is the whole
	// point of this function.
	//
	// The decision "no other run holds this row" is only meaningful if no run can
	// JOIN the row between making it and acting on it. Ownership is published under
	// this mutex (record(), before the storage call), so holding it across
	// releaseSlotNames makes the two atomic with respect to every acquire: a later
	// run either publishes BEFORE the scan and is seen, or lands AFTER the delete
	// and creates a fresh row. Neither loses a row it believes it holds.
	//
	// Making the decision under the lock and then releasing it before the DELETE —
	// which is what this did — leaves a window a full DB ROUND TRIP wide, orders of
	// magnitude larger than the acquire-side window a previous round called a real
	// defect and fixed. Reproduced: run #2 renews the row through
	// TryAcquireConcurrencySlot's idempotent branch, run #1's in-flight DELETE then
	// removes it, and a further job is admitted past a FLEET-WIDE cap while run #2
	// is still executing. ReleaseConcurrencySlotOwned cannot refuse it — same job
	// id, same worker id.
	//
	// COST, corrected — the first version of this note understated it. Everything
	// that touches slotJobIDMu now waits on an in-flight DELETE, and that is THREE
	// paths, not two:
	//   1. other releases,
	//   2. acquires (record(), so the dispatch loop, which is sequential), and
	//   3. renewConcurrencySlots — which runHeartbeat calls after every successful
	//      Heartbeat for EVERY running job. Measured with a 900ms DELETE: an
	//      unrelated running job's slot renewal blocked for 851ms.
	//
	// (3) is the one that matters, because a heartbeat that does not return to its
	// select does not send the next Heartbeat, and a lapsed lease is how the
	// stale-lock reaper hands a still-executing job to a peer. It is why the DELETE
	// is bounded at slotReleaseTimeout: the worst case is one bounded round trip
	// per queued release, against a 2-minute heartbeat interval and a 45-minute
	// StaleLockAge, rather than "as long as the database stays silent".
	//
	// Still proportionate: this mutex is touched ONLY when ConcurrencyCaps are
	// configured (tryAcquireConcurrencySlots returns at the len()==0 guard
	// otherwise), and a cap exists precisely to keep the number of concurrent
	// capped jobs small. A worker with no caps never contends here at all.
	//
	// Safe to hold across I/O: this mutex is a LEAF. releaseSlotNames touches only
	// the storage interface and a read-only config field, GormStorage's release is
	// a single DELETE with no callback, and no path in this package acquires
	// another worker mutex beneath it — verified by a static scan of every w.*Mu
	// site. The honest residual is a THIRD-PARTY core.Storage (or a custom slog
	// handler on the warn path) that calls back into this worker while the mutex is
	// held; nothing in-tree does, and liveness under a genuinely slow DELETE is
	// covered by TestReleaseConcurrencySlots_SlowDeleteDoesNotWedgeDispatch.
	w.slotJobIDMu.Lock()
	defer w.slotJobIDMu.Unlock()

	hold := w.slotJobID[runToken]
	delete(w.slotJobID, runToken)
	for otherToken, other := range w.slotJobID {
		if otherToken == runToken || other.jobID != jobID {
			continue
		}
		// Another run still holds this job's row: hand our names to it and leave
		// the row alone. It releases the union when it finishes.
		//
		// ONE holder, not all of them, and Go's map order makes that one arbitrary
		// — which is fine, and worth the two lines to show why. Every departing run
		// merges into SOME remaining holder, so by induction the last run out has
		// accumulated every name any of them ever held. Which intermediate holder
		// receives them cannot matter, because that holder either departs (and
		// passes them on again) or is itself the last one out. Merging into all of
		// them would only duplicate the names and the DELETEs.
		other.names = unionSlotNames(other.names, hold.names)
		w.slotJobID[otherToken] = other
		return
	}
	w.releaseSlotNames(ctx, jobID, hold.names)
}

// unionSlotNames appends the names of into a that are not already present. Slot
// lists are at most one entry per configured cap, so the quadratic scan is on a
// handful of strings.
func unionSlotNames(a, b []string) []string {
	out := append([]string(nil), a...)
	for _, name := range b {
		found := false
		for _, existing := range out {
			if existing == name {
				found = true
				break
			}
		}
		if !found {
			out = append(out, name)
		}
	}
	return out
}

// slotReleaseTimeout bounds the TOTAL time a release may spend in storage, across
// every slot it is releasing. It exists because releaseConcurrencySlots holds
// slotJobIDMu across this call: an unbounded query would hold a process-wide mutex
// for as long as the database takes to answer, which on a partitioned connection
// (TCP that neither errors nor RSTs) is minutes. database/sql applies no query
// timeout of its own.
//
// It is a TOTAL, not a per-DELETE budget, because the mutex hold is what needs
// bounding and that is the sum. But it is DIVIDED among the slots rather than
// shared: a single context for the whole loop let the first DELETE consume the
// entire budget, after which every later slot ran on an already-expired context,
// failed instantly, and left its row to expire at the TTL. Each slot now gets its
// own fair share, so all of them get a genuine attempt and the total stays capped.
const slotReleaseTimeout = 5 * time.Second

// slotReleaseBudget is the TOTAL time a release may spend in storage while holding
// slotJobIDMu.
//
// It is derived from the heartbeat interval rather than being a constant, because
// the safety argument depends on it. runHeartbeat calls renewConcurrencySlots
// synchronously in its ticker arm, and that takes this same mutex — so a release
// that outlives a heartbeat interval delays the next Heartbeat write. The reaper
// reclaims a job whose last heartbeat is older than StaleLockAge, and
// hbInterval = StaleLockAge/3, so delaying at most ONE beat still leaves two
// beats of margin. Exceeding StaleLockAge means a still-executing job is handed to
// a peer: double execution, the thing runHeartbeat exists to prevent.
//
// An earlier version used a flat 5s and justified it as "far inside the 2-minute
// heartbeat interval". Both of those are DEFAULTS, not invariants: WithStaleLockAge
// is a public option, and at StaleLockAge=2s the interval is ~667ms, so a 5s hold
// was 7.4x the interval and 2.5x StaleLockAge — reclaimable while the handler was
// still running. The chaos harness configures exactly that shape.
//
// WHICH PROPERTY WINS, stated because the two genuinely conflict at extreme
// configurations: bounding the TOTAL beats giving every slot a comfortable share.
// A slot whose DELETE is cut short leaks one cap row until its TTL, which
// self-heals; a lease that lapses double-executes a job, which does not.
//
// WHERE THAT BITES, computed rather than left implicit. The per-slot share is
// budget/len(caps):
//
//	default (2m interval):        5s budget  -> 5s at 1 cap, 250ms at 20
//	StaleLockAge 2s (~667ms):     333ms      -> 333ms at 1 cap, 17ms at 20
//	StaleLockAge <=600ms (200ms): 100ms      -> 100ms at 1 cap, 5ms at 20
//
// So a sub-second StaleLockAge combined with many caps means slot rows are
// effectively reclaimed by TTL expiry rather than by explicit release. That is the
// correct end of the trade — such a deployment has asked for aggressive
// reclamation and gets it — but it is a real consequence and not a rounding error.
// A worker built by NewWorker always has a heartbeat interval (floored at 200ms),
// so the 5s ceiling only applies when the interval exceeds 10s; the `> 0` guard
// below is for a zero-valued struct in tests, not a production path.
//
// One further cost of giving the job release and the slot release INDEPENDENT
// budgets: a worst-case shutdown release is now their sum rather than a shared 5s.
// That is the right direction — the shared budget's failure mode was releasing no
// slot rows at all — but it is more wall time, bounded and paid only when storage
// is already unresponsive.
func (w *Worker) slotReleaseBudget() time.Duration {
	budget := slotReleaseTimeout
	// HALF an interval, not a whole one: the hold must leave room for the beat it
	// is delaying to still land inside its own interval. Budgeting a full interval
	// means the next Heartbeat lands exactly one interval late in the worst case,
	// which eats a third of the StaleLockAge margin outright.
	if w.heartbeatInterval > 0 {
		if half := w.heartbeatInterval / 2; half < budget {
			budget = half
		}
	}
	return budget
}

func (w *Worker) releaseSlotNames(ctx context.Context, jobID core.UUID, slots []string) {
	if len(slots) == 0 {
		return
	}
	// BOUND THE I/O HERE, not at the call sites. Two of the three callers already
	// passed a 5s context; the hot-path defer in processJobRun passed
	// context.WithoutCancel(ctx), which strips the DEADLINE as well as the cancel —
	// so the DELETE under the mutex had no bound at all.
	//
	// That mattered only after the mutex started being held across this call. A
	// hung DELETE used to block one goroutine; it would now block (a) the whole
	// dispatch loop, since record() takes the same mutex and dispatchDequeuedJobs
	// is sequential, and (b) renewConcurrencySlots for UNRELATED still-running
	// jobs — which runHeartbeat calls inside its ticker arm, so that goroutine
	// never returns to its select and stops sending heartbeats. At StaleLockAge
	// those leases lapse and the reaper hands still-executing jobs to peers: the
	// double-execution runHeartbeat exists to prevent, reachable from one hung
	// query.
	//
	// Bounding here rather than at one call site means every entry point is capped,
	// including any added later. A caller that already supplied a tighter deadline
	// keeps it.
	// NO FLOOR. An earlier version clamped the per-slot share to a minimum, which
	// quietly contradicted the priority stated on slotReleaseBudget: at 20 caps and
	// an aggressive StaleLockAge the clamp pushed the TOTAL to 500ms against a
	// 333ms budget, so the bound that exists to protect the heartbeat was not
	// actually being honoured. It was also a constant no test could fail on — the
	// clamp only binds past ~11 caps, and removing it left the suite green.
	//
	// Dividing exactly means the total is the budget, full stop. A very large cap
	// count therefore gives each slot a small share, and a DELETE that cannot
	// finish in it leaves its row to expire at the TTL. That is the trade this
	// function already declares: a leaked row self-heals, a lapsed lease
	// double-executes a job.
	perSlot := w.slotReleaseBudget() / time.Duration(len(slots))

	storage, ok := w.queue.Storage().(concurrencySlotStorage)
	if !ok {
		return
	}
	// Prefer the ownership-fenced release. Without the worker_id predicate, a
	// deferred release from a worker whose job was already reclaimed by the
	// stale-lock reaper deletes the slot row the NEW holder is relying on,
	// under-counting the cap and admitting an extra concurrent job.
	owned, fenced := storage.(concurrencySlotOwnedReleaser)
	for _, slot := range slots {
		slotCtx, cancel := context.WithTimeout(ctx, perSlot)
		var err error
		if fenced {
			err = owned.ReleaseConcurrencySlotOwned(slotCtx, slot, jobID, w.config.WorkerID)
		} else {
			err = storage.ReleaseConcurrencySlot(slotCtx, slot, jobID)
		}
		cancel()
		if err != nil {
			// Say what actually happens now. The in-memory hold is already gone, so
			// this worker will not retry: the row survives until its expires_at
			// lapses, and TryAcquireConcurrencySlot's live count filters on
			// `expires_at >= now`, so the cap self-heals at the TTL rather than
			// leaking permanently. That is bounded but not free — it is one slot of
			// a fleet-wide cap held by nobody for up to concurrencySlotTTL.
			//
			// This path became more likely when the DELETE gained a deadline: a
			// database slow enough to blow slotReleaseTimeout lands here instead of
			// blocking the worker. That is the better failure, and an operator
			// should be able to tell the two apart from the log alone.
			w.logger.Warn("failed to release concurrency slot; it stays held until its TTL expires",
				"job_id", jobID,
				"slot", slot,
				"ttl", w.concurrencySlotTTL(),
				"error", err)
		}
	}
}

func (w *Worker) renewConcurrencySlots(ctx context.Context, jobID core.UUID, runToken uint64) {
	w.slotJobIDMu.Lock()
	slots := append([]string(nil), w.slotJobID[runToken].names...)
	w.slotJobIDMu.Unlock()
	if len(slots) == 0 {
		return
	}
	storage, ok := w.queue.Storage().(concurrencySlotRenewer)
	if !ok {
		return
	}
	ttl := w.concurrencySlotTTL()
	for _, slot := range slots {
		if _, err := storage.RenewConcurrencySlot(ctx, slot, jobID, ttl); err != nil {
			w.logger.Warn("failed to renew concurrency slot", "job_id", jobID, "slot", slot, "error", err)
		}
	}
}

// dequeueWithRetry attempts to dequeue a job with exponential backoff on failure.
func (w *Worker) dequeueWithRetry(ctx context.Context, queues []string) (*core.Job, error) {
	var job *core.Job
	err := retryWithBackoff(ctx, *w.config.DequeueRetry, func() error {
		var dequeueErr error
		job, dequeueErr = w.queue.Storage().Dequeue(ctx, queues, w.config.WorkerID)
		return dequeueErr
	})
	return job, err
}

func (w *Worker) dequeueBatchWithRetry(ctx context.Context, storage batchDequeuer, queues []string, limit int) ([]*core.Job, error) {
	var jobs []*core.Job
	err := retryWithBackoff(ctx, *w.config.DequeueRetry, func() error {
		var dequeueErr error
		jobs, dequeueErr = storage.DequeueBatch(ctx, queues, w.config.WorkerID, limit)
		return dequeueErr
	})
	return jobs, err
}

func (w *Worker) dequeueBatchPerQueueWithRetry(ctx context.Context, storage perQueueDequeuer, budgets map[string]int, limit int) ([]*core.Job, error) {
	if limit <= 0 || len(budgets) == 0 {
		return []*core.Job{}, nil
	}
	cappedBudgets := make(map[string]int, len(budgets))
	for queueName, budget := range budgets {
		if budget <= 0 {
			continue
		}
		cappedBudgets[queueName] = budget
	}
	if len(cappedBudgets) == 0 {
		return []*core.Job{}, nil
	}

	var jobs []*core.Job
	err := retryWithBackoff(ctx, *w.config.DequeueRetry, func() error {
		var dequeueErr error
		jobs, dequeueErr = storage.DequeueBatchPerQueue(ctx, w.config.WorkerID, cappedBudgets)
		return dequeueErr
	})
	return jobs, err
}

// dispatchedJob carries a dequeued job together with the run token allocated for
// it at admission, so the run that registered the per-queue counter and the
// concurrency slots is the run that releases them.
type dispatchedJob struct {
	job   *core.Job
	token uint64
}

func (w *Worker) processLoop(ctx context.Context, jobs <-chan dispatchedJob) {
	defer w.handlerWG.Done()

	for dj := range jobs {
		w.processJobRun(ctx, dj.job, dj.token)
	}
}

// processJob runs a job under a freshly allocated run token. Callers that already
// hold one (the dispatch path, which registered admission state against it) use
// processJobRun instead.
func (w *Worker) processJob(ctx context.Context, job *core.Job) {
	w.processJobRun(ctx, job, w.nextRunToken.Add(1))
}

func (w *Worker) processJobRun(ctx context.Context, job *core.Job, runToken uint64) {
	// Defense-in-depth: no panic may escape processJob and crash the processLoop
	// goroutine (an unrecovered goroutine panic terminates the whole process).
	// User callbacks are individually recovered (queue.safeUserCallback,
	// RunExecutionMiddleware, IsFailure) and the handler is recovered in
	// executeHandler; this outermost net catches any unexpected internal panic,
	// logs it with a stack, and releases the job back to pending so it is
	// reclaimed by a healthy worker instead of stranded locked. Registered first
	// so it runs last during unwind, after the slot/counter cleanup defers below.
	//
	// Release is status='running'-guarded, so it cannot resurrect a job that
	// already reached a terminal/waiting/paused state (a panic after a terminal
	// write no-ops here). Note that release-to-pending does not burn a retry
	// attempt, so a persistently-panicking LIBRARY-INTERNAL bug (as opposed to a
	// user-callback panic, which is recovered upstream and never reaches here)
	// reclaim-loops rather than dead-lettering. That is deliberate for a
	// last-resort net — fleet-wide survival beats a single stuck job — and it is
	// logged with a stack on every pass.
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("recovered panic in processJob; releasing job",
				"job_id", job.ID,
				"job_type", job.Type,
				"panic", r,
				"stack", string(debug.Stack()))
			w.releaseAfterTerminalWriteError(context.WithoutCancel(ctx), job.ID, "processJob panic")
		}
	}()

	// Ensure per-queue concurrency counter is decremented when job finishes
	defer w.untrackQueueJob(runToken)
	defer w.releaseConcurrencySlots(context.WithoutCancel(ctx), job.ID, runToken)

	startTime := time.Now()

	h, ok := w.queue.GetHandler(job.Type)
	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		if failer, ok := w.queue.Storage().(failTerminalWithResultStorage); ok {
			fo, err := w.failTerminalWithResult(ctx, failer, job.ID, fmt.Sprintf("no handler for %s", job.Type))
			if errors.Is(err, core.ErrJobNotOwned) {
				w.logger.Warn("job no longer owned after no-handler failure; skipping sub-job completion",
					"job_id", job.ID)
				return
			}
			if err != nil {
				w.logger.Error("failed to terminally fail no-handler job after retries", "job_id", job.ID, "error", err)
				w.releaseAfterTerminalWriteError(ctx, job.ID, "no-handler failure")
				return
			}
			if err := w.checkFanOutCompletion(ctx, fo); err != nil {
				w.logger.Error("failed to handle no-handler sub-job failure", "job_id", job.ID, "error", err)
			}
			return
		}
		// The failure must be RECORDED before the (non-idempotent) fan-out
		// accounting runs for it. A worker that reclaims the job may even have
		// the handler registered.
		if !w.dispositionWriteLanded(ctx, job.ID,
			w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil),
			"no-handler failure",
			"job no longer owned after no-handler failure; skipping sub-job completion") {
			return
		}
		if err := w.handleSubJobCompletion(ctx, job, false); err != nil {
			w.logger.Error("failed to handle no-handler sub-job failure", "job_id", job.ID, "error", err)
		}
		return
	}

	// Create context for this job — per-job timeout overrides handler default.
	var jobCtx context.Context
	var cancelJob context.CancelFunc
	effectiveTimeout := h.Timeout
	if job.Timeout > 0 {
		effectiveTimeout = job.Timeout
	}
	if effectiveTimeout > 0 {
		jobCtx, cancelJob = context.WithTimeout(ctx, effectiveTimeout)
	} else {
		jobCtx, cancelJob = context.WithCancel(ctx)
	}
	defer cancelJob()

	// Track this running job for aggressive pause (worker-local + queue-level registry)
	w.runningJobsMu.Lock()
	w.runningJobs[job.ID] = runningJobEntry{cancel: cancelJob, token: runToken}
	w.runningJobsMu.Unlock()
	w.queue.RegisterRunningJob(job.ID, cancelJob)
	defer func() {
		w.runningJobsMu.Lock()
		// KEYED delete: only remove the entry if it is still OURS.
		//
		// The aggressive-pause path is the first that releases a job to `pending`
		// while this worker is still alive and polling, so the same job can be
		// re-dequeued into a second processJob before this deferred cleanup runs.
		// An unconditional delete would then remove the SECOND run's registration,
		// leaving it invisible to Pause(Aggressive), Queue.CancelJob's local cancel
		// and the ownership audit for its whole duration. The per-run token keeps
		// each run responsible for exactly its own entry — see runningJobEntry for
		// why the cancel func itself cannot serve as that identity.
		//
		// BOTH deletes are inside the guard. The pause mark needs it for the same
		// reason and more urgently: Pause(Aggressive) marks by JOB ID from
		// runningJobs, so once a later run has replaced the entry a new mark
		// belongs to THAT run. An unconditional delete here let run #1's cleanup
		// eat run #2's mark, after which run #2's pause cancellation fell through
		// to the ordinary failure path and called Fail — the precise
		// attempt-burning this packet exists to prevent, reintroduced by the fix
		// for a different race.
		if cur, ok := w.runningJobs[job.ID]; ok && cur.token == runToken {
			delete(w.runningJobs, job.ID)
			// Unregister WITHOUT dropping runningJobsMu, so the two registries are
			// torn down as one step. Dropping it here would reopen the window the
			// token closes, one level down: a later run that both registered in
			// runningJobs AND registered with the queue inside the gap would have
			// its QUEUE-level entry deleted by this one, leaving a live handler
			// invisible to Queue.CancelJob and Queue.PauseJob for its whole
			// duration.
			//
			// HONESTY: unlike the token guard above, that interleave is ARGUED, not
			// reproduced. It needs another run to complete both registrations inside
			// a window of a few instructions, and nothing outside this function can
			// schedule that, so no test here fails when the lock is narrowed — do
			// not read the surrounding tests as covering it. The lock is held
			// because it costs nothing and closes the window by construction, which
			// is a better trade than a race that would be invisible in production.
			//
			// Safe because q.runningJobsMu is a LEAF: all five of its sites
			// (queue.go RegisterRunningJob / UnregisterRunningJob / PauseJob /
			// CancelJob / the resume path) are Lock, read-or-write, Unlock, copying
			// the cancel func out before invoking it, so nothing acquires THIS mutex
			// while holding that one and the nesting cannot invert.
			w.queue.UnregisterRunningJob(job.ID)
		}
		// Drop this run's mark if it went unconsumed — which happens when the
		// handler finished without ever surfacing the cancellation. The key is our
		// own token, so this can never touch another run's.
		delete(w.pauseCancelled, runToken)
		w.runningJobsMu.Unlock()
	}()

	// Call start hooks
	w.queue.CallStartHooks(jobCtx, job)

	// Call context-modifying start hooks (e.g. OTel span injection)
	jobCtx = w.queue.CallStartCtxHooks(jobCtx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// The heartbeat is DETACHED from jobCtx on purpose.
	//
	// It used to be a child of it, so anything that cancelled the handler also
	// killed the lease renewal — including an aggressive pause and a shutdown.
	// But cancelling a handler does not stop it: a handler mid-I/O, or one that
	// ignores ctx entirely, keeps running and keeps holding the job. With the
	// lease no longer renewed it lapses, the stale-lock reaper hands the job to a
	// peer, and the original handler is still executing it — cancellation causing
	// DOUBLE EXECUTION, which is the one thing the lease exists to prevent.
	//
	// The lease must track whether the handler is still RUNNING, not whether it
	// has been asked to stop. The deferred cancelHeartbeat below ends it when
	// processJob actually returns, which is the moment we genuinely stop holding
	// the job.
	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.WithoutCancel(jobCtx))
	defer cancelHeartbeat()

	// Start heartbeat goroutine to extend lock during long-running jobs
	go w.runHeartbeat(heartbeatCtx, job, runToken)

	resultBytes, err := w.queue.RunExecutionMiddleware(jobCtx, job, func(ctx context.Context, j *core.Job) ([]byte, error) {
		return w.executeHandler(ctx, j, h)
	})

	// Enforce the result size limit on the top-level handler result too — Call
	// already enforces it for nested results, but a top-level handler can return
	// an arbitrarily large value. Oversized results are a non-retryable failure:
	// persisting a multi-megabyte blob per job would bloat the table, and a retry
	// would just reproduce the same oversized result.
	if err == nil && len(resultBytes) > security.MaxResultSize {
		err = core.NoRetry(fmt.Errorf("jobs: job %q result is %d bytes, limit is %d",
			job.Type, len(resultBytes), security.MaxResultSize))
		resultBytes = nil
	}

	// Declared outside the block: the pause-cancel branch below needs it to
	// distinguish this worker's own cancellation from a genuine handler failure.
	var selfCancelled bool
	if err != nil {
		// Self-suspension signal — the handler moved its job to StatusWaiting
		// (fan-out or signal wait) and returned. Not a failure: just stop.
		if core.IsWaiting(err) {
			w.logger.Info("job waiting", "job_id", job.ID)
			// End the per-attempt span (OTel) before returning. Unlike
			// complete/fail, a parked attempt fires neither hook, so without this
			// its span would leak (never exported) until the resume starts a new
			// attempt with a fresh span. Use jobCtx — it carries the span injected
			// by CallStartCtxHooks, same as the complete/fail hooks below.
			w.queue.CallWaitingHooks(jobCtx, job)
			cancelHeartbeat()
			// Job is already in StatusWaiting; just return
			return
		}
		// Graceful shutdown: the handler was cancelled because the worker is
		// stopping, not because the job failed. Release it back to pending
		// (status→pending, lock cleared) so a surviving/new worker resumes it in
		// seconds via its idempotent phases — instead of burning a retry attempt
		// and a backoff delay. This is evaluated BEFORE IsFailure so a custom
		// IsFailure that treats context.Canceled as non-failure cannot zero err and
		// silently mark a shutdown-interrupted job COMPLETED (dropping its
		// unfinished phases). Only this worker's own shutdown-cancel qualifies.
		if w.shuttingDown.Load() && errors.Is(err, context.Canceled) {
			w.releaseDequeuedJobOnShutdown(ctx, job, runToken)
			cancelHeartbeat()
			return
		}
		// A worker-induced cancel (aggressive Pause / CancelJob on this owned,
		// still-running job) surfaces as context.Canceled on jobCtx while the parent
		// handler context is still alive (jobCtx.Err()!=nil, ctx.Err()==nil). It must
		// NOT be zeroed by a custom IsFailure that treats context.Canceled as
		// non-failure: zeroing falls through to the success branch and marks the
		// interrupted job COMPLETED, dropping its unfinished phases (and counting a
		// fan-out sub-job as a success). Route it through the normal fail-with-retry
		// path instead — exactly what the default IsFailure does — so the job re-runs.
		// Shutdown is handled above; a timeout yields DeadlineExceeded (not Canceled)
		// so it is unaffected.
		selfCancelled = errors.Is(err, context.Canceled) && ctx.Err() == nil && jobCtx.Err() != nil
		if !selfCancelled && !w.queue.IsFailure(job, err) {
			err = nil
		}
	}

	// The mark alone is NOT sufficient. Pause marks every id in runningJobs, and
	// processJob only removes its own id in the deferred cleanup — so a handler
	// that returns a GENUINE error at the instant a pause lands would also be
	// marked. Releasing on that would drop a real failure on the floor: no Fail,
	// no JobFailed, no attempt burned, error discarded. Require the error to
	// actually BE this worker's self-cancel.
	if err != nil && selfCancelled && w.takePauseCancelled(runToken) {
		// An aggressive pause cancelled this handler. That is an OPERATOR
		// instruction to stop, not a job failure, so it must not travel the
		// failure path: doing so burned an attempt and — at the default
		// MaxRetries, with the attempt already advanced — permanently
		// dead-lettered a job that the docs present as the reversible half of
		// Pause/Resume. It also emitted JobFailed/JobRetrying for an outcome
		// nobody chose.
		//
		// Release to pending with the attempt intact so Resume simply re-dispatches
		// it. Released on a detached context because ctx is frequently already
		// cancelled by the time we get here.
		cancelHeartbeat()
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		// RETRIED, like every other disposition write in this function. A
		// single-shot release loses to ordinary contention — most often this
		// worker's OWN poll loop, which holds an open dequeue transaction while
		// Pause fires from outside. On shared-cache SQLite that surfaces as
		// SQLITE_LOCKED, which busy_timeout does not retry; on Postgres/MySQL the
		// equivalents are a serialization failure and a lock-wait timeout.
		//
		// Losing it is expensive: Release is the ONLY write on this path, so the
		// row stays 'running' with our lock and NOTHING re-dispatches it until the
		// stale-lock reaper fires at StaleLockAge — 45 MINUTES by default. Resume()
		// re-dispatches PENDING rows, so it does not help.
		//
		// retryWithBackoff gets releaseCtx, NOT ctx: it returns immediately on a
		// context error, so handing it the frequently-already-cancelled parent
		// would quietly collapse it back to a single attempt.
		relErr := retryWithBackoff(releaseCtx, *w.config.StorageRetry, func() error {
			return w.queue.Storage().Release(releaseCtx, job.ID, w.config.WorkerID)
		})
		cancelRelease()
		// End the attempt's observability span BEFORE reporting the outcome, so it
		// is closed on every branch below. Without this the pause path is the one
		// disposition that ends no span at all: complete, fail, retry and waiting
		// each have a hook, so an aggressively-paused job leaked a span that was
		// never exported — on EVERY paused job, and pausing a busy worker leaks one
		// per in-flight job at once.
		//
		// The waiting hooks are the right shape and are reused deliberately: this
		// attempt completed neither successfully nor with failure, and the resume
		// starts a brand-new attempt with a fresh span, which is exactly the
		// fan-out/signal case they were written for. Span consumers therefore see
		// job.disposition="waiting" on this path as well as on a genuine fan-out
		// wait; the log line below is what distinguishes them. (Not JobPaused: that
		// is emitted only by Queue.PauseJob, which sets no pause mark and so never
		// reaches this branch — Worker.Pause emits WorkerPaused.)
		w.queue.CallWaitingHooks(jobCtx, job)

		// EXACTLY ONE of these fires. An earlier version added this switch but left
		// the original unconditional Info below it, so the failure branch logged an
		// error and then immediately promised a resume that was not coming.
		switch {
		case relErr != nil && !errors.Is(relErr, core.ErrJobNotOwned):
			// Say what actually happens now, not what the happy path would have
			// done. The row is still 'running' holding our lock, and NOTHING
			// re-dispatches it until the stale-lock reaper fires at StaleLockAge —
			// 45 minutes by default. Resume() re-dispatches PENDING rows, so it
			// does not help. This is the line an operator greps for during exactly
			// that incident, and it used to be followed by an unconditional Info
			// promising a resume that was not coming.
			w.logger.Error("failed to release job cancelled by aggressive pause; it will NOT re-dispatch on resume and stays locked until the stale-lock reaper reclaims it",
				"job_id", job.ID, "stale_lock_age", w.config.StaleLockAge, "error", relErr)
		case errors.Is(relErr, core.ErrJobNotOwned):
			// Another worker already owns it — the reaper or an ownership audit got
			// there first. Not our job to re-dispatch, and not an error.
			w.logger.Info("job cancelled by aggressive pause was already reclaimed by another owner",
				"job_id", job.ID)
		default:
			w.logger.Info("job released by aggressive pause; it will re-dispatch on resume", "job_id", job.ID)
		}
	} else if err != nil {
		w.queue.CallErrorHandler(jobCtx, job, err)
		w.handleError(ctx, jobCtx, job, err)
		cancelHeartbeat()
	} else {
		if w.config.BatchCompletion.Enabled && job.FanOutID == nil && w.batchCompleter != nil {
			committed, completeErr := w.batchCompleter.Submit(job.ID, resultBytes)
			if errors.Is(completeErr, errBatchCompletionClosed) {
				w.logger.Debug("batch completion accumulator closed; falling back to per-job completion",
					"job_id", job.ID)
			} else {
				if completeErr != nil {
					cancelHeartbeat()
					w.logger.Error("failed to complete job after batched retries", "job_id", job.ID, "error", completeErr)
					w.releaseAfterTerminalWriteError(ctx, job.ID, "completion")
					return
				}
				if !committed {
					cancelHeartbeat()
					w.logger.Warn("job no longer owned at completion; skipping completion handling",
						"job_id", job.ID)
					return
				}

				cancelHeartbeat()
				w.queue.CallCompleteHooks(jobCtx, job)
				w.queue.Emit(&core.JobCompleted{Job: job, Duration: time.Since(startTime), Timestamp: time.Now()})
				return
			}
		}

		completer, ok := w.queue.Storage().(completeWithResultStorage)
		if !ok {
			w.logger.Error("storage does not implement CompleteWithResult; cannot complete job", "job_id", job.ID)
			cancelHeartbeat()
			w.releaseAfterTerminalWriteError(ctx, job.ID, "completion")
			return
		}
		fo, completeErr := w.completeWithResult(ctx, completer, job.ID, resultBytes)
		cancelHeartbeat()
		if errors.Is(completeErr, core.ErrJobNotOwned) {
			w.logger.Warn("job no longer owned at completion; skipping completion handling",
				"job_id", job.ID)
			return
		}
		if completeErr != nil {
			w.logger.Error("failed to complete job after retries", "job_id", job.ID, "error", completeErr)
			w.releaseAfterTerminalWriteError(ctx, job.ID, "completion")
			return
		}

		w.queue.CallCompleteHooks(jobCtx, job)
		w.queue.Emit(&core.JobCompleted{Job: job, Duration: time.Since(startTime), Timestamp: time.Now()})
		if err := w.checkFanOutCompletion(ctx, fo); err != nil {
			w.logger.Error("failed to handle sub-job completion", "job_id", job.ID, "error", err)
		}
		return
	}
}

func (w *Worker) completeWithResult(ctx context.Context, storage completeWithResultStorage, jobID core.UUID, result []byte) (*core.FanOut, error) {
	var fo *core.FanOut
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var completeErr error
		fo, completeErr = storage.CompleteWithResult(ctx, jobID, w.config.WorkerID, result)
		return completeErr
	})
	return fo, err
}

func (w *Worker) failTerminalWithResult(ctx context.Context, storage failTerminalWithResultStorage, jobID core.UUID, errMsg string) (*core.FanOut, error) {
	var fo *core.FanOut
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var failErr error
		fo, failErr = storage.FailTerminalWithResult(ctx, jobID, w.config.WorkerID, errMsg)
		return failErr
	})
	return fo, err
}

// dispositionWriteLanded reports whether a disposition write (Fail, retry
// scheduling, terminal failure) actually reached the database, and performs the
// correct cleanup when it did not.
//
// It returns TRUE only when the caller may proceed to hooks, events and fan-out
// accounting. It returns false in both failure shapes:
//
//   - ErrJobNotOwned: the job was reclaimed or cancelled by another path, which
//     now owns the outcome. Nothing to clean up.
//   - any other error: the write did NOT land, so the row is still 'running'
//     under our lock and nothing re-dispatches it until the stale-lock reaper
//     (45 minutes by default). Release for reclaim.
//
// This exists as ONE function because the contract was previously open-coded at
// three call sites with subtly different shapes, and the two that got it wrong
// went on to fire hooks and emit events describing a state the database never
// entered. A contract repeated by hand is a contract that drifts — the same way
// two hand-synced checkpoint column lists let span_end go missing.
func (w *Worker) dispositionWriteLanded(ctx context.Context, jobID core.UUID, failErr error, action, notOwnedMsg string) bool {
	if failErr == nil {
		return true
	}
	if errors.Is(failErr, core.ErrJobNotOwned) {
		w.logger.Warn(notOwnedMsg, "job_id", jobID)
		return false
	}
	// failWithRetry already logged the underlying error; don't log it twice.
	w.releaseAfterTerminalWriteError(ctx, jobID, action)
	return false
}

func (w *Worker) releaseAfterTerminalWriteError(ctx context.Context, jobID core.UUID, action string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := w.queue.Storage().Release(releaseCtx, jobID, w.config.WorkerID); err != nil && !errors.Is(err, core.ErrJobNotOwned) {
		w.logger.Warn("failed to release job after transient terminal write error",
			"job_id", jobID,
			"action", action,
			"error", err)
	}
}

// orphanHeartbeatThreshold is the number of consecutive Heartbeat calls
// returning ErrJobNotOwned that runHeartbeat will tolerate before
// concluding the job has been reclaimed by another worker (via stale-lock
// recovery) and cancelling the in-flight handler.
//
// Set to 3 so a transient ownership blip — e.g. a clock skew between the
// worker and the DB at the moment of a lock-renewal race — doesn't kill
// a legitimate run. With a 2-minute tick, 3 consecutive failures = 6
// minutes of confirmed orphaning, which is well past any normal lock
// contention window.
const orphanHeartbeatThreshold = 3

// runHeartbeat periodically extends the job lock during execution.
// This prevents long-running jobs from being reclaimed as stale.
//
// If the heartbeat repeatedly receives core.ErrJobNotOwned, the handler
// is presumed orphaned (the stale-lock reaper at line 708 has released
// the lock and another worker has picked the job up). In that case
// runHeartbeat cancels THIS RUN's handler via cancelRun and returns, so:
//
//  1. The handler stops doing wasted work against a job it doesn't own.
//  2. The "heartbeat failed after retries / jobs: job not owned by this
//     worker" log line stops repeating forever — observed in production
//     on 2026-05-19 firing every ~2 minutes for HOURS after the job
//     was reclaimed.
//  3. Activities the orphaned handler had spawned in goroutines (e.g.
//     FireAndForgetNotification) stop racing the new handler's state
//     transitions.
//
// Non-ownership errors (DB unreachable, retry exhaustion on a transient
// error) are logged but don't trip the counter — those are operational
// issues to fix elsewhere, not orphaning.
//
// The cancel is by RUN TOKEN, not by job id. Cancelling by id would let an
// orphaned heartbeat kill whichever run currently owns the id — including
// the healthy run that REPLACED this one after a pause-release, which would
// then fail and burn an attempt it never earned.
//
// It also RENEWS this run's fleet concurrency slot on every successful beat.
// Without that the row expires at its TTL while the handler is still running,
// another worker acquires the same cap slot, and the fleet cap over-admits.
func (w *Worker) runHeartbeat(ctx context.Context, job *core.Job, runToken uint64) {
	// Heartbeat every 2 minutes (lock is 45 minutes, so plenty of buffer).
	// Tests override w.heartbeatInterval directly to drive the loop at
	// sub-second speed.
	interval := w.heartbeatInterval
	if interval <= 0 {
		interval = 2 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var consecutiveOrphanErrs int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The heartbeat deliberately CONTINUES under an aggressive pause. It
			// used to return here, which dropped the lease of a job that is still
			// running — the handler may not observe cancellation for some time,
			// and a handler that ignores ctx entirely never does. Once the lease
			// lapses the stale-lock reaper hands the job to a peer while the
			// original handler is still executing it: a pause that causes
			// double-execution. Ownership is released explicitly when the
			// pause-cancelled job returns, not by letting the lease rot.

			err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				return w.queue.Storage().Heartbeat(ctx, job.ID, w.config.WorkerID)
			})
			switch {
			case err == nil:
				consecutiveOrphanErrs = 0
				w.renewConcurrencySlots(ctx, job.ID, runToken)
				w.logger.Debug("heartbeat sent", "job_id", job.ID)
			case errors.Is(err, core.ErrJobNotOwned):
				consecutiveOrphanErrs++
				w.logger.Warn("heartbeat failed: job not owned by this worker",
					"job_id", job.ID,
					"consecutive_orphan_errs", consecutiveOrphanErrs,
					"threshold", orphanHeartbeatThreshold)
				if consecutiveOrphanErrs >= orphanHeartbeatThreshold {
					w.logger.Error("heartbeat abandoning orphaned job — cancelling handler",
						"job_id", job.ID,
						"consecutive_orphan_errs", consecutiveOrphanErrs)
					// Cancel THIS run, not whichever run currently owns the job id.
					// The pause path deliberately allows two runs of one id to be
					// alive at once, and the orphan condition belongs to the run whose
					// heartbeat failed — cancelling by id alone reaches into a
					// healthy later run and kills it, which then travels the failure
					// path and burns an attempt it never earned.
					w.cancelRun(job.ID, runToken)
					return
				}
			default:
				// Some other error (DB down, retry exhaustion on a transient
				// failure, etc.). Log but don't trip the orphan counter — these
				// are operational concerns, not ownership transfer.
				consecutiveOrphanErrs = 0
				w.logger.Warn("heartbeat failed after retries", "job_id", job.ID, "error", err)
			}
		}
	}
}

func (w *Worker) executeHandler(ctx context.Context, job *core.Job, h *handler.Handler) (resultBytes []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Check if the panicked value is an error - preserve type for special errors
			// like WaitingError that need type-based detection
			if e, ok := r.(error); ok {
				// Self-suspension signal raised via panic (fan-out or signal wait)
				if core.IsWaiting(e) {
					// Don't log as panic - this is expected behavior
					w.logger.Debug("job handler signaled waiting via panic",
						"job_id", job.ID,
						"job_type", job.Type)
					err = e
					return
				}
				// Capture stack trace for debugging - critical for production troubleshooting
				stack := debug.Stack()
				w.logger.Error("job handler panicked with error",
					"job_id", job.ID,
					"job_type", job.Type,
					"error", e,
					"stack", string(stack))
				err = e
			} else {
				// Capture stack trace for debugging - critical for production troubleshooting
				stack := debug.Stack()
				w.logger.Error("job handler panicked",
					"job_id", job.ID,
					"job_type", job.Type,
					"panic", r,
					"stack", string(stack))
				err = fmt.Errorf("panic: %v", r)
			}
		}
	}()

	// Load checkpoints for replay
	checkpoints, err := w.queue.Storage().GetCheckpoints(ctx, job.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoints: %w", err)
	}

	// Create job context with all necessary references
	jc := &intctx.JobContext{
		Job:               job,
		Storage:           w.queue.Storage(),
		WorkerID:          w.config.WorkerID,
		BestEffortReplay:  job.Determinism == int(queue.BestEffort),
		DeterminismStrict: job.Determinism == int(queue.Strict),
		Logger:            w.logger,
		HandlerLookup: func(name string) (any, bool) {
			return w.queue.GetHandler(name)
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			// The activity already ran; the checkpoint must land even if the
			// per-job deadline/cancellation fired microseconds after the handler
			// returned, or the (possibly non-idempotent) side effect re-runs on
			// replay. Strip cancellation/deadline from the INCOMING ctx (which
			// preserves a long-lived ctx supplied via CallWithCheckpointCtx) and
			// apply an independent bounded budget. WithoutCancel keeps ctx values
			// (OTel span, hooks), so tracing/propagation is unaffected.
			writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), checkpointWriteTimeout)
			defer cancel()
			return w.queue.Storage().SaveCheckpoint(writeCtx, cp)
		},
	}
	jobCtx := intctx.WithJobContext(ctx, jc)
	jobCtx = intctx.WithCallState(jobCtx, checkpoints)

	resultBytes, err = h.Execute(jobCtx, job.Args)

	// Strict determinism: after a successful replay, every recorded Call
	// checkpoint must have been consumed. An unconsumed checkpoint means the
	// handler's Call sequence changed between runs — a nondeterminism the
	// stricter mode surfaces as a terminal (non-retryable) failure. Skipped for
	// the default ExplicitCheckpoints and for BestEffort.
	if err == nil && jc.DeterminismStrict {
		if cs := intctx.GetCallState(jobCtx); cs != nil {
			if n := cs.UnconsumedCallCheckpoints(); n > 0 {
				return nil, core.NoRetry(fmt.Errorf(
					"jobs: strict determinism violation: %d recorded Call checkpoint(s) were not replayed (handler issued fewer or reordered Calls than the original run)", n))
			}
		}
	}
	return resultBytes, err
}

func (w *Worker) handleError(ctx context.Context, jobCtx context.Context, job *core.Job, err error) {
	// Decide the disposition: a scheduled retry (retryAt != nil) or a terminal
	// failure (retryAt == nil). NoRetry always wins; otherwise we retry while
	// attempts remain. This mirrors the original branch-by-branch logic.
	var retryAt *time.Time
	var noRetry *core.NoRetryError
	var retryAfter *core.RetryAfterError
	switch {
	case errors.As(err, &noRetry):
		// terminal — NoRetry overrides any remaining attempts.
	case errors.As(err, &retryAfter) && job.Attempt < job.MaxRetries:
		t := time.Now().Add(retryAfter.Delay)
		retryAt = &t
	case job.Attempt < job.MaxRetries:
		t := time.Now().Add(w.retryBackoff(job, err))
		retryAt = &t
	default:
		// terminal — attempts exhausted.
	}

	if retryAt != nil {
		// Persist the retry first. If storage reports the job is no longer owned
		// by this worker, it was reclaimed or cancelled by another path. The owner
		// is now responsible for hooks, events, and fan-out accounting.
		// If the retry was not scheduled, firing the retry hooks and emitting
		// JobRetrying would describe a state the database never entered —
		// downstream consumers would act on a reschedule that does not exist.
		// Release instead: a healthy worker reclaims within a poll, and Release
		// decrements attempt so the job still gets the retry it was owed, just
		// without the backoff delay.
		if !w.dispositionWriteLanded(ctx, job.ID,
			w.failWithRetry(ctx, job.ID, err.Error(), retryAt),
			"retry scheduling",
			"job no longer owned by this worker; skipping failure handling") {
			return
		}
		w.queue.CallRetryHooks(jobCtx, job, job.Attempt, err)
		w.queue.Emit(&core.JobRetrying{Job: job, Attempt: job.Attempt, Error: err, NextRunAt: *retryAt, Timestamp: time.Now()})
		return
	}

	if failer, ok := w.queue.Storage().(failTerminalWithResultStorage); ok {
		fo, failErr := w.failTerminalWithResult(ctx, failer, job.ID, err.Error())
		if errors.Is(failErr, core.ErrJobNotOwned) {
			w.logger.Warn("job no longer owned by this worker; skipping failure handling",
				"job_id", job.ID, "error", err)
			return
		}
		if failErr != nil {
			w.logger.Error("failed to terminally fail job after retries", "job_id", job.ID, "error", failErr)
			w.releaseAfterTerminalWriteError(ctx, job.ID, "terminal failure")
			return
		}
		w.queue.CallFailHooks(jobCtx, job, err)
		w.queue.Emit(&core.JobFailed{Job: job, Error: err, Timestamp: time.Now()})
		if handleErr := w.checkFanOutCompletion(ctx, fo); handleErr != nil {
			w.logger.Error("failed to handle sub-job failure", "job_id", job.ID, "error", handleErr)
		}
		return
	}

	// Legacy storage path: terminal failures use the original split
	// Fail+fan-out accounting sequence.
	// IncrementFanOutFailed is not idempotent, so running the accounting for a
	// terminal write that did not land double-charges the count once the reaper
	// re-runs the job.
	if !w.dispositionWriteLanded(ctx, job.ID,
		w.failWithRetry(ctx, job.ID, err.Error(), nil),
		"terminal failure",
		"job no longer owned by this worker; skipping failure handling") {
		return
	}

	// Terminal failure.
	w.queue.CallFailHooks(jobCtx, job, err)
	w.queue.Emit(&core.JobFailed{Job: job, Error: err, Timestamp: time.Now()})
	// Handle sub-job failure (resume parent if needed).
	if handleErr := w.handleSubJobCompletion(ctx, job, false); handleErr != nil {
		w.logger.Error("failed to handle sub-job failure", "job_id", job.ID, "error", handleErr)
	}
}

// failWithRetry marks a job as failed with retry on transient storage failures.
// It returns the final storage error so callers can detect a lost-ownership
// outcome (core.ErrJobNotOwned) and skip downstream side effects.
func (w *Worker) failWithRetry(ctx context.Context, jobID core.UUID, errMsg string, retryAt *time.Time) error {
	if retryAt != nil {
		now := time.Now()
		if !retryAt.After(now) {
			retryAt = &now
		}
	}

	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		return w.queue.Storage().Fail(ctx, jobID, w.config.WorkerID, errMsg, retryAt)
	})
	// ErrJobNotOwned is an expected, caller-handled outcome — don't log it as
	// an error here (the caller decides what to do about lost ownership).
	if err != nil && !errors.Is(err, core.ErrJobNotOwned) {
		w.logger.Error("failed to mark job as failed after retries", "job_id", jobID, "error", err)
	}
	return err
}

// handleSubJobCompletion updates fan-out counters and resumes parent if needed.
// Uses retry to prevent lost increments that would leave parent jobs stuck forever.
func (w *Worker) handleSubJobCompletion(ctx context.Context, job *core.Job, succeeded bool) error {
	if job.FanOutID == nil {
		return nil // Not a sub-job
	}

	var fo *core.FanOut

	// Retry the increment to prevent lost counts (which cause stuck parents).
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var incrementErr error
		if succeeded {
			fo, incrementErr = w.queue.Storage().IncrementFanOutCompleted(ctx, *job.FanOutID)
		} else {
			fo, incrementErr = w.queue.Storage().IncrementFanOutFailed(ctx, *job.FanOutID)
		}
		return incrementErr
	})
	if err != nil {
		return fmt.Errorf("failed to update fan-out after retries: %w", err)
	}
	if fo == nil {
		return nil
	}

	// Check if fan-out is complete
	return w.checkFanOutCompletion(ctx, fo)
}

// checkFanOutCompletion checks if a fan-out is complete and resumes parent.
func (w *Worker) checkFanOutCompletion(ctx context.Context, fo *core.FanOut) error {
	if fo == nil {
		return nil
	}
	done, status := fo.TerminalStatus()
	if !done {
		return nil
	}
	return w.completeFanOut(ctx, fo, status)
}

// completeFanOut marks a fan-out as complete and resumes the parent job.
// Uses atomic status update to prevent race conditions when multiple workers
// complete the last sub-jobs simultaneously.
func (w *Worker) completeFanOut(ctx context.Context, fo *core.FanOut, status core.FanOutStatus) error {
	// Atomic update: only succeeds if status is still 'pending'. This is a CAS
	// that picks at most one winner among concurrent live completers.
	updated, err := w.queue.Storage().UpdateFanOutStatus(ctx, fo.ID, status)
	if err != nil {
		return err
	}
	if !updated {
		// The status was already advanced — and, with P2's in-tx advance, the
		// VERY worker responsible for resuming the parent (the last sub-job's
		// own terminal transaction advanced the status) sees updated==false
		// here. We must NOT early-return: completeFanOut is only ever reached
		// from checkFanOutCompletion when done==true, so we fall through to the
		// idempotent cancel+resume below. ResumeJob (parent status=waiting CAS),
		// CancelSubJobs (still-pending sub-jobs only), and the local CancelJob
		// are all idempotent, so a concurrent/duplicate caller is single-effect.
		w.logger.Debug("fan-out status already terminal; proceeding to idempotent resume", "fan_out_id", fo.ID)
	}

	// Cancel remaining sub-jobs if needed. CancelSubJobs only updates the
	// DB rows — to actually stop the in-flight handlers we have to cancel
	// their contexts via w.CancelJob (one entry per local sub-job in the
	// runningJobs map). Sub-jobs running on OTHER workers in the fleet
	// won't see this signal directly; they'll notice via their heartbeat
	// returning ErrJobNotOwned and abandon after the configured threshold
	// (see runHeartbeat).
	if status == core.FanOutFailed && fo.CancelOnFail {
		cancelledIDs, err := w.queue.Storage().CancelSubJobs(ctx, fo.ID)
		if err != nil {
			w.logger.Error("failed to cancel sub-jobs", "fan_out_id", fo.ID, "error", err)
		} else {
			cancelledLocally := 0
			for _, jobID := range cancelledIDs {
				if w.CancelJob(jobID) {
					cancelledLocally++
				}
			}
			if cancelledLocally > 0 {
				w.logger.Info("cancelled in-flight sub-job handlers on this worker",
					"fan_out_id", fo.ID,
					"cancelled_locally", cancelledLocally,
					"cancelled_total", len(cancelledIDs))
			}
		}
	}

	// Resume the parent. Try once inline — the common case is that the parent has
	// already reached 'waiting', so this succeeds immediately. If it is not yet
	// resumable (still transitioning running → waiting), hand the bounded retry to
	// a tracked background goroutine so the processLoop is never blocked (it
	// previously stalled up to ~1.5s here). ctx is the worker-lifetime handler
	// context, so a shutdown cancels the retry; the pollWaitingJobs backstop is
	// the ultimate safety net.
	resumed, err := w.queue.Storage().ResumeJob(ctx, fo.ParentJobID)
	if err != nil {
		return fmt.Errorf("failed to resume parent job: %w", err)
	}
	if resumed {
		w.logger.Info("resumed parent job after fan-out completion",
			"parent_job_id", fo.ParentJobID, "fan_out_id", fo.ID, "status", status)
		return nil
	}

	parentID, fanOutID := fo.ParentJobID, fo.ID
	// A parent that is already TERMINAL is not "not yet resumable", it is never
	// resumable, and retrying is pure waste. This is the ordinary steady state with
	// CancelOnFail=false: the fan-out settles early on a failure, the parent runs to
	// a terminal status, and every sibling that finishes NATURALLY afterwards
	// arrives here. Without this check each one drove four more doomed ResumeJob
	// writes and then logged a WARN saying it was "relying on the stalled-parent
	// backstop" — for a parent no backstop will ever touch, which sends an operator
	// looking for a stall that does not exist.
	if terminal, err := w.parentIsTerminal(ctx, parentID); err == nil && terminal {
		w.logger.Debug("parent job already terminal at fan-out completion; nothing to resume",
			"parent_job_id", parentID, "fan_out_id", fanOutID, "status", status)
		return nil
	}
	w.goTracked(func() { w.resumeParentWithRetry(ctx, parentID, fanOutID, status) })
	return nil
}

// parentIsTerminal reports whether a fan-out parent has already reached a terminal
// status, i.e. no resume can ever apply to it. A read error is reported as
// not-terminal so the caller falls back to retrying, which is the safe direction:
// the cost of a needless retry is four writes, whereas wrongly skipping a resume
// strands a waiting parent until the stalled-parent backstop notices.
func (w *Worker) parentIsTerminal(ctx context.Context, parentID core.UUID) (bool, error) {
	job, err := w.queue.Storage().GetJob(ctx, parentID)
	if err != nil || job == nil {
		return false, err
	}
	return job.Status.IsTerminal(), nil
}

// resumeParentWithRetry retries ResumeJob with bounded backoff for a parent that
// has not yet reached a resumable status when its fan-out completed. It runs on a
// tracked background goroutine off the processLoop; if every attempt finds the
// parent not-yet-waiting, the pollWaitingJobs backstop heals it on a later tick.
// ctx is a worker-lifetime context (the handler context on the live completion
// path, the poll context on the backstop path), so worker shutdown cancels it and
// w.wg (via goTracked) makes shutdown wait for it — it is never the per-job ctx.
func (w *Worker) resumeParentWithRetry(ctx context.Context, parentID, fanOutID core.UUID, status core.FanOutStatus) {
	for attempt := 0; attempt < 4; attempt++ {
		delay := time.Duration(100*(1<<attempt)) * time.Millisecond // 100ms, 200ms, 400ms, 800ms
		select {
		case <-ctx.Done():
			return // shutdown; the stalled-parent backstop heals it
		case <-time.After(delay):
		}
		resumed, err := w.queue.Storage().ResumeJob(ctx, parentID)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("background parent resume failed",
					"parent_job_id", parentID, "fan_out_id", fanOutID, "error", err)
			}
			return
		}
		if resumed {
			w.logger.Info("resumed parent job after fan-out completion (background)",
				"parent_job_id", parentID, "fan_out_id", fanOutID, "status", status)
			return
		}
	}
	// Same distinction as above: if the parent reached a terminal status while we
	// were retrying, there is nothing to resume and nothing for the backstop to do,
	// so a WARN here would be actively misleading.
	if terminal, err := w.parentIsTerminal(ctx, parentID); err == nil && terminal {
		w.logger.Debug("parent job reached a terminal status during resume retries; nothing to resume",
			"parent_job_id", parentID, "fan_out_id", fanOutID)
		return
	}
	w.logger.Warn("parent job not yet resumable after background retries; relying on the stalled-parent backstop",
		"parent_job_id", parentID, "fan_out_id", fanOutID)
}

func (w *Worker) calculateBackoff(attempt int) time.Duration {
	maxBackoff := w.config.MaxRetryBackoff
	if maxBackoff <= 0 {
		maxBackoff = time.Minute
	}
	shift := attempt
	if shift < 0 {
		shift = 0
	}
	if shift > 30 {
		shift = 30
	}

	backoff := time.Second << uint(shift)
	if backoff <= 0 || backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

func (w *Worker) retryBackoff(job *core.Job, err error) time.Duration {
	policy := w.config.JobBackoff
	if h, ok := w.queue.GetHandler(job.Type); ok && h.Backoff != nil {
		policy = h.Backoff
	}
	if policy == nil {
		policy = DefaultBackoffPolicy()
	}

	delay := policy.NextRetry(job.Attempt, err)
	maxBackoff := w.config.MaxRetryBackoff
	if maxBackoff <= 0 {
		maxBackoff = time.Minute
	}
	if delay > maxBackoff {
		return maxBackoff
	}
	if delay <= 0 {
		return time.Nanosecond
	}
	return delay
}

// maxCatchUpIterations bounds the seed scan so a pathologically dense schedule
// (e.g. millisecond interval over a long outage) cannot spin. Real schedules
// are far coarser; hitting the cap falls back to "no catch-up" (resume from now).
const maxCatchUpIterations = 100_000

// seedLastRun computes the lastRun cursor for a scheduled job so that the very
// next schedule.Next(seedLastRun(...)) yields the most-recent boundary that is
// already due (<= now) when one or more boundaries were missed, causing exactly
// one catch-up fire, after which natural cadence resumes. When no boundary is
// due (fresh start or no gap) it returns persisted unchanged. Pure: no clock,
// no storage.
// The returned cappedCatchUp is true when the scan hit maxCatchUpIterations and
// fell back to "resume from now", silently dropping the missed boundaries — the
// caller logs a warning so this is observable rather than invisible.
func seedLastRun(schedule schedule.Schedule, persisted, now time.Time) (cursor time.Time, cappedCatchUp bool) {
	next := schedule.Next(persisted)
	if next.IsZero() || next.After(now) {
		return persisted, false
	}

	prev := persisted
	iter := 0
	for {
		n2 := schedule.Next(next)
		if n2.IsZero() || n2.After(now) {
			return prev, false
		}
		prev = next
		next = n2
		iter++
		if iter >= maxCatchUpIterations {
			return now, true
		}
	}
}

// establishScheduleBase computes the fire-boundary cursor for a schedule the
// first time this worker sees it, in a way that is consistent across the fleet.
//
//   - If a prior fire (or anchor) is persisted, it seeds from that shared value
//     — running at most one catch-up fire for boundaries missed while the whole
//     fleet was down (seedLastRun).
//   - If the schedule is fresh (no persisted value), it anchors a shared base in
//     storage via SeedScheduledFire(name, now). This records last_fire_at (the
//     anchor cursor) but never last_fired_at (real fires only — the UI's
//     last_run depends on that distinction), so anti-boot-storm holds (the
//     first real fire is one
//     interval later), and — crucially — every worker then derives the SAME next
//     boundary from that shared anchor. Without it, two workers seeing a fresh
//     schedule at slightly skewed local times would compute different nextRun
//     values and each claim its own, double-firing the first tick.
//
// For absolute schedules (cron/daily/weekly) Next() snaps to the same wall-clock
// boundary regardless of base, so they were never skew-sensitive; the anchor is
// what protects interval (Every) schedules. Storage backends that don't persist
// fire times fall back to the local clock (single-worker deployments are
// unaffected; multi-worker without persistence cannot be coordinated anyway).
func (w *Worker) establishScheduleBase(ctx context.Context, name string, sched schedule.Schedule, now time.Time) time.Time {
	reader, ok := w.queue.Storage().(scheduledFireReader)
	if !ok {
		return now
	}
	persisted, found, err := reader.GetScheduledFireTime(ctx, name)
	if err != nil {
		w.logger.Error("failed to read scheduled fire time", "name", name, "error", err)
		return now
	}
	if found && persisted.After(time.Unix(0, 0).UTC()) {
		cursor, capped := seedLastRun(sched, persisted, now)
		if capped {
			w.logger.Warn("scheduled job catch-up exceeded the iteration cap; missed boundaries were dropped and the schedule resumes from now",
				"name", name,
				"persisted_last_fire_at", persisted,
				"max_catch_up_iterations", maxCatchUpIterations)
		}
		return cursor
	}
	// Fresh schedule: anchor a shared base via insert-if-absent so the whole
	// fleet derives the same first boundary. This does NOT fire or advance the
	// boundary — it only records the starting cursor. Backends that don't
	// support seeding fall back to the local clock (fine for single-worker).
	if seeder, ok := w.queue.Storage().(scheduledFireSeeder); ok {
		base, err := seeder.SeedScheduledFire(ctx, name, now)
		if err != nil {
			w.logger.Error("failed to anchor schedule base", "name", name, "error", err)
			return now
		}
		if base.After(time.Unix(0, 0).UTC()) {
			return base
		}
	}
	return now
}

// Per-schedule retry backoff for a GENUINE scheduled-fire failure. The scheduler
// ticks at 10Hz; without a backoff every failing schedule costs one transaction
// and one ERROR log per tick for as long as the failure lasts. The first retry is
// still one tick later, so a transient blip recovers as fast as before.
const (
	scheduleFireRetryBase = 100 * time.Millisecond
	scheduleFireRetryMax  = 30 * time.Second
)

// scheduleFireRetryDelay doubles from scheduleFireRetryBase and saturates at
// scheduleFireRetryMax. Deliberately jitter-free: a fire claim is a single-row
// transaction, the fleet-wide herd is bounded by the worker count, and
// determinism keeps it testable.
func scheduleFireRetryDelay(consecutiveFailures int) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	delay := scheduleFireRetryBase
	for i := 1; i < consecutiveFailures; i++ {
		if delay >= scheduleFireRetryMax {
			break
		}
		delay *= 2
	}
	if delay > scheduleFireRetryMax {
		return scheduleFireRetryMax
	}
	return delay
}

func (w *Worker) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastRun := make(map[string]time.Time)
	// Per-schedule backoff state for GENUINE fire failures, and a once-per-name
	// latch for schedules that can never fire.
	fireFailures := make(map[string]int)
	fireRetryAt := make(map[string]time.Time)
	neverFiresLogged := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduled := w.queue.GetScheduledJobs()

			if scheduled == nil {
				continue
			}

			now := time.Now()
			for name, sj := range scheduled {
				// A schedule whose last attempt failed for a GENUINE reason is backing
				// off. lastRun is not advanced while it does, so the due boundary is
				// still due when the backoff expires — nothing is dropped, it is just
				// not re-attempted at the tick rate.
				if scheduleIsBackingOff(fireRetryAt, name, now) {
					continue
				}
				if _, ok := lastRun[name]; !ok {
					// First sight of this schedule: establish a fire-boundary base
					// that every worker in the fleet agrees on, so skewed wall
					// clocks cannot make two workers target different boundaries
					// for the same logical tick (which would double-fire).
					lastRun[name] = w.establishScheduleBase(ctx, name, sj.Schedule, now)
				}
				// Collapse a BACKLOG to a single catch-up fire, the same way the
				// cold-start path does. establishScheduleBase runs once per schedule
				// per process, so after it the durable cursor is never re-read and
				// lastRun[name] is advanced one boundary per successful fire. A storage
				// outage the worker SURVIVES therefore leaves lastRun stale by
				// outage/period boundaries, and this loop then fires every one of them
				// — one per 100ms tick, i.e. at 10 Hz, each a real Enqueue. The
				// genuine-failure backoff makes it worse, since more boundaries elapse
				// while it waits.
				//
				// seedLastRun is exactly the clamp for this and is pure (no clock, no
				// storage), so it can run every tick: it returns the cursor unchanged
				// when zero or one boundary is due, and only when TWO OR MORE are due
				// does it skip to the most recent, yielding one fire instead of N. Its
				// own doc already states the intended contract ("causing exactly one
				// catch-up fire, after which natural cadence resumes") — the cold path
				// implemented it and the warm path did not.
				//
				// Fleet safety is unaffected: the durable claim in EnqueueScheduledFire
				// remains the authority on which boundary is consumed, so clamping only
				// reduces how many doomed claims this worker attempts.
				if cursor, capped := seedLastRun(sj.Schedule, lastRun[name], now); !cursor.Equal(lastRun[name]) {
					skipped := cursor
					lastRun[name] = cursor
					if capped {
						w.logger.Warn("scheduled job catch-up exceeded the iteration cap after a gap; missed boundaries were dropped and the schedule resumes from now",
							"job_type", name, "max_catch_up_iterations", maxCatchUpIterations)
					} else {
						w.logger.Info("scheduled job fell behind; collapsing the missed boundaries to a single catch-up fire",
							"job_type", name, "resumed_from", skipped)
					}
				}
				nextRun := sj.Schedule.Next(lastRun[name])
				if scheduleNeverFires(nextRun) {
					// The schedule has no future fire (an unsatisfiable cron such as
					// "0 0 30 2 *"; cron.SpecSchedule.Next returns the zero time when
					// nothing matches within five years). Next is pure in its input and
					// lastRun is not advanced, so this is permanent. Without the guard
					// the zero time is "due" — every instant is after it — and every
					// tick runs a doomed claim transaction forever, silently.
					if !neverFiresLogged[name] {
						neverFiresLogged[name] = true
						w.logger.Error("scheduled job never fires: its schedule has no future boundary, so it is skipped",
							"name", name, "cursor", lastRun[name])
					}
					continue
				}
				if now.After(nextRun) || now.Equal(nextRun) {
					// Build the enqueue options first, then claim the fire boundary
					// and enqueue the job ATOMICALLY via EnqueueScheduledFire: if the
					// enqueue fails its transaction rolls back the claim, so the
					// boundary stays re-claimable instead of advancing the durable
					// cursor and silently dropping a due run (teardown g8).
					opts := []queue.Option{
						queue.QueueOpt(sj.Options.Queue),
						queue.Priority(sj.Options.Priority),
						queue.Retries(sj.Options.MaxRetries),
					}
					if sj.Options.UniqueKey != "" {
						opts = append(opts, queue.Unique(sj.Options.UniqueKey))
					}
					if sj.Options.Delay > 0 {
						opts = append(opts, queue.Delay(sj.Options.Delay))
					}
					if sj.Options.RunAt != nil {
						opts = append(opts, queue.At(*sj.Options.RunAt))
					}
					if sj.Options.Timeout > 0 {
						opts = append(opts, queue.Timeout(sj.Options.Timeout))
					}
					opts = append(opts, queue.Determinism(sj.Options.Determinism))
					// Forward the remaining configured options so a scheduled fire is
					// not silently stripped of its tenant/tags/dedup (arch-10x/F3).
					if sj.Options.Tenant != "" {
						opts = append(opts, queue.WithTenant(sj.Options.Tenant))
					}
					if sj.Options.Metadata != nil {
						opts = append(opts, queue.WithMetadata(map[string]string(*sj.Options.Metadata)))
					}
					// Forward unconditionally (incl. 0 = unlimited) so an author's
					// explicit metadata-size choice isn't reset to the default on fires.
					opts = append(opts, queue.WithMaxMetadataSize(sj.Options.MaxMetadataSize))
					// Handler backoff is intentionally NOT forwarded: Options.Backoff is
					// only read by RegisterE at handler-registration time, and the retry
					// path resolves backoff from the registered handler keyed by job
					// type — never from enqueue options — so it would be a no-op here.
					// Dedup controls are mutually exclusive (each constructor zeroes the
					// other). A fixed IdempotencyKey on a RECURRING schedule dedups fires
					// within UniqueLockTTL (at-most-once-per-window) — the schedule
					// author's explicit choice, honored rather than silently dropped.
					if sj.Options.IdempotencyKey != "" {
						opts = append(opts, queue.IdempotencyKey(sj.Options.IdempotencyKey, sj.Options.UniqueLockTTL))
					} else if sj.Options.UniqueForTTL > 0 {
						opts = append(opts, queue.UniqueFor(sj.Options.UniqueForTTL))
					}
					_, _, err := w.queue.EnqueueScheduledFire(ctx, name, nextRun, sj.Name, sj.Args, opts...)
					w.applyScheduleFireDisposition(scheduleFireOutcome{
						name: name, nextRun: nextRun, now: now, err: err,
						failures: fireFailures, retryAt: fireRetryAt, lastRun: lastRun,
					})
				}
			}
		}
	}
}

// pollWaitingJobs periodically checks for waiting jobs that should be resumed.
// This is a fallback mechanism in case event-driven resume fails.
func (w *Worker) pollWaitingJobs(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// If the backend supports a recovery lease, only the worker holding it runs
	// the scan each tick. This bounds the cost of the recovery queries to one
	// scan per tick for the whole fleet instead of one per worker. The primary,
	// event-driven resume path (completeFanOut, on every worker) is unaffected;
	// this poll is only the fallback for missed resumes and stalled parents.
	leaser, hasLeaser := w.queue.Storage().(recoveryLeaser)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if hasLeaser {
				held, err := leaser.TryAcquireRecoveryLease(ctx, recoveryLeaseName, w.config.WorkerID, recoveryLeaseTTL)
				if err != nil {
					// Don't let a lease hiccup wedge recovery — resume is
					// idempotent, so scanning anyway is safe.
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						w.logger.Warn("recovery lease acquisition failed; scanning anyway", "error", err)
					}
				} else if !held {
					continue
				}
			}
			w.pollWaitingJobsOnce(ctx)
		}
	}
}

func (w *Worker) pollWaitingJobsOnce(ctx context.Context) {
	var jobs []*core.Job
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var queryErr error
		jobs, queryErr = w.queue.Storage().GetWaitingJobsToResume(ctx)
		return queryErr
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			w.logger.Error("failed to get waiting jobs after retries", "error", err)
		}
		// Do not return. Each recovery scan below guards its own (nil-on-error)
		// result, and the signal/timer resume backstop at the end MUST run even
		// when an earlier OPTIONAL scan fails: this whole function runs only on
		// the single recovery-lease holder, so a recurring error in one scan
		// returning early would freeze every durable timer/signal in the fleet
		// (teardown g8). Log and fall through to the independent next block.
	}
	for _, job := range jobs {
		resumeErr := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			_, err := w.queue.Storage().ResumeJob(ctx, job.ID)
			return err
		})
		if resumeErr != nil {
			w.logger.Error("failed to resume waiting job after retries", "job_id", job.ID, "error", resumeErr)
		} else {
			w.logger.Info("resumed waiting job via polling fallback", "job_id", job.ID)
		}
	}

	stalledCutoff := time.Now().Add(-w.config.FanOutRecoveryStaleAge)
	var stalled []*core.Job
	err = retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var queryErr error
		stalled, queryErr = w.queue.Storage().GetStalledFanOutParents(ctx, stalledCutoff)
		return queryErr
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			w.logger.Error("failed to get stalled fan-out parents after retries", "error", err)
		}
		// Log-and-continue (see the GetWaitingJobsToResume block above): an
		// optional fan-out scan failure must not skip the signal/timer backstop.
	}
	for _, job := range stalled {
		resumeErr := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			_, err := w.queue.Storage().ResumeJob(ctx, job.ID)
			return err
		})
		if resumeErr != nil {
			w.logger.Error("failed to resume stalled fan-out parent after retries", "job_id", job.ID, "error", resumeErr)
		} else {
			w.logger.Info("resumed stalled fan-out parent via polling fallback", "job_id", job.ID)
		}
	}

	// Rescue fan-outs left status='pending' with terminal counts and a waiting
	// parent — the post-crash strand where a worker died between the
	// counter-increment commit and the status advance (or any non-atomic
	// increment path). Each row is driven through checkFanOutCompletion →
	// completeFanOut, the SAME UpdateFanOutStatus CAS + idempotent resume the
	// live path uses, so this can never double-resume. Reuses stalledCutoff and
	// inherits the recovery-lease gating applied by pollWaitingJobs.
	if cps, ok := w.queue.Storage().(completablePendingFanOutStorage); ok {
		var completable []*core.FanOut
		err = retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			var queryErr error
			completable, queryErr = cps.GetCompletablePendingFanOuts(ctx, stalledCutoff)
			return queryErr
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to get completable pending fan-outs after retries", "error", err)
			}
			// Log-and-continue: must not skip the signal/timer backstop below.
		}
		for _, fo := range completable {
			if resumeErr := w.checkFanOutCompletion(ctx, fo); resumeErr != nil {
				w.logger.Error("failed to complete stranded pending fan-out", "fan_out_id", fo.ID, "error", resumeErr)
			} else {
				w.logger.Info("rescued stranded pending fan-out via polling fallback", "fan_out_id", fo.ID)
			}
		}
	}

	if afs, ok := w.queue.Storage().(abandonedFanOutStorage); ok {
		var cleaned int64
		err = retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			var qErr error
			cleaned, qErr = afs.CleanAbandonedFanOuts(ctx, stalledCutoff)
			return qErr
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to clean abandoned fan-outs after retries", "error", err)
			}
			// Log-and-continue: must not skip the signal/timer backstop below.
		}
		if cleaned > 0 {
			w.logger.Info("cleaned abandoned pending fan-outs", "count", cleaned)
		}
	}

	// Resume jobs waiting on a signal that has arrived (or whose timeout wake
	// deadline has passed). This is the backstop for the deliver-vs-suspend
	// race — a signal delivered just before MarkWaiting commits would otherwise
	// miss the event-driven resume and leave the job waiting forever.
	if sr, ok := w.queue.Storage().(signalResumeStorage); ok {
		w.pollSignalWaitingJobs(ctx, sr)
	}
}

func (w *Worker) pollSignalWaitingJobs(ctx context.Context, sr signalResumeStorage) {
	w.pruneExpiredFutureSleepMemos(time.Now())
	batchSize := signalResumePollBatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	pager, paged := w.queue.Storage().(signalResumePager)
	signalNames, canReadSignalNames := w.queue.Storage().(pendingSignalNameReader)
	afterJobID := core.NilUUID

	for {
		var sigWaiting []*core.Job
		err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			var queryErr error
			if paged {
				sigWaiting, queryErr = pager.GetSignalWaitingJobsToResumeAfter(ctx, afterJobID, batchSize)
			} else {
				sigWaiting, queryErr = sr.GetSignalWaitingJobsToResume(ctx)
			}
			return queryErr
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to get signal-waiting jobs after retries", "error", err)
			}
			return
		}
		if len(sigWaiting) == 0 {
			return
		}
		for _, job := range sigWaiting {
			if job == nil {
				continue
			}
			afterJobID = job.ID
			if w.waitingOnMemoizedFutureSleep(job) {
				w.logger.Debug("suppressed signal resume for durable timer",
					"job_id", job.ID,
					"run_at", job.RunAt,
					"memoized", true)
				continue
			}
			if signal.WaitingOnFutureSleep(ctx, w.queue.Storage(), job, w.logger) {
				w.memoizeFutureSleep(job)
				continue
			}
			signalName := ""
			hasPendingSignal := false
			if canReadSignalNames {
				var nameErr error
				signalName, hasPendingSignal, nameErr = signalNames.GetPendingSignalName(ctx, job.ID)
				if nameErr != nil {
					w.logger.Warn("failed to inspect pending signal before resume event", "job_id", job.ID, "error", nameErr)
				}
			}
			w.clearFutureSleepMemo(job.ID)
			resumed := false
			resumeErr := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				var err error
				resumed, err = sr.ResumeSignalWaitingJob(ctx, job.ID)
				return err
			})
			if resumeErr != nil {
				w.logger.Error("failed to resume signal-waiting job after retries", "job_id", job.ID, "error", resumeErr)
			} else {
				if resumed && hasPendingSignal {
					w.queue.Emit(&core.JobResumedBySignal{JobID: job.ID, SignalName: signalName, Timestamp: time.Now()})
				}
				w.logger.Info("resumed signal-waiting job via polling fallback", "job_id", job.ID)
			}
		}
		if !paged || len(sigWaiting) < batchSize {
			return
		}
	}
}

func (w *Worker) waitingOnMemoizedFutureSleep(job *core.Job) bool {
	if job == nil || job.RunAt == nil {
		return false
	}
	if !job.RunAt.After(time.Now()) {
		w.clearFutureSleepMemo(job.ID)
		return false
	}
	runAt := job.RunAt.UnixNano()
	w.futureSleepMu.Lock()
	memoizedRunAt, ok := w.futureSleepSuppressions[job.ID]
	w.futureSleepMu.Unlock()
	return ok && memoizedRunAt == runAt
}

func (w *Worker) memoizeFutureSleep(job *core.Job) {
	if job == nil || job.RunAt == nil {
		return
	}
	w.futureSleepMu.Lock()
	w.futureSleepSuppressions[job.ID] = job.RunAt.UnixNano()
	w.futureSleepMu.Unlock()
}

func (w *Worker) clearFutureSleepMemo(jobID core.UUID) {
	w.futureSleepMu.Lock()
	delete(w.futureSleepSuppressions, jobID)
	w.futureSleepMu.Unlock()
}

func (w *Worker) pruneExpiredFutureSleepMemos(now time.Time) {
	nowUnix := now.UnixNano()
	w.futureSleepMu.Lock()
	for jobID, runAtUnix := range w.futureSleepSuppressions {
		if runAtUnix <= nowUnix {
			delete(w.futureSleepSuppressions, jobID)
		}
	}
	w.futureSleepMu.Unlock()
}

// emitReclaimed publishes a JobReclaimed event and fires the OnJobReclaimed
// hooks for a single reclaimed job. It is best-effort observability only: the
// emit may be dropped on full subscriber buffers and the hook list is copied
// under RLock before invocation, matching every other Call*Hooks. A duplicate
// emit is harmless (it only nudges a monotonic counter).
func (w *Worker) emitReclaimed(ctx context.Context, jobID core.UUID, reason string) {
	w.queue.Emit(&core.JobReclaimed{
		JobID:     jobID,
		WorkerID:  w.config.WorkerID,
		Reason:    reason,
		Timestamp: time.Now(),
	})
	w.queue.CallJobReclaimedHooks(ctx, jobID, reason)
}

// reapStaleLocks periodically releases locks on jobs that are stuck in running
// status with expired locks. This handles cases where:
// - A worker crashed without properly completing/failing the job
// - Complete/Fail failed due to ErrJobNotOwned (lock expired during processing)
// - A handler hung and the heartbeat eventually stopped
func (w *Worker) reapStaleLocks(ctx context.Context) {
	// Defensive: NewWorker guarantees a positive interval, but guard against a
	// zero value (which would panic time.NewTicker) in case the config field
	// is set directly.
	interval := w.config.StaleLockInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			released, err := w.queue.Storage().ReleaseStaleLocks(ctx, w.config.StaleLockAge)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("failed to release stale locks", "error", err)
				}
				continue
			}
			if len(released) == 0 {
				continue
			}

			// Cancel any local in-flight handlers for jobs whose locks
			// were just released. The DB-level release already reverted
			// the lock fields; without this loop the original handler
			// would keep running until its own heartbeat-abandon timer
			// fires (~6 minutes by default). This brings the local
			// cancel latency down to "next heartbeat tick."
			cancelledLocally := 0
			for _, jobID := range released {
				// A reclaim is observable even when the original handler ran
				// on a different worker (no local cancel target); emit for
				// every released ID so the leading crash-indicator is visible.
				w.emitReclaimed(ctx, jobID, core.ReclaimReasonStaleLock)
				if w.CancelJob(jobID) {
					cancelledLocally++
				}
			}
			w.logger.Info("released stale running jobs",
				"count", len(released),
				"cancelled_locally", cancelledLocally)
		}
	}
}

// runOwnershipAudit periodically checks whether any of this worker's
// running jobs have been cancelled or reclaimed by another worker, and
// cancels the corresponding local handler context. This is the
// cross-worker counterpart of the cancellation logic in completeFanOut
// and reapStaleLocks (which only see local sub-jobs).
//
// The query cost is one row per running job per tick — bounded by THIS
// worker's concurrency, not by the size of the fleet.
//
// No grace window is needed for newly-acquired jobs: a job only enters
// runningJobs (in processJob) after Dequeue has returned, and Dequeue
// commits locked_by=this-worker before returning. So any ID in the
// snapshot already has its ownership row persisted, and a freshly
// dequeued job can't be mis-flagged as orphaned.
func (w *Worker) runOwnershipAudit(ctx context.Context) {
	ticker := time.NewTicker(w.config.OwnershipAuditInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Snapshot the IDs we think we own. Holding the mutex during
			// the DB call would block dequeue/complete; copy and release.
			w.runningJobsMu.Lock()
			ids := make([]core.UUID, 0, len(w.runningJobs))
			for id := range w.runningJobs {
				ids = append(ids, id)
			}
			w.runningJobsMu.Unlock()
			if len(ids) == 0 {
				continue
			}

			orphaned, err := w.queue.Storage().FindOrphanedJobs(ctx, ids, w.config.WorkerID)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Warn("ownership audit query failed", "error", err)
				}
				continue
			}
			if len(orphaned) == 0 {
				continue
			}

			cancelled := 0
			for _, id := range orphaned {
				// Emit for every orphaned ID — a peer reclaiming our in-flight
				// job is observable even if the local handler already exited.
				w.emitReclaimed(ctx, id, core.ReclaimReasonOwnershipAudit)
				if w.CancelJob(id) {
					cancelled++
				}
			}
			if cancelled > 0 {
				w.logger.Warn("ownership audit cancelled orphaned local handlers",
					"orphaned_count", len(orphaned),
					"cancelled_count", cancelled,
					"audit_interval", w.config.OwnershipAuditInterval)
			}
		}
	}
}

// takePauseCancelled reports whether this job's handler was cancelled by an
// aggressive pause, clearing the mark so it is consumed exactly once.
//
// takePauseCancelled reports whether THIS run's handler was cancelled by an
// aggressive pause, consuming the mark if so.
//
// Consuming it matters: a job released by a pause is re-dispatched on resume, and
// a surviving mark would make a genuine failure on that later run look like "just
// a pause" and be released forever.
func (w *Worker) takePauseCancelled(runToken uint64) bool {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()
	if _, ok := w.pauseCancelled[runToken]; !ok {
		return false
	}
	delete(w.pauseCancelled, runToken)
	return true
}

// Pause pauses the worker.
func (w *Worker) Pause(mode core.PauseMode) {
	w.pauseMode.Store(mode)
	w.paused.Store(true)

	if mode == core.PauseModeAggressive {
		// Cancel every running handler, and MARK each one first. Without the mark
		// the resulting context.Canceled is indistinguishable from a handler that
		// failed, so it fell through the normal failure path: the attempt was
		// burned and — at the default MaxRetries, with the attempt already
		// advanced — the job was permanently DEAD-LETTERED by an operation
		// documented as the reversible half of Pause/Resume.
		w.runningJobsMu.Lock()
		for _, rj := range w.runningJobs {
			w.pauseCancelled[rj.token] = struct{}{}
			rj.cancel()
		}
		w.runningJobsMu.Unlock()
	}

	// Emit event
	w.queue.Emit(&core.WorkerPaused{
		WorkerID:  w.config.WorkerID,
		Mode:      mode,
		Timestamp: time.Now(),
	})
}

// CancelJob cancels a specific running job's context.
// Returns true if the job was found and cancelled.
func (w *Worker) CancelJob(jobID core.UUID) bool {
	w.runningJobsMu.Lock()
	rj, ok := w.runningJobs[jobID]
	cancel := rj.cancel
	w.runningJobsMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// cancelRun cancels a SPECIFIC run of a job, and does nothing if that run is no
// longer the one registered for the id.
//
// CancelJob is the right tool for an operator cancelling "that job" — whichever
// run is current. It is the wrong tool for a condition that belongs to one
// particular run, such as an orphaned heartbeat: since the pause path lets two
// runs of one id be alive at once, cancelling by id there reaches past the failed
// run into a healthy later one.
//
// The other two CancelJob call sites are by-id ON PURPOSE and should stay that
// way: the fan-out CancelOnFail sweep cancels CHILD ids the caller holds no token
// for, and the stale-lock reaper cancels ids whose storage row was reclaimed, so
// any local run of that id is orphaned regardless of which one it is.
func (w *Worker) cancelRun(jobID core.UUID, runToken uint64) bool {
	w.runningJobsMu.Lock()
	rj, ok := w.runningJobs[jobID]
	w.runningJobsMu.Unlock()
	if !ok || rj.token != runToken {
		return false
	}
	rj.cancel()
	return true
}

// Resume lifts the pause and lets the poll loop dispatch again.
//
// It deliberately does NOT clear the pause-cancel marks. It used to, and that
// bulk clear reintroduced the very bug the marks exist to prevent: a handler
// still blocked in I/O has not yet surfaced its cancellation, so an operator who
// resumes promptly wiped its mark, and the context.Canceled that arrived a moment
// later fell through to the ordinary failure path — attempt burned, and
// dead-lettered on the last one. Marks are now dropped per job in processJob's
// own cleanup, which is exact and covers the same leak.
func (w *Worker) Resume() {
	w.paused.Store(false)

	// Emit event
	w.queue.Emit(&core.WorkerResumed{
		WorkerID:  w.config.WorkerID,
		Timestamp: time.Now(),
	})
}

// IsPaused returns true if the worker is paused.
func (w *Worker) IsPaused() bool {
	return w.paused.Load()
}

// WorkerHealth is a point-in-time snapshot of local worker state.
type WorkerHealth struct {
	// RunningCount is the number of jobs currently executing on this worker.
	RunningCount int

	// Paused reports whether this worker is operator-paused.
	Paused bool

	// Started reports whether Start is currently running.
	Started bool
}

// Health returns a point-in-time snapshot of local worker state.
func (w *Worker) Health() WorkerHealth {
	return WorkerHealth{
		RunningCount: w.RunningJobCount(),
		Paused:       w.IsPaused(),
		Started:      w.started.Load(),
	}
}

// HealthHandler returns a standalone probe handler for headless workers.
//
// The returned handler registers /healthz and /readyz. /healthz is a liveness
// probe: it always returns 200 OK and performs zero database work. /readyz is a
// readiness probe: it returns 200 OK when the storage backend either does not
// expose storage.Healther or its Ping method succeeds, and returns 503 Service
// Unavailable when Ping fails.
//
// Operator pause is a reversible control-plane state, not a readiness failure:
// pausing the worker does not make /readyz return 503.
func (w *Worker) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, r *http.Request) {
		healther, ok := w.queue.Storage().(storage.Healther)
		if !ok {
			// Readiness cannot verify backing storage without the optional Ping
			// capability, so degrade to ready because there is no positive
			// evidence the worker is unhealthy.
			rw.WriteHeader(http.StatusOK)
			return
		}

		// Operator pause is a reversible control-plane state, not a readiness
		// failure. Do not consult IsPaused here or orchestration will restart
		// deliberately quiesced workers.
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		defer cancel()
		if err := healther.Ping(ctx); err != nil {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.WriteHeader(http.StatusOK)
	})
	return mux
}

// PauseMode returns the current pause mode.
func (w *Worker) PauseMode() core.PauseMode {
	mode := w.pauseMode.Load()
	if mode == nil {
		return core.PauseModeGraceful
	}
	return mode.(core.PauseMode)
}

// RunningJobCount returns the number of currently running jobs.
func (w *Worker) RunningJobCount() int {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()
	return len(w.runningJobs)
}

// WaitForPause blocks until all running jobs complete or the timeout expires.
// Returns nil if all jobs completed, or an error if timeout was reached.
// The worker must be paused before calling this method.
func (w *Worker) WaitForPause(timeout time.Duration) error {
	if !w.IsPaused() {
		return errors.New("worker is not paused")
	}

	deadline := time.Now().Add(timeout)
	pollInterval := 50 * time.Millisecond

	for {
		w.runningJobsMu.Lock()
		count := len(w.runningJobs)
		w.runningJobsMu.Unlock()

		if count == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d running jobs to complete", count)
		}

		time.Sleep(pollInterval)
	}
}

// runningJobEntry is one execution of a job on this worker.
//
// token identifies THAT run. Go cannot compare funcs, and every closure from a
// single context.WithCancel call site shares a code pointer, so the cancel func
// itself cannot serve as identity — an explicit monotonic token can.
type runningJobEntry struct {
	cancel context.CancelFunc
	token  uint64
}

// slotHold is the concurrency-slot row one RUN holds. jobID is carried alongside
// the names because the row is keyed (slot_name, job_id) in the database while
// this map is keyed by run token — see Worker.slotJobID and releaseConcurrencySlots.
type slotHold struct {
	jobID core.UUID
	names []string
}

// scheduleFireOutcome is one EnqueueScheduledFire attempt plus the scheduler's
// local bookkeeping.
type scheduleFireOutcome struct {
	name     string
	nextRun  time.Time
	now      time.Time
	err      error
	failures map[string]int
	retryAt  map[string]time.Time
	lastRun  map[string]time.Time
}

// applyScheduleFireDisposition records the outcome of one scheduled-fire attempt.
//
// Extracted from runScheduler's loop so it can be tested: driving the loop itself
// requires real time to pass, so the three dispositions previously had no
// coverage at all and swapping any of them for the old log-and-continue behaviour
// left the suite green. The distinction between them is the whole point of the
// change — whether a boundary is skipped once or retried at 10 Hz forever.
func (w *Worker) applyScheduleFireDisposition(o scheduleFireOutcome) {
	switch {
	case errors.Is(o.err, core.ErrDuplicateJob):
		// DELIBERATE SKIP, not a failure: the schedule declared queue.Unique and a
		// previous fire is still live, so running a second instance is exactly what
		// the author asked us not to do. EnqueueScheduledFire COMMITTED the claim,
		// so the durable cursor already advanced and peers will not re-attempt this
		// boundary. Advance locally too, and log at Info — a normal outcome.
		w.logger.Info("scheduled fire skipped: a job with this schedule's unique key is still active",
			"name", o.name, "fire_time", o.nextRun)
		delete(o.failures, o.name)
		delete(o.retryAt, o.name)
		o.lastRun[o.name] = o.nextRun
	case o.err != nil:
		// GENUINE failure. Claim+enqueue are atomic, so this rolled back the claim
		// and the boundary stays re-claimable. Do NOT advance lastRun — retry the
		// same boundary rather than drop the fire — but back off, so a persistent
		// failure does not cost one transaction and one ERROR log every 100ms tick.
		o.failures[o.name]++
		delay := scheduleFireRetryDelay(o.failures[o.name])
		o.retryAt[o.name] = o.now.Add(delay)
		w.logger.Error("failed to claim+enqueue scheduled fire; will retry boundary",
			"name", o.name, "fire_time", o.nextRun, "error", o.err,
			"consecutive_failures", o.failures[o.name], "retry_in", delay)
	default:
		// Either we claimed and enqueued, or a peer already claimed this boundary;
		// in both cases this worker is done with it.
		delete(o.failures, o.name)
		delete(o.retryAt, o.name)
		o.lastRun[o.name] = o.nextRun
	}
}

// scheduleIsBackingOff reports whether this schedule is inside a failure backoff
// window and must not be re-attempted on this tick.
//
// This is the gate that actually CONSUMES the delay applyScheduleFireDisposition
// records — the only thing standing between a persistently failing schedule and
// one claim transaction plus one ERROR log every 100ms tick. It is a named
// function rather than an inline condition so that consuming the delay can be
// tested, not just computing it.
func scheduleIsBackingOff(fireRetryAt map[string]time.Time, name string, now time.Time) bool {
	retryAt, backingOff := fireRetryAt[name]
	return backingOff && now.Before(retryAt)
}

// scheduleNeverFires reports whether a schedule has no future fire at all.
//
// cron.SpecSchedule.Next returns the ZERO time when nothing matches within five
// years — an unsatisfiable expression such as "0 0 30 2 *". Next is pure in its
// input and lastRun is not advanced, so the condition is permanent, and without
// this gate the zero time reads as "due" (every instant is after it) and every
// tick runs a doomed claim transaction forever, silently.
func scheduleNeverFires(nextRun time.Time) bool {
	return nextRun.IsZero()
}
