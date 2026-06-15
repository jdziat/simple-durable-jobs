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

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v3/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

// Worker processes jobs from the queue.
type Worker struct {
	queue  *queue.Queue
	config WorkerConfig
	logger *slog.Logger
	wg     sync.WaitGroup

	// Pause state
	started   atomic.Bool
	paused    atomic.Bool
	pauseMode atomic.Value // stores core.PauseMode

	// Running job cancellation (for aggressive pause)
	runningJobs   map[core.UUID]context.CancelFunc
	runningJobsMu sync.Mutex

	// Per-queue concurrency tracking
	queueRunning map[string]*atomic.Int32 // queue name -> active count
	queueJobID   map[core.UUID]string     // job ID -> queue name (for decrement on completion)
	queueJobIDMu sync.Mutex

	// DB-backed concurrency slots acquired for dequeued jobs. Only populated
	// when the storage backend implements concurrencySlotStorage.
	slotJobIDMu sync.Mutex
	slotJobID   map[core.UUID][]string

	// Per-worker queue rate-limit buckets. Only populated for queues configured
	// with WithQueueRateLimit.
	queueRateBuckets map[string]*tokenBucket

	rateLimitStorageMissingLogged     atomic.Bool
	retentionStorageMissingLogged     atomic.Bool
	retentionUnconfiguredLogged       atomic.Bool
	retentionConfiguredLogged         atomic.Bool
	uniqueLockStorageMissingLogged    atomic.Bool
	slotSweepStorageMissingLogged     atomic.Bool
	readyPromoterStorageMissingLogged atomic.Bool

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

// completablePendingFanOutStorage is implemented by backends that can find
// fan-outs left status='pending' with terminal counts and a waiting parent —
// the post-crash strand the recovery poll heals by routing each row through the
// same completeFanOut path the live worker uses. Optional: backends without it
// simply skip the backstop (they still benefit from the in-tx status advance).
type completablePendingFanOutStorage interface {
	GetCompletablePendingFanOuts(ctx context.Context, olderThan time.Time) ([]*core.FanOut, error)
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

var signalResumePollBatchSize = 100

// NewWorker creates a new worker for the given queue.
func NewWorker(q *queue.Queue, opts ...WorkerOption) *Worker {
	config := WorkerConfig{
		Queues:           nil, // Will be set to default if no queue options provided
		PollInterval:     100 * time.Millisecond,
		WorkerID:         string(core.NewID()),
		DrainTimeout:     30 * time.Second,
		DequeueBatchSize: 10,
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
	if config.StaleLockAge == 0 {
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

	return &Worker{
		queue:                   q,
		config:                  config,
		logger:                  slog.Default(),
		runningJobs:             make(map[core.UUID]context.CancelFunc),
		queueRunning:            queueRunning,
		queueJobID:              make(map[core.UUID]string),
		slotJobID:               make(map[core.UUID][]string),
		queueRateBuckets:        queueRateBuckets,
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
	w.started.Store(true)
	defer w.started.Store(false)

	if err := w.validateConfiguredStorageCapabilities(); err != nil {
		return err
	}
	w.logStorageCapabilities()

	totalConcurrency := 0
	for _, c := range w.config.Queues {
		totalConcurrency += c
	}

	jobsChan := make(chan *core.Job, totalConcurrency)
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

	for i := 0; i < totalConcurrency; i++ {
		w.wg.Add(1)
		go w.processLoop(handlerBase, jobsChan)
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobsChan)
			if w.config.DrainTimeout <= 0 {
				cancelHandlers()
				w.wg.Wait()
				return ctx.Err()
			}
			if !w.waitForDrain() {
				w.logger.Warn("worker drain timeout reached; cancelling in-flight handlers",
					"in_flight", w.RunningJobCount(),
					"drain_timeout", w.config.DrainTimeout)
				cancelHandlers()
				w.wg.Wait()
			}
			return ctx.Err()
		case <-ticker.C:
			w.drainDequeuedJobs(ctx, jobsChan, totalConcurrency)
		}
	}
}

func (w *Worker) validateConfiguredStorageCapabilities() error {
	storage := w.queue.Storage()
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
	return nil
}

func (w *Worker) drainDequeuedJobs(ctx context.Context, jobsChan chan<- *core.Job, totalConcurrency int) {
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

func (w *Worker) dispatchDequeuedJobs(ctx context.Context, jobsChan chan<- *core.Job, jobs []*core.Job) (dispatched int, released int) {
	for _, job := range jobs {
		if job == nil {
			continue
		}
		if !w.tryTrackQueueJob(job.ID, job.Queue) {
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
			continue
		}
		if !w.tryConsumeQueueRateLimit(job.Queue) {
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
			continue
		}
		if ok := w.tryAcquireConcurrencySlots(ctx, job); !ok {
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
			continue
		}
		if ctx.Err() != nil {
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
			continue
		}
		if ok := w.tryConsumeRateLimits(ctx, job); !ok {
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
			continue
		}

		select {
		case jobsChan <- job:
			dispatched++
		case <-ctx.Done():
			w.refundQueueRateLimit(job.Queue)
			w.releaseDequeuedJobOnShutdown(ctx, job)
			released++
		}
	}
	return dispatched, released
}

func (w *Worker) releaseDequeuedJobOnShutdown(ctx context.Context, job *core.Job) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := w.queue.Storage().Release(releaseCtx, job.ID, w.config.WorkerID); err != nil && !errors.Is(err, core.ErrJobNotOwned) {
		w.logger.Warn("failed to release dequeued job during shutdown",
			"job_id", job.ID,
			"error", err)
	}
	w.releaseConcurrencySlots(releaseCtx, job.ID)
	w.untrackQueueJob(job.ID)
}

func (w *Worker) waitForDrain() bool {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(w.config.DrainTimeout)
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

// trackQueueJob increments the running counter for a queue and records the job→queue mapping.
func (w *Worker) trackQueueJob(jobID core.UUID, queueName string) {
	if counter, ok := w.queueRunning[queueName]; ok {
		counter.Add(1)
	}
	w.queueJobIDMu.Lock()
	w.queueJobID[jobID] = queueName
	w.queueJobIDMu.Unlock()
}

// tryTrackQueueJob is the authoritative per-queue admission gate. Its atomic
// CAS is not advisory: once a queue is at its configured cap, dispatch must
// release the dequeued job instead of letting it borrow capacity from another
// queue.
func (w *Worker) tryTrackQueueJob(jobID core.UUID, queueName string) bool {
	counter, ok := w.queueRunning[queueName]
	if !ok {
		w.trackQueueJob(jobID, queueName)
		return true
	}
	maxConcurrency, ok := w.config.Queues[queueName]
	if !ok {
		w.trackQueueJob(jobID, queueName)
		return true
	}
	for {
		current := counter.Load()
		if int(current) >= maxConcurrency {
			return false
		}
		if counter.CompareAndSwap(current, current+1) {
			w.queueJobIDMu.Lock()
			w.queueJobID[jobID] = queueName
			w.queueJobIDMu.Unlock()
			return true
		}
	}
}

// untrackQueueJob decrements the running counter for a job's queue.
func (w *Worker) untrackQueueJob(jobID core.UUID) {
	w.queueJobIDMu.Lock()
	queueName, ok := w.queueJobID[jobID]
	if ok {
		delete(w.queueJobID, jobID)
	}
	w.queueJobIDMu.Unlock()

	if ok {
		if counter, exists := w.queueRunning[queueName]; exists {
			counter.Add(-1)
		}
	}
}

func (w *Worker) concurrencySlotTTL() time.Duration {
	if w.config.LockDuration > 0 {
		return w.config.LockDuration
	}
	return defaultConcurrencySlotTTL
}

func (w *Worker) capSlotName(cap ConcurrencyCapConfig, job *core.Job) string {
	if cap.Key == nil {
		return cap.Name
	}
	return cap.Name + ":" + cap.Key(job)
}

func (w *Worker) rateLimitName(limit RateLimitConfig, job *core.Job) string {
	if limit.Key == nil {
		return limit.Name
	}
	return limit.Name + ":" + limit.Key(job)
}

func (w *Worker) tryConsumeRateLimits(ctx context.Context, job *core.Job) bool {
	if len(w.config.RateLimits) == 0 {
		return true
	}
	storage, ok := w.queue.Storage().(rateLimiterStorage)
	if !ok {
		if w.rateLimitStorageMissingLogged.CompareAndSwap(false, true) {
			w.logger.Warn("storage backend does not support fleet-wide rate limits; continuing without RateLimit enforcement")
		}
		return true
	}
	for _, limit := range w.config.RateLimits {
		if limit.Name == "" || limit.PerSecond <= 0 {
			continue
		}
		window := limit.Window
		if window <= 0 {
			window = defaultRateLimitWindow
		}
		limitName := w.rateLimitName(limit, job)
		allowed, err := storage.TryConsumeRate(ctx, limitName, limit.PerSecond, window, time.Time{})
		if err != nil {
			w.logger.Warn("failed to consume rate limit; releasing dequeued job",
				"job_id", job.ID,
				"limit", limitName,
				"error", err)
			return false
		}
		if !allowed {
			return false
		}
	}
	return true
}

func (w *Worker) tryAcquireConcurrencySlots(ctx context.Context, job *core.Job) bool {
	if len(w.config.ConcurrencyCaps) == 0 {
		return true
	}
	storage, ok := w.queue.Storage().(concurrencySlotStorage)
	if !ok {
		return true
	}
	ttl := w.concurrencySlotTTL()
	acquiredSlots := make([]string, 0, len(w.config.ConcurrencyCaps))
	for _, cap := range w.config.ConcurrencyCaps {
		slotName := w.capSlotName(cap, job)
		acquired, err := storage.TryAcquireConcurrencySlot(ctx, slotName, job.ID, w.config.WorkerID, cap.Limit, ttl)
		if err != nil {
			w.logger.Warn("failed to acquire concurrency slot; releasing dequeued job",
				"job_id", job.ID,
				"slot", slotName,
				"error", err)
			w.releaseSlotNames(ctx, job.ID, acquiredSlots)
			return false
		}
		if !acquired {
			w.releaseSlotNames(ctx, job.ID, acquiredSlots)
			return false
		}
		acquiredSlots = append(acquiredSlots, slotName)
	}
	w.slotJobIDMu.Lock()
	w.slotJobID[job.ID] = acquiredSlots
	w.slotJobIDMu.Unlock()
	return true
}

func (w *Worker) releaseConcurrencySlots(ctx context.Context, jobID core.UUID) {
	w.slotJobIDMu.Lock()
	slots := w.slotJobID[jobID]
	delete(w.slotJobID, jobID)
	w.slotJobIDMu.Unlock()
	w.releaseSlotNames(ctx, jobID, slots)
}

func (w *Worker) releaseSlotNames(ctx context.Context, jobID core.UUID, slots []string) {
	if len(slots) == 0 {
		return
	}
	storage, ok := w.queue.Storage().(concurrencySlotStorage)
	if !ok {
		return
	}
	for _, slot := range slots {
		if err := storage.ReleaseConcurrencySlot(ctx, slot, jobID); err != nil {
			w.logger.Warn("failed to release concurrency slot",
				"job_id", jobID,
				"slot", slot,
				"error", err)
		}
	}
}

func (w *Worker) renewConcurrencySlots(ctx context.Context, jobID core.UUID) {
	w.slotJobIDMu.Lock()
	slots := append([]string(nil), w.slotJobID[jobID]...)
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

func (w *Worker) processLoop(ctx context.Context, jobs <-chan *core.Job) {
	defer w.wg.Done()

	for job := range jobs {
		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *core.Job) {
	// Ensure per-queue concurrency counter is decremented when job finishes
	defer w.untrackQueueJob(job.ID)
	defer w.releaseConcurrencySlots(context.WithoutCancel(ctx), job.ID)

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
		if err := w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil); errors.Is(err, core.ErrJobNotOwned) {
			w.logger.Warn("job no longer owned after no-handler failure; skipping sub-job completion",
				"job_id", job.ID)
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
	w.runningJobs[job.ID] = cancelJob
	w.runningJobsMu.Unlock()
	w.queue.RegisterRunningJob(job.ID, cancelJob)
	defer func() {
		w.runningJobsMu.Lock()
		delete(w.runningJobs, job.ID)
		w.runningJobsMu.Unlock()
		w.queue.UnregisterRunningJob(job.ID)
	}()

	// Call start hooks
	w.queue.CallStartHooks(jobCtx, job)

	// Call context-modifying start hooks (e.g. OTel span injection)
	jobCtx = w.queue.CallStartCtxHooks(jobCtx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// Create a cancellable context for the heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()

	// Start heartbeat goroutine to extend lock during long-running jobs
	go w.runHeartbeat(heartbeatCtx, job)

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

	if err != nil {
		// Self-suspension signal — the handler moved its job to StatusWaiting
		// (fan-out or signal wait) and returned. Not a failure: just stop.
		if core.IsWaiting(err) {
			w.logger.Info("job waiting", "job_id", job.ID)
			cancelHeartbeat()
			// Job is already in StatusWaiting; just return
			return
		}
		if !w.queue.IsFailure(job, err) {
			err = nil
		}
	}

	if err != nil {
		w.queue.CallErrorHandler(jobCtx, job, err)
		w.handleError(ctx, jobCtx, job, err)
		cancelHeartbeat()
	} else {
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
// runHeartbeat cancels the handler's context via CancelJob and returns,
// so:
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
func (w *Worker) runHeartbeat(ctx context.Context, job *core.Job) {
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
			// Stop heartbeat if aggressively paused
			if w.IsPaused() && w.PauseMode() == core.PauseModeAggressive {
				return
			}

			err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				return w.queue.Storage().Heartbeat(ctx, job.ID, w.config.WorkerID)
			})
			switch {
			case err == nil:
				consecutiveOrphanErrs = 0
				w.renewConcurrencySlots(ctx, job.ID)
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
					w.CancelJob(job.ID)
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
		if failErr := w.failWithRetry(ctx, job.ID, err.Error(), retryAt); errors.Is(failErr, core.ErrJobNotOwned) {
			w.logger.Warn("job no longer owned by this worker; skipping failure handling",
				"job_id", job.ID, "error", err)
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
	if failErr := w.failWithRetry(ctx, job.ID, err.Error(), nil); errors.Is(failErr, core.ErrJobNotOwned) {
		w.logger.Warn("job no longer owned by this worker; skipping failure handling",
			"job_id", job.ID, "error", err)
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

	// Resume parent job — retry a few times because the parent might still be
	// transitioning from running → waiting (MarkWaiting hasn't completed yet).
	var resumed bool
	for attempt := 0; attempt < 5; attempt++ {
		resumed, err = w.queue.Storage().ResumeJob(ctx, fo.ParentJobID)
		if err != nil {
			return fmt.Errorf("failed to resume parent job: %w", err)
		}
		if resumed {
			break
		}
		// Parent not in waiting/paused status yet — wait briefly and retry
		if attempt < 4 {
			w.logger.Debug("parent job not yet in resumable status, retrying",
				"parent_job_id", fo.ParentJobID,
				"attempt", attempt+1)
			delay := time.Duration(100*(1<<attempt)) * time.Millisecond // 100ms, 200ms, 400ms, 800ms
			select {
			case <-ctx.Done():
				// Abort the inline retry promptly on shutdown/cancellation (do
				// not park the processLoop). The fan-out status was already
				// CAS-advanced upstream; a parent left in 'waiting' is healed by
				// the GetStalledFanOutParents backstop poll, so returning nil
				// here is safe — it is not a completion failure.
				return nil
			case <-time.After(delay):
			}
		}
	}
	if !resumed {
		w.logger.Error("CRITICAL: parent job could not be resumed after retries — may be stuck",
			"parent_job_id", fo.ParentJobID,
			"fan_out_id", fo.ID)
	}

	w.logger.Info("resumed parent job after fan-out completion",
		"parent_job_id", fo.ParentJobID,
		"fan_out_id", fo.ID,
		"status", status,
		"resumed", resumed)

	return nil
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

func (w *Worker) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastRun := make(map[string]time.Time)

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
				if _, ok := lastRun[name]; !ok {
					// First sight of this schedule: establish a fire-boundary base
					// that every worker in the fleet agrees on, so skewed wall
					// clocks cannot make two workers target different boundaries
					// for the same logical tick (which would double-fire).
					lastRun[name] = w.establishScheduleBase(ctx, name, sj.Schedule, now)
				}
				nextRun := sj.Schedule.Next(lastRun[name])
				if now.After(nextRun) || now.Equal(nextRun) {
					claimed, err := w.queue.Storage().ClaimScheduledFire(ctx, name, nextRun)
					if err != nil {
						w.logger.Error("failed to claim scheduled fire", "name", name, "fire_time", nextRun, "error", err)
						continue
					}
					if !claimed {
						lastRun[name] = nextRun
						continue
					}
					lastRun[name] = nextRun
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
					_, err = w.queue.Enqueue(ctx, sj.Name, sj.Args,
						opts...,
					)
					if err != nil {
						w.logger.Error("failed to enqueue scheduled job", "name", name, "error", err)
					}
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
		return
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
		return
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
			return
		}
		for _, fo := range completable {
			if resumeErr := w.checkFanOutCompletion(ctx, fo); resumeErr != nil {
				w.logger.Error("failed to complete stranded pending fan-out", "fan_out_id", fo.ID, "error", resumeErr)
			} else {
				w.logger.Info("rescued stranded pending fan-out via polling fallback", "fan_out_id", fo.ID)
			}
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

// Pause pauses the worker.
func (w *Worker) Pause(mode core.PauseMode) {
	w.pauseMode.Store(mode)
	w.paused.Store(true)

	if mode == core.PauseModeAggressive {
		// Cancel all running jobs
		w.runningJobsMu.Lock()
		for _, cancel := range w.runningJobs {
			cancel()
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
	cancel, ok := w.runningJobs[jobID]
	w.runningJobsMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// Resume resumes the worker.
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
