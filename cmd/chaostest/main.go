package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const defaultDatabaseURL = "postgres://jobs:jobs@postgres:5432/jobs_test?sslmode=disable"

type app struct {
	db      *gorm.DB
	store   *jobs.GormStorage
	q       *jobs.Queue
	dialect string // "postgres" or "mysql"
}

type subArgs struct {
	Index int `json:"index"`
}

// waiterArgs is the payload for the chaos.signal_waiter scenario: how many
// signals the waiter must consume (one per WaitForSignal call).
type waiterArgs struct {
	Count int `json:"count"`
}

// signalTarget is the scan struct for chaos_signal_targets, which records the
// (waiter job_id, signal count) pairs the chaos.signal_sender delivers to.
type signalTarget struct {
	JobID    string `gorm:"column:job_id"`
	SigCount int    `gorm:"column:sig_count"`
}

type invariant struct {
	name   string
	level  string
	pass   bool
	detail string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		fatalf("usage: chaostest worker|seed|check")
	}

	ctx := context.Background()
	a, err := openApp(ctx)
	if err != nil {
		fatalf("open app: %v", err)
	}

	switch os.Args[1] {
	case "worker":
		if err := runWorker(ctx, a); err != nil {
			fatalf("worker: %v", err)
		}
	case "seed":
		if err := runSeed(ctx, a); err != nil {
			fatalf("seed: %v", err)
		}
	case "check":
		if err := runCheck(ctx, a); err != nil {
			os.Exit(1)
		}
	default:
		fatalf("unknown subcommand %q", os.Args[1])
	}
}

func openApp(ctx context.Context) (*app, error) {
	dialector, dialect := openDialector()
	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, err
	}
	store := jobs.NewGormStorage(db)
	// All harness jobs run <3s, well under the 2-min heartbeat tick, so a short
	// lock is safe and lets a crashed worker's jobs be reclaimed and REPLAYED
	// within seconds. The default 45-min lock would orphan them past the drain
	// window and mask the checkpoint-replay bug (finding 0.1).
	store.SetLockDuration(5 * time.Second)
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	if err := ensureLedger(ctx, db, dialect); err != nil {
		return nil, err
	}
	q := jobs.New(store)
	registerHandlers(q, db, dialect)
	return &app{db: db, store: store, q: q, dialect: dialect}, nil
}

// openDialector selects the storage backend from the environment so the chaos
// harness can exercise BOTH multi-worker backends. TEST_MYSQL_URL takes
// precedence (MySQL is first-class); otherwise TEST_DATABASE_URL / the default
// selects Postgres.
func openDialector() (gorm.Dialector, string) {
	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		return mysql.Open(dsn), "mysql"
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultDatabaseURL
	}
	return postgres.Open(dsn), "postgres"
}

func ensureLedger(ctx context.Context, db *gorm.DB, dialect string) error {
	var stmts []string
	if dialect == "mysql" {
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS chaos_effects (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				job_id VARCHAR(191) NOT NULL,
				marker VARCHAR(191) NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				UNIQUE KEY uq_job_marker (job_id, marker)
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_ticks (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				fired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_signal_targets (
				job_id VARCHAR(191) NOT NULL PRIMARY KEY,
				sig_count INT NOT NULL
			)`,
		}
	} else {
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS chaos_effects (
				id bigserial PRIMARY KEY,
				job_id text NOT NULL,
				marker text NOT NULL,
				created_at timestamptz NOT NULL DEFAULT now(),
				UNIQUE(job_id, marker)
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_ticks (
				id bigserial PRIMARY KEY,
				fired_at timestamptz NOT NULL DEFAULT now()
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_signal_targets (
				job_id text PRIMARY KEY,
				sig_count int NOT NULL
			)`,
		}
	}
	for _, stmt := range stmts {
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return err
		}
	}
	return nil
}

func registerHandlers(q *jobs.Queue, db *gorm.DB, dialect string) {
	q.Register("chaos.unit", func(ctx context.Context, _ struct{}) error {
		return insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "done")
	})

	q.Register("chaos.unique_windowed", func(ctx context.Context, _ struct{}) error {
		return insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "windowed-done")
	})

	q.Register("chaos.pipeline", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		for _, phase := range []string{"extract", "transform", "load"} {
			if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, phase); ok {
				continue
			}
			time.Sleep(150 * time.Millisecond)
			marker := "phase:" + phase
			err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				if err := insertEffect(ctx, tx, jobID, marker); err != nil {
					return err
				}
				return jobs.SavePhaseCheckpointTx(ctx, tx, phase, "ok")
			})
			if err != nil {
				if isDuplicate(err) {
					_ = insertEffectIgnoreDuplicate(ctx, db, dialect, jobID, "phase-reexec:"+phase)
				}
				return err
			}
		}
		// Force exactly one replay so the checkpoint-keying bug (finding 0.1) is
		// reproduced DETERMINISTICALLY, independent of chaos timing: on the first
		// execution we checkpoint "committed" and return a transient error to
		// trigger a retry. On replay, correct (CallIndex,CallType) keying skips
		// all completed phases and finishes cleanly; the buggy int-only keying
		// collapses every -1 phase checkpoint onto one map slot, so the phase
		// loop re-executes the lost phases — surfaced as phase-reexec markers.
		if _, done := jobs.LoadPhaseCheckpoint[string](ctx, "committed"); !done {
			if err := jobs.SavePhaseCheckpoint(ctx, "committed", "ok"); err != nil {
				return err
			}
			return fmt.Errorf("chaostest: forced replay to exercise checkpoint keying")
		}
		return nil
	})

	// chaos.pipeline_window deliberately keeps the two-commit effect/checkpoint
	// pattern to demonstrate the documented at-least-once window. A SIGKILL
	// landing between the two commits leaves the effect without its checkpoint;
	// every retry then re-hits the unique constraint, so such jobs END FAILED
	// after retries by design — counted by INV-AT-LEAST-ONCE-WINDOW (INFO),
	// never a HARD failure.
	q.Register("chaos.pipeline_window", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		for _, phase := range []string{"extract", "transform", "load"} {
			if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, phase); ok {
				continue
			}
			time.Sleep(150 * time.Millisecond)
			marker := "phase:" + phase
			if err := insertEffect(ctx, db, jobID, marker); err != nil {
				if isDuplicate(err) {
					_ = insertEffectIgnoreDuplicate(ctx, db, dialect, jobID, "window-reexec:"+phase)
				}
				return err
			}
			// This handler intentionally keeps the old two-commit pattern to
			// demonstrate the documented at-least-once crash window.
			if err := jobs.SavePhaseCheckpoint(ctx, phase, "ok"); err != nil {
				return err
			}
		}
		return nil
	})

	q.Register("chaos.fanout", func(ctx context.Context, _ struct{}) error {
		subs := make([]jobs.SubJob, 0, 5)
		for i := 0; i < 5; i++ {
			subs = append(subs, jobs.Sub("chaos.sub", subArgs{Index: i}, jobs.Retries(0)))
		}
		_, err := jobs.FanOut[string](ctx, subs, jobs.FailFast(), jobs.CancelOnParentFailure(), jobs.WithFanOutRetries(0))
		return err
	})

	q.Register("chaos.sub", func(ctx context.Context, args subArgs) (string, error) {
		if err := insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "sub"); err != nil {
			return "", err
		}
		if args.Index == 4 {
			return "", fmt.Errorf("deterministic sub-job failure at index 4")
		}
		return fmt.Sprintf("sub-%d", args.Index), nil
	})

	q.Register("chaos.slow", func(ctx context.Context, _ struct{}) error {
		select {
		case <-time.After(3 * time.Second):
			return insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "slow")
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// chaos.signal_waiter defends P1 (atomic signal consume + replay checkpoint).
	// It consumes exactly args.Count signals named "sig" — calling WaitForSignal on
	// EVERY iteration (never skipped on replay, since WaitForSignal's own
	// (CallIndex, "signal:sig") checkpoint keeps the consume ordering deterministic).
	// Each successful consume records an idempotent downstream effect. A P1
	// lost-signal (consumed_at committed without its checkpoint) re-consumes the
	// next FIFO signal on replay, leaving the waiter one short at the final
	// iteration -> WaitForSignal returns nil -> MarkWaiting -> wedged forever, which
	// INV-SIGNAL-EXACTLY-ONCE catches as consumed<expected AND unfinished_waiters>0.
	q.Register("chaos.signal_waiter", func(ctx context.Context, args waiterArgs) error {
		jobID := jobs.JobIDFromContext(ctx)
		for i := 0; i < args.Count; i++ {
			if _, err := jobs.WaitForSignal[int](ctx, "sig"); err != nil {
				return err
			}
			marker := "sig-consumed:" + strconv.Itoa(i)
			if err := insertEffect(ctx, db, jobID, marker); err != nil {
				if isDuplicate(err) {
					// Benign at-least-once replay: the consume already landed; record
					// the duplicate as an INFO-only re-exec marker and move on.
					_ = insertEffectIgnoreDuplicate(ctx, db, dialect, jobID, "sig-reexec:"+strconv.Itoa(i))
					continue
				}
				return err
			}
		}
		return nil
	})

	// chaos.signal_sender delivers the signals each waiter is waiting on. It reads
	// the (job_id, sig_count) targets seeded BEFORE it was enqueued, so the targets
	// always exist when it runs. Each (target, seq) send is guarded by a phase
	// checkpoint so a killed-and-retried sender does not flood the buffered signals
	// table; the waiter consumes exactly Count regardless.
	q.Register("chaos.signal_sender", func(ctx context.Context, _ struct{}) error {
		var targets []signalTarget
		if err := db.WithContext(ctx).Raw(`SELECT job_id, sig_count FROM chaos_signal_targets`).Scan(&targets).Error; err != nil {
			return err
		}
		for _, t := range targets {
			for seq := 0; seq < t.SigCount; seq++ {
				phase := fmt.Sprintf("sent:%s:%d", t.JobID, seq)
				if _, done := jobs.LoadPhaseCheckpoint[bool](ctx, phase); done {
					continue
				}
				if err := q.Signal(ctx, jobs.UUID(t.JobID), "sig", seq); err != nil {
					return err
				}
				if err := jobs.SavePhaseCheckpoint(ctx, phase, true); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// chaos.timer defends the durable-timer path and P3 (crash-resistant checkpoint
	// write). It Sleeps 2s (suspending via &WaitingError, resumed on the ORIGINAL
	// checkpointed deadline) then performs ONE effect using the atomic transaction
	// pattern proven by chaos.pipeline: insertEffect + SavePhaseCheckpointTx commit
	// together, so a SIGKILL either commits both (replay short-circuits via
	// LoadPhaseCheckpoint) or neither (replay redoes cleanly). A lost timer effect
	// shows as fired<expected; a doubled one as a duplicate timer-fired or a
	// timer-reexec marker; a wedge as an unfinished chaos.timer row.
	q.Register("chaos.timer", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		if err := jobs.Sleep(ctx, 2*time.Second); err != nil {
			return err
		}
		if _, done := jobs.LoadPhaseCheckpoint[string](ctx, "timer-effect"); done {
			return nil
		}
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if e := insertEffect(ctx, tx, jobID, "timer-fired"); e != nil {
				return e
			}
			return jobs.SavePhaseCheckpointTx(ctx, tx, "timer-effect", "ok")
		})
		if err != nil {
			if isDuplicate(err) {
				_ = insertEffectIgnoreDuplicate(ctx, db, dialect, jobID, "timer-reexec")
			}
			return err
		}
		return nil
	})

	q.Register("chaos.tick", func(ctx context.Context, _ struct{}) error {
		stmt := `INSERT INTO chaos_ticks DEFAULT VALUES`
		if dialect == "mysql" {
			stmt = `INSERT INTO chaos_ticks () VALUES ()`
		}
		return db.WithContext(ctx).Exec(stmt).Error
	})
	if err := q.Schedule("chaos.tick", nil, jobs.Every(5*time.Second), jobs.Retries(0)); err != nil {
		panic(err)
	}
}

func runWorker(parent context.Context, a *app) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := jobs.NewWorker(
		a.q,
		jobs.Concurrency(8),
		jobs.ConcurrencyCap("chaos", 64),
		jobs.RateLimit("chaos", 1000),
		jobs.WithScheduler(true),
		jobs.WithPollInterval(50*time.Millisecond),
		jobs.WithStaleLockInterval(2*time.Second),
		jobs.WithStaleLockAge(2*time.Second),
		// Recover parents wedged mid-fan-out fast (default is 2m) so the
		// harness sees INV-NO-WEDGE clear within the drain window.
		jobs.WithFanOutRecoveryStaleAge(3*time.Second),
	)
	log.Printf("chaostest worker started")
	err := w.Start(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func runSeed(ctx context.Context, a *app) error {
	if err := resetHarnessData(ctx, a.db, a.dialect); err != nil {
		return err
	}

	counts := map[string]int{
		"chaos.unit":            200,
		"chaos.pipeline":        30,
		"chaos.pipeline_window": 20,
		"chaos.fanout":          20,
		"chaos.slow":            10,
	}
	for typ, n := range counts {
		for i := 0; i < n; i++ {
			if _, err := a.q.Enqueue(ctx, typ, struct{}{}, jobs.Retries(3)); err != nil {
				return fmt.Errorf("enqueue %s: %w", typ, err)
			}
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var uniqueOK, uniqueDup, uniqueErr int
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
			_, err := a.q.Enqueue(ctx, "chaos.unit", struct{}{}, jobs.Unique("dup-key-1"), jobs.Retries(0))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				uniqueOK++
			case errors.Is(err, jobs.ErrDuplicateJob):
				uniqueDup++
			default:
				uniqueErr++
				log.Printf("unique enqueue error: %v", err)
			}
		}()
	}
	wg.Wait()

	var uniqueWindowedOK, uniqueWindowedErr int
	windowedIDs := make(map[string]struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
			id, err := a.q.Enqueue(ctx, "chaos.unique_windowed", struct{}{}, jobs.IdempotencyKey("windowed-dup-key-1", 24*time.Hour), jobs.Retries(0))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				uniqueWindowedErr++
				log.Printf("windowed unique enqueue error: %v", err)
				return
			}
			uniqueWindowedOK++
			windowedIDs[string(id)] = struct{}{}
		}()
	}
	wg.Wait()

	// Signal + timer durability scenarios (P6). Seed each waiter's target row
	// BEFORE enqueueing the sender so the sender always finds its targets. The
	// sender is enqueued AFTER all targets are recorded.
	const (
		signalWaiters    = 10
		signalsPerWaiter = 3
		timerJobs        = 15
	)
	for i := 0; i < signalWaiters; i++ {
		id, err := a.q.Enqueue(ctx, "chaos.signal_waiter", waiterArgs{Count: signalsPerWaiter}, jobs.Retries(10))
		if err != nil {
			return fmt.Errorf("enqueue signal_waiter: %w", err)
		}
		if err := insertSignalTarget(ctx, a.db, id, signalsPerWaiter); err != nil {
			return fmt.Errorf("record signal target: %w", err)
		}
	}
	if _, err := a.q.Enqueue(ctx, "chaos.signal_sender", struct{}{}, jobs.Retries(20)); err != nil {
		return fmt.Errorf("enqueue signal_sender: %w", err)
	}
	for i := 0; i < timerJobs; i++ {
		if _, err := a.q.Enqueue(ctx, "chaos.timer", struct{}{}, jobs.Retries(10)); err != nil {
			return fmt.Errorf("enqueue timer: %w", err)
		}
	}

	fmt.Printf("seeded workload: unit=%d pipeline_tx=%d pipeline_window=%d fanout=%d slow=%d unique_attempts=50 unique_inserted=%d duplicate_rejected=%d unique_errors=%d windowed_unique_attempts=50 windowed_unique_ok=%d windowed_unique_returned_ids=%d windowed_unique_errors=%d signal_waiters=%d signals_per_waiter=%d timers=%d\n",
		counts["chaos.unit"], counts["chaos.pipeline"], counts["chaos.pipeline_window"], counts["chaos.fanout"], counts["chaos.slow"], uniqueOK, uniqueDup, uniqueErr, uniqueWindowedOK, len(windowedIDs), uniqueWindowedErr, signalWaiters, signalsPerWaiter, timerJobs)
	return nil
}

func resetHarnessData(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect == "mysql" {
		// MySQL TRUNCATE can't target multiple tables or CASCADE; truncate each
		// with FK checks off (the schema has no inter-table FKs, but this keeps
		// the order-independent regardless).
		tables := []string{"chaos_effects", "chaos_ticks", "chaos_signal_targets", "signals", "checkpoints", "fan_outs", "jobs", "unique_locks", "queue_states", "scheduled_fires", "leases"}
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(`SET FOREIGN_KEY_CHECKS=0`).Error; err != nil {
				return err
			}
			for _, t := range tables {
				if err := tx.Exec(`TRUNCATE TABLE ` + t).Error; err != nil {
					return err
				}
			}
			return tx.Exec(`SET FOREIGN_KEY_CHECKS=1`).Error
		})
	}
	return db.WithContext(ctx).Exec(`TRUNCATE TABLE chaos_effects, chaos_ticks, chaos_signal_targets, signals, checkpoints, fan_outs, jobs, unique_locks, queue_states, scheduled_fires, leases RESTART IDENTITY CASCADE`).Error
}

func runCheck(ctx context.Context, a *app) error {
	if err := waitForDrain(ctx, a.db, 120*time.Second, 10*time.Second); err != nil {
		fmt.Printf("drain wait: %v\n", err)
		inv := checkNoWedge(ctx, a.db)
		status := "PASS"
		if !inv.pass {
			status = "FAIL"
		}
		fmt.Println("chaostest invariant report:")
		fmt.Printf("%-26s %-4s %-4s %s\n", inv.name, inv.level, status, inv.detail)
		fmt.Println("chaostest result: DID NOT DRAIN (wedged or still draining)")
		return fmt.Errorf("chaostest did not drain: %w", err)
	}

	results := []invariant{
		checkExactlyOnce(ctx, a.db),
		checkAtLeastOnceWindow(ctx, a.db),
		checkNoWedge(ctx, a.db),
		checkFanOutCounts(ctx, a.db),
		checkUnique(ctx, a.db),
		checkUniqueWindowed(ctx, a.db),
		checkSchedule(ctx, a.db),
		checkSignalExactlyOnce(ctx, a.db),
		checkTimerExactlyOnce(ctx, a.db),
		checkSlotNoLeak(ctx, a.db),
		checkRateWellFormed(ctx, a.db, a.dialect),
	}

	hardFailed := 0
	fmt.Println("chaostest invariant report:")
	for _, inv := range results {
		status := "PASS"
		if !inv.pass {
			status = "FAIL"
			if inv.level == "HARD" {
				hardFailed++
			}
		}
		fmt.Printf("%-26s %-4s %-4s %s\n", inv.name, inv.level, status, inv.detail)
	}
	if hardFailed > 0 {
		fmt.Printf("chaostest result: RED baseline reproduced with %d HARD failure(s)\n", hardFailed)
		return fmt.Errorf("%d hard invariant failures", hardFailed)
	}
	fmt.Println("chaostest result: no HARD failures observed")
	return nil
}

func waitForDrain(ctx context.Context, db *gorm.DB, timeout, quietFor time.Duration) error {
	deadline := time.Now().Add(timeout)
	quietSince := time.Time{}
	for time.Now().Before(deadline) {
		var active int64
		if err := db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE status IN ('pending','running','waiting')`).Scan(&active).Error; err != nil {
			return err
		}
		if active == 0 {
			if quietSince.IsZero() {
				quietSince = time.Now()
			}
			if time.Since(quietSince) >= quietFor {
				return nil
			}
		} else {
			quietSince = time.Time{}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for 10s quiescence")
}

func checkExactlyOnce(ctx context.Context, db *gorm.DB) invariant {
	var duplicateRows, reexecRows, windowCheckpointedRows int64
	db.WithContext(ctx).Raw(`
		SELECT count(*) FROM (
			SELECT job_id, marker FROM chaos_effects GROUP BY job_id, marker HAVING count(*) > 1
		) dup`).Scan(&duplicateRows)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker LIKE 'phase-reexec:%'`).Scan(&reexecRows)
	db.WithContext(ctx).Raw(`
		SELECT count(*)
		FROM chaos_effects ce
		WHERE ce.marker LIKE 'window-reexec:%'
		  AND EXISTS (
			SELECT 1
			FROM checkpoints cp
			WHERE cp.job_id = ce.job_id
			  AND cp.call_index = -1
			  AND cp.call_type = SUBSTRING(ce.marker FROM 15)
		  )`).Scan(&windowCheckpointedRows)
	pass := duplicateRows == 0 && reexecRows == 0 && windowCheckpointedRows == 0
	return invariant{
		name:   "INV-EXACTLY-ONCE",
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("tx pipeline: duplicate_effect_groups=%d phase_reexec_markers=%d; window checkpointed_reexec_markers=%d", duplicateRows, reexecRows, windowCheckpointedRows),
	}
}

func checkAtLeastOnceWindow(ctx context.Context, db *gorm.DB) invariant {
	var windowRows int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker LIKE 'window-reexec:%'`).Scan(&windowRows)
	return invariant{
		name:   "INV-AT-LEAST-ONCE-WINDOW",
		level:  "INFO",
		pass:   true,
		detail: fmt.Sprintf("window_reexec_markers=%d expected at-least-once re-execution under SIGKILL; bounded by design", windowRows),
	}
}

func checkNoWedge(ctx context.Context, db *gorm.DB) invariant {
	var waiting, running int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE status = 'waiting'`).Scan(&waiting)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE status = 'running'`).Scan(&running)
	return invariant{
		name:   "INV-NO-WEDGE",
		level:  "HARD",
		pass:   waiting == 0 && running == 0,
		detail: fmt.Sprintf("waiting=%d running=%d", waiting, running),
	}
}

func checkFanOutCounts(ctx context.Context, db *gorm.DB) invariant {
	var bad, total int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM fan_outs`).Scan(&total)
	db.WithContext(ctx).Raw(`
		SELECT count(*) FROM fan_outs
		WHERE completed_count + failed_count + cancelled_count <> total_count`).Scan(&bad)
	return invariant{
		name:   "INV-FANOUT-COUNTS",
		level:  "HARD",
		pass:   bad == 0,
		detail: fmt.Sprintf("fan_out_rows=%d mismatched_counts=%d", total, bad),
	}
}

func checkUnique(ctx context.Context, db *gorm.DB) invariant {
	var count int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE unique_key = 'dup-key-1'`).Scan(&count)
	return invariant{
		name:   "INV-UNIQUE",
		level:  "HARD",
		pass:   count == 1,
		detail: fmt.Sprintf("jobs_with_dup_key_1=%d", count),
	}
}

func checkUniqueWindowed(ctx context.Context, db *gorm.DB) invariant {
	var count int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE type = 'chaos.unique_windowed'`).Scan(&count)
	return invariant{
		name:   "INV-UNIQUE-WINDOWED",
		level:  "HARD",
		pass:   count == 1,
		detail: fmt.Sprintf("jobs_with_windowed_dup_key_1=%d", count),
	}
}

func checkSchedule(ctx context.Context, db *gorm.DB) invariant {
	// Measure the steady-state fire rate over a fresh window while the worker
	// replicas are still running. A correctly fleet-deduplicated scheduler
	// fires the 5s tick ~once per period regardless of replica count; without
	// dedup, N replicas each fire (and a scheduler that boot-storms re-fires on
	// every chaos respawn). Counting over a window — rather than total ticks
	// since seed — avoids the earlier drain-time accounting error.
	const window = 12 * time.Second
	const period = 5 * time.Second
	var before, after int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_ticks`).Scan(&before)
	select {
	case <-ctx.Done():
	case <-time.After(window):
	}
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_ticks`).Scan(&after)
	got := after - before
	// One logical scheduler: floor(window/period) boundaries, +2 slack for
	// boundary alignment and a tick landing at each edge of the window.
	maxExpected := int64(window/period) + 2
	return invariant{
		name: "INV-SCHED",
		// HARD as of the shared-anchor scheduler fix: a fresh schedule now seeds
		// a fleet-wide base (SeedScheduledFire), so skewed worker clocks can no
		// longer make replicas target different first boundaries and double-fire.
		level:  "HARD",
		pass:   got <= maxExpected,
		detail: fmt.Sprintf("ticks_in_%s_window=%d max_expected_single_scheduler=%d", window, got, maxExpected),
	}
}

// checkSignalExactlyOnce defends P1: every seeded signal is consumed exactly once
// and no waiter is left wedged. A P1 lost-signal regression manifests as
// consumed<expected AND/OR a chaos.signal_waiter row stuck in a non-completed
// status. The at-least-once re-exec count is reported for visibility only (the
// downstream effect is idempotent by design and never fails this HARD check).
// expected>0 guards against a vacuous PASS when seeding produced nothing.
func checkSignalExactlyOnce(ctx context.Context, db *gorm.DB) invariant {
	var expected, consumed, unfinished, reexec int64
	db.WithContext(ctx).Raw(`SELECT COALESCE(SUM(sig_count),0) FROM chaos_signal_targets`).Scan(&expected)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker LIKE 'sig-consumed:%'`).Scan(&consumed)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE type = 'chaos.signal_waiter' AND status <> 'completed'`).Scan(&unfinished)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker LIKE 'sig-reexec:%'`).Scan(&reexec)
	pass := expected > 0 && consumed == expected && unfinished == 0
	return invariant{
		name:   "INV-SIGNAL-EXACTLY-ONCE",
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("expected=%d consumed=%d unfinished_waiters=%d at_least_once_reexec=%d", expected, consumed, unfinished, reexec),
	}
}

// checkTimerExactlyOnce defends the durable-timer path and P3: each timer fires
// its effect exactly once with no re-execution and no wedge. A lost effect shows
// as fired<expected, a doubled effect as reexec>0 (or a duplicate timer-fired),
// and a re-sleep/wedge as unfinished>0. expected>0 guards against a vacuous PASS.
func checkTimerExactlyOnce(ctx context.Context, db *gorm.DB) invariant {
	var expected, fired, reexec, unfinished int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE type = 'chaos.timer'`).Scan(&expected)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker = 'timer-fired'`).Scan(&fired)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM chaos_effects WHERE marker = 'timer-reexec'`).Scan(&reexec)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE type = 'chaos.timer' AND status <> 'completed'`).Scan(&unfinished)
	pass := expected > 0 && fired == expected && reexec == 0 && unfinished == 0
	return invariant{
		name:   "INV-TIMER-EXACTLY-ONCE",
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("expected=%d fired=%d reexec=%d unfinished=%d", expected, fired, reexec, unfinished),
	}
}

func checkSlotNoLeak(ctx context.Context, db *gorm.DB) invariant {
	var n int64
	// The sentinel slot is stored as the nil UUID (16 zero bytes); job_id is now a
	// binary uuid column, so compare against the bound nil-UUID value (its Value()
	// encodes to 16 zero bytes per dialect) rather than the literal ''. Checking the
	// error matters: a comparison that fails to typecheck must fail the invariant,
	// not silently leave n=0 and report a false pass.
	if err := db.WithContext(ctx).
		Raw(`SELECT count(*) FROM concurrency_slots WHERE job_id <> ? AND expires_at > NOW()`, jobs.NilUUID).
		Scan(&n).Error; err != nil {
		return invariant{
			name:   "INV-SLOT-NO-LEAK",
			level:  "HARD",
			pass:   false,
			detail: fmt.Sprintf("slot-leak query failed: %v", err),
		}
	}
	return invariant{
		name:   "INV-SLOT-NO-LEAK",
		level:  "HARD",
		pass:   n == 0,
		detail: fmt.Sprintf("live_nonsentinel_slots=%d", n),
	}
}

func checkRateWellFormed(ctx context.Context, db *gorm.DB, dialect string) invariant {
	countColumn := `"count"`
	if dialect == "mysql" {
		countColumn = "`count`"
	}
	var negs, total int64
	db.WithContext(ctx).Raw(`SELECT count(*) FROM rate_limit_windows WHERE ` + countColumn + ` < 0`).Scan(&negs)
	db.WithContext(ctx).Raw(`SELECT count(*) FROM rate_limit_windows`).Scan(&total)
	return invariant{
		name:   "INV-RATE-WELLFORMED",
		level:  "HARD",
		pass:   negs == 0,
		detail: fmt.Sprintf("negative_counts=%d total_windows=%d", negs, total),
	}
}

func insertSignalTarget(ctx context.Context, db *gorm.DB, jobID jobs.UUID, n int) error {
	return db.WithContext(ctx).Exec(`INSERT INTO chaos_signal_targets (job_id, sig_count) VALUES (?, ?)`, string(jobID), n).Error
}

func insertEffect(ctx context.Context, db *gorm.DB, jobID jobs.UUID, marker string) error {
	id := string(jobID)
	if id == "" {
		id = "unknown"
	}
	return db.WithContext(ctx).Exec(`INSERT INTO chaos_effects (job_id, marker) VALUES (?, ?)`, id, marker).Error
}

func insertEffectIgnoreDuplicate(ctx context.Context, db *gorm.DB, dialect string, jobID jobs.UUID, marker string) error {
	id := string(jobID)
	if id == "" {
		id = "unknown"
	}
	stmt := `INSERT INTO chaos_effects (job_id, marker) VALUES (?, ?) ON CONFLICT (job_id, marker) DO NOTHING`
	if dialect == "mysql" {
		stmt = `INSERT IGNORE INTO chaos_effects (job_id, marker) VALUES (?, ?)`
	}
	return db.WithContext(ctx).Exec(stmt, id, marker).Error
}

func isDuplicate(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "duplicate key value") || // pg text
		strings.Contains(msg, "SQLSTATE 23505") || // pg code
		strings.Contains(msg, "Duplicate entry") || // mysql text
		strings.Contains(msg, "Error 1062") // mysql code
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
