package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
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

	q.Register("chaos.tick", func(ctx context.Context, _ struct{}) error {
		stmt := `INSERT INTO chaos_ticks DEFAULT VALUES`
		if dialect == "mysql" {
			stmt = `INSERT INTO chaos_ticks () VALUES ()`
		}
		return db.WithContext(ctx).Exec(stmt).Error
	})
	q.Schedule("chaos.tick", nil, jobs.Every(5*time.Second), jobs.Retries(0))
}

func runWorker(parent context.Context, a *app) error {
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := jobs.NewWorker(
		a.q,
		jobs.Concurrency(8),
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

	fmt.Printf("seeded workload: unit=%d pipeline_tx=%d pipeline_window=%d fanout=%d slow=%d unique_attempts=50 unique_inserted=%d duplicate_rejected=%d unique_errors=%d\n",
		counts["chaos.unit"], counts["chaos.pipeline"], counts["chaos.pipeline_window"], counts["chaos.fanout"], counts["chaos.slow"], uniqueOK, uniqueDup, uniqueErr)
	return nil
}

func resetHarnessData(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect == "mysql" {
		// MySQL TRUNCATE can't target multiple tables or CASCADE; truncate each
		// with FK checks off (the schema has no inter-table FKs, but this keeps
		// the order-independent regardless).
		tables := []string{"chaos_effects", "chaos_ticks", "checkpoints", "fan_outs", "jobs", "queue_states", "scheduled_fires", "leases"}
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
	return db.WithContext(ctx).Exec(`TRUNCATE TABLE chaos_effects, chaos_ticks, checkpoints, fan_outs, jobs, queue_states, scheduled_fires, leases RESTART IDENTITY CASCADE`).Error
}

func runCheck(ctx context.Context, a *app) error {
	if err := waitForDrain(ctx, a.db, 120*time.Second, 10*time.Second); err != nil {
		fmt.Printf("drain wait: %v\n", err)
	}

	results := []invariant{
		checkExactlyOnce(ctx, a.db),
		checkAtLeastOnceWindow(ctx, a.db),
		checkNoWedge(ctx, a.db),
		checkFanOutCounts(ctx, a.db),
		checkUnique(ctx, a.db),
		checkSchedule(ctx, a.db),
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
		fmt.Printf("%-18s %-4s %-4s %s\n", inv.name, inv.level, status, inv.detail)
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
		if err := db.WithContext(ctx).Raw(`SELECT count(*) FROM jobs WHERE status IN ('pending','running')`).Scan(&active).Error; err != nil {
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

func insertEffect(ctx context.Context, db *gorm.DB, jobID, marker string) error {
	if jobID == "" {
		jobID = "unknown"
	}
	return db.WithContext(ctx).Exec(`INSERT INTO chaos_effects (job_id, marker) VALUES (?, ?)`, jobID, marker).Error
}

func insertEffectIgnoreDuplicate(ctx context.Context, db *gorm.DB, dialect, jobID, marker string) error {
	if jobID == "" {
		jobID = "unknown"
	}
	stmt := `INSERT INTO chaos_effects (job_id, marker) VALUES (?, ?) ON CONFLICT (job_id, marker) DO NOTHING`
	if dialect == "mysql" {
		stmt = `INSERT IGNORE INTO chaos_effects (job_id, marker) VALUES (?, ?)`
	}
	return db.WithContext(ctx).Exec(stmt, jobID, marker).Error
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
