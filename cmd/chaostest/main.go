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

	jobs "github.com/jdziat/simple-durable-jobs/v4"
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
		return mysql.Open(dsn), dialectMySQL
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultDatabaseURL
	}
	return postgres.Open(dsn), dialectPostgres
}

const (
	dialectPostgres = "postgres"
	dialectMySQL    = "mysql"
	// dialectSQLite is not reachable from openDialector — the chaos harness needs
	// multiple worker PROCESSES against one database, which SQLite cannot serve.
	// It exists so the invariant checks themselves are testable in-process: every
	// one of them is a SQL assertion, and a release gate whose assertions are never
	// executed by a test is how a check comes to be silently vacuous.
	dialectSQLite = "sqlite"
)

// fixedAttemptNonce is the attempt_nonce every effect that is NOT written inside
// a checkpoint transaction carries. Those effects are at-least-once BY DESIGN —
// a SIGKILL between the effect commit and the job's completion write replays the
// handler — so a second row for the same (job_id, marker) must keep colliding
// with the unique index exactly as it did before attempt_nonce existed.
//
// See insertEffectAttempt for the other half.
const fixedAttemptNonce = ""

func ensureLedger(ctx context.Context, db *gorm.DB, dialect string) error {
	// WHY chaos_effects IS NOT UNIQUE ON (job_id, marker)
	//
	// It used to be, and that made INV-EXACTLY-ONCE's duplicate_effect_groups
	// sub-check worse than dead: the fixture MASKED the defect the check exists to
	// find. If a job really ran twice, the second effect INSERT failed with 23505
	// instead of being recorded, so a genuine duplicate-execution bug surfaced as a
	// handler error — retried, eventually a failed job nothing asserts on — and
	// `duplicate_effect_groups` reported 0. A population guard on a check whose data
	// CANNOT EXIST is still vacuous.
	//
	// The key is now (job_id, marker, attempt_nonce), which lets a duplicate be
	// RECORDED while keeping the collision behaviour every existing handler relies
	// on: only the four effects written inside a checkpoint transaction pass a
	// per-execution nonce (see insertEffectAttempt); everything else passes
	// fixedAttemptNonce and behaves exactly as before.
	//
	// That split is what keeps duplicate_effect_groups from FALSE-FIRING. The
	// library's guarantee is at-least-once, and the harness deliberately SIGKILLs
	// workers: chaos.unit, chaos.sub, chaos.pipeline_window and friends can legally
	// re-execute, and their duplicates must stay unrecordable. Only a tx-paired
	// effect — where the checkpoint that suppresses replay commits WITH the effect —
	// is exactly-once, so only there is a duplicate row unambiguously a defect.
	var stmts []string
	switch dialect {
	case dialectMySQL:
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS chaos_effects (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				job_id VARCHAR(191) NOT NULL,
				marker VARCHAR(191) NOT NULL,
				attempt_nonce VARCHAR(64) NOT NULL DEFAULT '',
				created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				UNIQUE KEY uq_job_marker_nonce (job_id, marker, attempt_nonce)
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
	case dialectSQLite:
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS chaos_effects (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				job_id text NOT NULL,
				marker text NOT NULL,
				attempt_nonce text NOT NULL DEFAULT '',
				created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(job_id, marker, attempt_nonce)
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_ticks (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				fired_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
			)`,
			`CREATE TABLE IF NOT EXISTS chaos_signal_targets (
				job_id text PRIMARY KEY,
				sig_count int NOT NULL
			)`,
		}
	default:
		stmts = []string{
			`CREATE TABLE IF NOT EXISTS chaos_effects (
				id bigserial PRIMARY KEY,
				job_id text NOT NULL,
				marker text NOT NULL,
				attempt_nonce text NOT NULL DEFAULT '',
				created_at timestamptz NOT NULL DEFAULT now(),
				UNIQUE(job_id, marker, attempt_nonce)
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
	// Fan-out width for the chaos.megaflow nested fan-out (CHAOS_FANOUT_WIDTH,
	// default 5). chaos.fanout keeps its own hardcoded width=5 because its
	// index-4 deterministic failure is load-bearing for CancelOnParentFailure.
	megaFanoutWidth := envInt("CHAOS_FANOUT_WIDTH", 5)
	if megaFanoutWidth < 1 {
		megaFanoutWidth = 1
	}
	windowGap := pipelineWindowGap()

	q.Register("chaos.unit", func(ctx context.Context, _ struct{}) error {
		return insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "done")
	})

	q.Register("chaos.unique_windowed", func(ctx context.Context, _ struct{}) error {
		return insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "windowed-done")
	})

	q.Register("chaos.pipeline", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		// One nonce per handler INVOCATION: a replay is a new invocation, so a phase
		// that runs twice writes two rows instead of failing on the unique index —
		// which is what makes the re-execution visible to duplicate_effect_groups.
		// This replaces the old phase-reexec marker, which only ever existed because
		// the duplicate could not be recorded.
		nonce := newAttemptNonce()
		for _, phase := range []string{"extract", "transform", "load"} {
			if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, phase); ok {
				continue
			}
			time.Sleep(150 * time.Millisecond)
			marker := "phase:" + phase
			err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				if err := insertEffectAttempt(ctx, tx, jobID, marker, nonce); err != nil {
					return err
				}
				return jobs.SavePhaseCheckpointTx(ctx, tx, phase, "ok")
			})
			if err != nil {
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
			// Normal runs leave this at zero. Torture runs can widen the exact
			// effect-committed/checkpoint-not-yet-written interval so SIGKILL can
			// exercise the documented at-least-once replay path repeatably instead
			// of depending on a sub-millisecond database timing coincidence.
			if windowGap > 0 {
				time.Sleep(windowGap)
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

	// chaos.mega_sub is the always-succeeding sub-job used by chaos.megaflow's
	// fan-out (unlike chaos.sub, which fails at index 4 by design). Each sub is its
	// own job, so (job_id, "mega-sub") is unique per sub; a duplicate on an
	// at-least-once replay of the same sub is benign.
	q.Register("chaos.mega_sub", func(ctx context.Context, args subArgs) (string, error) {
		if err := insertEffect(ctx, db, jobs.JobIDFromContext(ctx), "mega-sub"); err != nil {
			if isDuplicate(err) {
				return fmt.Sprintf("mega-sub-%d", args.Index), nil
			}
			return "", err
		}
		return fmt.Sprintf("mega-sub-%d", args.Index), nil
	})

	// chaos.megaflow is the deeply-nested torture workflow: it drives the WHOLE
	// durability stack in one job — an idempotent phase Call-chain, a nested
	// fan-out (suspend/resume), a durable timer (suspend/resume), and a final
	// transactional effect — every step keyed by a checkpoint so a SIGKILL at any
	// point replays cleanly. Each effect is unique per (job_id, marker), so it
	// rides the existing INV-EXACTLY-ONCE and INV-FANOUT-COUNTS invariants with no
	// new check. Unlike chaos.fanout it is designed to COMPLETE, exercising the
	// happy resume path end to end.
	q.Register("chaos.megaflow", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		// Per-invocation nonce; see chaos.pipeline.
		nonce := newAttemptNonce()

		// 1. Idempotent phase Call-chain (atomic effect + checkpoint per phase).
		for _, phase := range []string{"mega-extract", "mega-transform", "mega-load"} {
			if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, phase); ok {
				continue
			}
			err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				if err := insertEffectAttempt(ctx, tx, jobID, "phase:"+phase, nonce); err != nil {
					return err
				}
				return jobs.SavePhaseCheckpointTx(ctx, tx, phase, "ok")
			})
			if err != nil {
				return err
			}
		}

		// 2. Nested fan-out of always-succeeding sub-jobs (suspend -> resume).
		// FanOut is called UNCONDITIONALLY on every replay so its positional
		// checkpoint stays aligned with the Sleep below; it is itself replay-safe
		// (returns cached results, does not re-enqueue subs). Guarding it behind a
		// phase checkpoint would skip it on the post-Sleep replay and desync the
		// durable-call indices — the same rule chaos.fanout/chaos.timer follow.
		subs := make([]jobs.SubJob, 0, megaFanoutWidth)
		for i := 0; i < megaFanoutWidth; i++ {
			subs = append(subs, jobs.Sub("chaos.mega_sub", subArgs{Index: i}, jobs.Retries(3)))
		}
		if _, err := jobs.FanOut[string](ctx, subs, jobs.WithFanOutRetries(0)); err != nil {
			return err
		}

		// 3. Durable timer between fan-out completion and the final effect.
		// Unconditional for the same call-index reason; Sleep is replay-safe.
		if err := jobs.Sleep(ctx, time.Second); err != nil {
			return err
		}

		// 4. Final transactional effect, idempotent on replay.
		if _, done := jobs.LoadPhaseCheckpoint[string](ctx, "mega-done"); done {
			return nil
		}
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if e := insertEffectAttempt(ctx, tx, jobID, "megaflow-done", nonce); e != nil {
				return e
			}
			return jobs.SavePhaseCheckpointTx(ctx, tx, "mega-done", "ok")
		})
		if err != nil {
			return err
		}
		return nil
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
	// pattern proven by chaos.pipeline: the effect + SavePhaseCheckpointTx commit
	// together, so a SIGKILL either commits both (replay short-circuits via
	// LoadPhaseCheckpoint) or neither (replay redoes cleanly). A lost timer effect
	// shows as fired<expected; a doubled one as fired>expected (the per-execution
	// nonce lets the second row be RECORDED rather than rejected, which is the whole
	// reason the old timer-reexec marker existed); a wedge as an unfinished
	// chaos.timer row.
	q.Register("chaos.timer", func(ctx context.Context, _ struct{}) error {
		jobID := jobs.JobIDFromContext(ctx)
		nonce := newAttemptNonce()
		if err := jobs.Sleep(ctx, 2*time.Second); err != nil {
			return err
		}
		if _, done := jobs.LoadPhaseCheckpoint[string](ctx, "timer-effect"); done {
			return nil
		}
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if e := insertEffectAttempt(ctx, tx, jobID, "timer-fired", nonce); e != nil {
				return e
			}
			return jobs.SavePhaseCheckpointTx(ctx, tx, "timer-effect", "ok")
		})
		if err != nil {
			return err
		}
		return nil
	})

	q.Register("chaos.tick", func(ctx context.Context, _ struct{}) error {
		stmt := `INSERT INTO chaos_ticks DEFAULT VALUES`
		if dialect == dialectMySQL {
			stmt = `INSERT INTO chaos_ticks () VALUES ()`
		}
		return db.WithContext(ctx).Exec(stmt).Error
	})
	if err := q.Schedule("chaos.tick", nil, jobs.Every(5*time.Second), jobs.Retries(0)); err != nil {
		panic(err)
	}
}

func pipelineWindowGap() time.Duration {
	return envDuration("CHAOS_WINDOW_GAP", 0)
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

	// Seed counts scale with CHAOS_SCALE (default 1.0 = baseline), with optional
	// per-type CHAOS_SEED_<NAME> overrides. scripts/torture-test.sh raises these to
	// drive thousands of complex jobs; unset they reproduce the original workload.
	scale := envFloat("CHAOS_SCALE", 1.0)
	counts := map[string]int{
		"chaos.unit":            scaledCount("CHAOS_SEED_UNIT", 200, scale),
		"chaos.pipeline":        scaledCount("CHAOS_SEED_PIPELINE", 30, scale),
		"chaos.pipeline_window": scaledCount("CHAOS_SEED_PIPELINE_WINDOW", 20, scale),
		"chaos.fanout":          scaledCount("CHAOS_SEED_FANOUT", 20, scale),
		"chaos.slow":            scaledCount("CHAOS_SEED_SLOW", 10, scale),
		"chaos.megaflow":        scaledCount("CHAOS_SEED_MEGAFLOW", 15, scale),
	}
	for typ, n := range counts {
		for i := 0; i < n; i++ {
			if _, err := a.q.Enqueue(ctx, typ, struct{}{}, jobs.Retries(3)); err != nil {
				return fmt.Errorf("enqueue %s: %w", typ, err)
			}
		}
	}

	// The unique/windowed loops spawn one goroutine per attempt to create real
	// concurrent contention on a single key; cap the attempt count so a high
	// CHAOS_SCALE doesn't fan out into a pathological goroutine storm. They probe
	// dedup correctness, not volume — the bulk jobs above carry the load.
	uniqueAttempts := min(scaledCount("CHAOS_SEED_UNIQUE_ATTEMPTS", 50, scale), 500)
	windowedAttempts := min(scaledCount("CHAOS_SEED_WINDOWED_ATTEMPTS", 50, scale), 500)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var uniqueOK, uniqueDup, uniqueErr int
	for i := 0; i < uniqueAttempts; i++ {
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
	for i := 0; i < windowedAttempts; i++ {
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
	signalsPerWaiter := 3
	signalWaiters := scaledCount("CHAOS_SEED_SIGNAL_WAITERS", 10, scale)
	timerJobs := scaledCount("CHAOS_SEED_TIMER", 15, scale)
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

	// seeded_roots is the count of root jobs enqueued (excludes fan-out sub-jobs,
	// which each parent spawns at run time); scripts/torture-test.sh parses it to
	// report throughput.
	rootsTotal := counts["chaos.unit"] + counts["chaos.pipeline"] + counts["chaos.pipeline_window"] +
		counts["chaos.fanout"] + counts["chaos.slow"] + counts["chaos.megaflow"] +
		uniqueOK + uniqueWindowedOK + signalWaiters + 1 /*sender*/ + timerJobs

	fmt.Printf("seeded workload: scale=%.3g seeded_roots=%d unit=%d pipeline_tx=%d pipeline_window=%d fanout=%d slow=%d megaflow=%d mega_fanout_width=%d unique_attempts=%d unique_inserted=%d duplicate_rejected=%d unique_errors=%d windowed_unique_attempts=%d windowed_unique_ok=%d windowed_unique_returned_ids=%d windowed_unique_errors=%d signal_waiters=%d signals_per_waiter=%d timers=%d\n",
		scale, rootsTotal, counts["chaos.unit"], counts["chaos.pipeline"], counts["chaos.pipeline_window"], counts["chaos.fanout"], counts["chaos.slow"], counts["chaos.megaflow"], envInt("CHAOS_FANOUT_WIDTH", 5), uniqueAttempts, uniqueOK, uniqueDup, uniqueErr, windowedAttempts, uniqueWindowedOK, len(windowedIDs), uniqueWindowedErr, signalWaiters, signalsPerWaiter, timerJobs)
	return nil
}

// harnessTables is every table the seed clears, in one place so the MySQL,
// Postgres and SQLite reset paths cannot list different sets.
var harnessTables = []string{
	"chaos_effects", "chaos_ticks", "chaos_signal_targets", "signals", "checkpoints",
	"fan_outs", "jobs", "unique_locks", "queue_states", "scheduled_fires", "leases",
}

func resetHarnessData(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect == dialectSQLite {
		// SQLite has no TRUNCATE; DELETE FROM is the documented equivalent and the
		// optimizer turns an unqualified one into the same bulk operation.
		for _, t := range harnessTables {
			if err := db.WithContext(ctx).Exec(`DELETE FROM ` + t).Error; err != nil {
				return err
			}
		}
		return nil
	}
	if dialect == dialectMySQL {
		// MySQL TRUNCATE can't target multiple tables or CASCADE; truncate each
		// with FK checks off (the schema has no inter-table FKs, but this keeps
		// the order-independent regardless).
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(`SET FOREIGN_KEY_CHECKS=0`).Error; err != nil {
				return err
			}
			for _, t := range harnessTables {
				if err := tx.Exec(`TRUNCATE TABLE ` + t).Error; err != nil {
					return err
				}
			}
			return tx.Exec(`SET FOREIGN_KEY_CHECKS=1`).Error
		})
	}
	return db.WithContext(ctx).Exec(
		`TRUNCATE TABLE ` + strings.Join(harnessTables, ", ") + ` RESTART IDENTITY CASCADE`).Error
}

func runCheck(ctx context.Context, a *app) error {
	// Drain timeout scales with the workload; CHAOS_DRAIN_TIMEOUT (e.g. "25m")
	// overrides the 120s baseline so large torture runs are not cut short.
	drainTimeout := envDuration("CHAOS_DRAIN_TIMEOUT", 120*time.Second)
	obs, err := waitForDrain(ctx, a.db, drainTimeout, 10*time.Second)
	if err != nil {
		// A query error inside the drain loop is not "did not drain" — it is a
		// broken harness, and it must not be reported as a workload verdict.
		fmt.Printf("drain wait: %v\n", err)
		fmt.Println("chaostest result: DRAIN OBSERVATION FAILED")
		return fmt.Errorf("chaostest drain observation failed: %w", err)
	}

	// The full report runs whether or not the workload drained. It used to
	// short-circuit into a one-line checkNoWedge report on the not-drained path,
	// which meant the run that most needed the other eleven invariants printed none
	// of them. INV-NO-WEDGE and INV-READY-NO-STUCK now read the drain OBSERVATION
	// rather than re-querying afterwards — see the comment on drainObservation.
	results := runAllChecks(ctx, a.db, a.dialect, obs, schedWindow, schedPeriod)

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
		fmt.Printf("%-28s %-4s %-4s %s\n", inv.name, inv.level, status, inv.detail)
	}
	if hardFailed > 0 {
		fmt.Printf("chaostest result: RED baseline reproduced with %d HARD failure(s)\n", hardFailed)
		return fmt.Errorf("%d hard invariant failures", hardFailed)
	}
	fmt.Println("chaostest result: no HARD failures observed")
	return nil
}

// schedWindow / schedPeriod are the production INV-SCHED measurement window and
// the scheduled tick's period.
const (
	schedWindow = 12 * time.Second
	schedPeriod = 5 * time.Second
)

// runAllChecks is THE list of checks the release gate runs. It is a function so a
// test can enumerate the same list production does: the meta-guard
// (TestEveryHardInvariantCanFail) requires every HARD check here to FAIL on an
// empty database, and a check added to a list the test cannot see would slip past
// it — which is precisely how four checks came to be missing their population
// guards.
//
// The schedule window is a parameter only so the test does not have to sleep for
// the production 12 seconds; runCheck always passes the production values.
func runAllChecks(ctx context.Context, db *gorm.DB, dialect string, obs drainObservation, window, period time.Duration) []invariant {
	return []invariant{
		checkExactlyOnce(ctx, db, dialect),
		checkAtLeastOnceWindow(ctx, db),
		checkNoWedge(obs),
		checkReadyNoStuck(obs),
		checkFanOutCounts(ctx, db),
		checkUnique(ctx, db),
		checkUniqueWindowed(ctx, db),
		checkScheduleWindow(ctx, db, window, period),
		checkSignalExactlyOnce(ctx, db),
		checkTimerExactlyOnce(ctx, db),
		checkSlotNoLeak(ctx, db, dialect),
		checkRateWellFormed(ctx, db, dialect),
	}
}

// drainObservation is what the drain loop SAW, retained so the wedge invariants
// can be evaluated against it.
//
// INV-NO-WEDGE and INV-READY-NO-STUCK used to run AFTER waitForDrain returned
// successfully, over sets waitForDrain had just proven empty — waiting=0,
// running=0 and, since no pending row survives a drain, eligible_but_unready=0
// too. They could not fail at the point they ran. Evaluating them BEFORE the drain
// is not the fix either: mid-run there are of course running jobs, and the dq_ready
// promoter heals an unready row within a poll, so a pre-drain snapshot false-fires
// on a perfectly healthy system.
//
// What is real is what PERSISTS across the whole drain window: a workload that
// never quiesced, and a row that stayed eligible-but-unready poll after poll. Both
// are recorded here as the loop runs.
type drainObservation struct {
	drained bool
	// waiting/running are the last sample, which on the not-drained path is the
	// state at the deadline — the wedge itself.
	waiting int64
	running int64
	// stuckStreak is the longest run of consecutive polls that ONE PARTICULAR row
	// spent pending, eligible and dq_ready=false. It is per ROW, not per poll,
	// and the difference is the whole reason this can be trusted: under retry
	// churn a stream of DIFFERENT rows passes through the promoter's ~50ms healing
	// window, so "some row was unready at this instant" is nonzero on nearly every
	// poll of a perfectly healthy busy run. A count-based streak would therefore
	// fail the release gate on exactly the workload the gate exists to run. What
	// cannot happen on a healthy system is the SAME row surviving poll after poll.
	stuckStreak int
	// maxStuck is the largest single-poll count, reported for context only.
	maxStuck int64
	polls    int
	quietFor time.Duration
}

// stuckStreakLimit is how many consecutive 1s polls ONE row must stay
// eligible-but-unready before INV-READY-NO-STUCK calls it a wedge.
//
// Deliberately far out. runReadyPromoter is a dedicated per-worker loop at
// ReadyPromoteInterval (defaults to PollInterval = 50ms here), promoting up to
// maxResumeBatch=100 rows a pass, so four chaos workers heal on the order of 8,000
// rows/second. The only benign way a row waits 30 seconds is a promotion backlog
// of ~240,000 simultaneously-eligible rows, which no chaos or torture seed
// produces. A genuinely unhealed row, by contrast, waits forever.
const stuckStreakLimit = 30

// maxStuckIDsSampled bounds the per-poll id fetch. The streak only needs to know
// whether SOME row persists; a backlog larger than this is already reported by
// maxStuck, and sampling its first rows is enough to notice one that never leaves.
const maxStuckIDsSampled = 200

// foldStuckStreaks advances the per-row consecutive-poll counters by one poll and
// returns the new counters plus the longest streak now standing.
//
// Rows present in this poll carry their previous count forward; every row ABSENT
// from it drops out of the map, which is what resets a transient row to zero. It
// is a separate pure function because the property that matters — a churning
// stream of different rows never accumulates a streak, only a persistent one does
// — is the entire reason INV-READY-NO-STUCK can be trusted on a busy run, and it
// is not something a timing-dependent integration test can pin reliably.
func foldStuckStreaks(prev map[string]int, ids []jobs.UUID) (map[string]int, int) {
	next := make(map[string]int, len(ids))
	longest := 0
	for _, id := range ids {
		n := prev[string(id)] + 1
		next[string(id)] = n
		if n > longest {
			longest = n
		}
	}
	return next, longest
}

func waitForDrain(ctx context.Context, db *gorm.DB, timeout, quietFor time.Duration) (drainObservation, error) {
	obs := drainObservation{quietFor: quietFor}
	deadline := time.Now().Add(timeout)
	quietSince := time.Time{}
	streaks := map[string]int{}
	for time.Now().Before(deadline) {
		obs.polls++
		waiting, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE status = 'waiting'`)
		if err != nil {
			return obs, err
		}
		running, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE status = 'running'`)
		if err != nil {
			return obs, err
		}
		pending, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE status = 'pending'`)
		if err != nil {
			return obs, err
		}
		var stuckIDs []jobs.UUID
		if err := db.WithContext(ctx).Model(&jobs.Job{}).
			Where("status = ?", "pending").
			Where("dq_ready = ?", false).
			Where("(run_at IS NULL OR run_at <= ?)", time.Now()).
			Order("id").
			Limit(maxStuckIDsSampled).
			Pluck("id", &stuckIDs).Error; err != nil {
			return obs, err
		}
		obs.waiting, obs.running = waiting, running
		if int64(len(stuckIDs)) > obs.maxStuck {
			obs.maxStuck = int64(len(stuckIDs))
		}
		var longest int
		streaks, longest = foldStuckStreaks(streaks, stuckIDs)
		if longest > obs.stuckStreak {
			obs.stuckStreak = longest
		}

		if waiting+running+pending == 0 {
			if quietSince.IsZero() {
				quietSince = time.Now()
			}
			if time.Since(quietSince) >= quietFor {
				obs.drained = true
				return obs, nil
			}
		} else {
			quietSince = time.Time{}
		}
		time.Sleep(1 * time.Second)
	}
	return obs, nil
}

// scanCount runs a single scalar count query and SURFACES the error. A bare
// .Scan(&x) that ignores .Error silently turns a failed query (e.g. a MySQL
// "Illegal mix of collations" error) into a zero count, which makes a HARD
// invariant pass vacuously — exactly the masking bug that hid the INV-EXACTLY-ONCE
// window-checkpoint collation failure on MySQL. Every invariant count query must
// go through this so a broken check fails LOUDLY rather than reporting a false PASS.
func scanCount(ctx context.Context, db *gorm.DB, query string, args ...any) (int64, error) {
	var n int64
	if err := db.WithContext(ctx).Raw(query, args...).Scan(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

// checkErr builds a failed invariant for when a check's OWN query errored, so the
// broken check is reported as a HARD/INFO failure instead of masquerading as a pass.
func checkErr(name, level string, err error) invariant {
	return invariant{
		name:   name,
		level:  level,
		pass:   false,
		detail: fmt.Sprintf("check query error: %v", err),
	}
}

// checkExactlyOnce defends the exactly-once contract of effects that commit
// together with their replay-suppressing checkpoint.
//
// duplicate_effect_groups is the primary signal and it is LIVE again: chaos_effects
// is keyed on (job_id, marker, attempt_nonce), so a tx-paired effect that runs a
// second time is RECORDED as a second row instead of being rejected by the unique
// index. Under the old (job_id, marker) key the second INSERT failed with 23505,
// the handler returned an error, and this count could only ever read 0 — the
// fixture masked the exact defect the check exists to find.
//
// It cannot false-fire on the documented at-least-once window: every effect that
// is NOT tx-paired is written with fixedAttemptNonce and therefore still collides,
// so it can never form a duplicate group no matter how many times a SIGKILL
// replays it. atomic_effects is the population guard — the count of rows that
// COULD have duplicated. Zero of them means the exactly-once handlers never ran,
// which must read as a broken gate, not a pass.
func checkExactlyOnce(ctx context.Context, db *gorm.DB, dialect string) invariant {
	const name = "INV-EXACTLY-ONCE"
	duplicateRows, err := scanCount(ctx, db, `
		SELECT count(*) FROM (
			SELECT job_id, marker FROM chaos_effects GROUP BY job_id, marker HAVING count(*) > 1
		) dup`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	atomicEffects, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_effects WHERE attempt_nonce <> ''`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	// This window-checkpoint join has two cross-type hazards that the old
	// error-swallowing .Scan() silently hid on BOTH backends (so the sub-check was
	// effectively dead since the v3 binary-UUID migration):
	//
	//  (a) job_id: chaos_effects.job_id stores the canonical UUID STRING
	//      (insertEffect writes string(jobID)), but checkpoints.job_id is the native
	//      UUID PK — `uuid` on Postgres, `binary(16)` on MySQL. `cp.job_id = ce.job_id`
	//      is "operator does not exist: uuid = text" on Postgres and never matches on
	//      MySQL. Convert the NATIVE column to its canonical text form and compare
	//      strings — converting the native side (not parsing ce.job_id) means a
	//      malformed marker job_id just fails to match instead of raising a cast error.
	//  (b) call_type: checkpoints.call_type is utf8mb4_0900_as_cs (case-sensitivity
	//      hardening) vs SUBSTRING(ce.marker) at the ai_ci table default → MySQL
	//      Error 1267; pin the SUBSTRING operand to as_cs.
	//
	// Postgres has neither the binary encoding nor "illegal mix of collations", so
	// only the ::text cast is needed there.
	cpJobID := "cp.job_id::text"
	markerExpr := "SUBSTRING(ce.marker FROM 15)"
	switch dialect {
	case dialectMySQL:
		cpJobID = "BIN_TO_UUID(cp.job_id)"
		markerExpr = "SUBSTRING(ce.marker FROM 15) COLLATE utf8mb4_0900_as_cs"
	case dialectSQLite:
		// SQLite stores the UUID as a 16-byte blob and has neither a UUID type nor
		// a formatting function, so the canonical text form is assembled by hand.
		// Same principle as the other two: convert the NATIVE column, never parse
		// the marker side.
		cpJobID = "lower(substr(hex(cp.job_id),1,8)||'-'||substr(hex(cp.job_id),9,4)||'-'||" +
			"substr(hex(cp.job_id),13,4)||'-'||substr(hex(cp.job_id),17,4)||'-'||substr(hex(cp.job_id),21,12))"
		markerExpr = "substr(ce.marker, 15)"
	}
	windowCheckpointedRows, err := scanCount(ctx, db, `
		SELECT count(*)
		FROM chaos_effects ce
		WHERE ce.marker LIKE 'window-reexec:%'
		  AND EXISTS (
			SELECT 1
			FROM checkpoints cp
			WHERE `+cpJobID+` = ce.job_id
			  AND cp.call_index = -1
			  AND cp.call_type = `+markerExpr+`
		  )`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	pass := atomicEffects > 0 && duplicateRows == 0 && windowCheckpointedRows == 0
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("tx pipeline: atomic_effects=%d duplicate_effect_groups=%d; window checkpointed_reexec_markers=%d", atomicEffects, duplicateRows, windowCheckpointedRows),
	}
}

// checkAtLeastOnceWindow is REPORTING ONLY and is named accordingly.
//
// It hardcodes pass:true — the at-least-once re-execution it counts is the
// documented behaviour of chaos.pipeline_window, not a defect — so calling it
// INV-anything put a line in the release gate's report that says PASS no matter
// what the system did. That is precisely how a vacuous check regrows: the name
// claims an invariant, the reader counts the PASS, and nothing is being asserted.
// The REPORT- prefix says what it is.
func checkAtLeastOnceWindow(ctx context.Context, db *gorm.DB) invariant {
	const name = "REPORT-AT-LEAST-ONCE-WINDOW"
	windowRows, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_effects WHERE marker LIKE 'window-reexec:%'`)
	if err != nil {
		return checkErr(name, "INFO", err)
	}
	return invariant{
		name:   name,
		level:  "INFO",
		pass:   true,
		detail: fmt.Sprintf("reporting only, asserts nothing: window_reexec_markers=%d expected at-least-once re-execution under SIGKILL; bounded by design", windowRows),
	}
}

// checkNoWedge asserts the workload actually reached quiescence inside the drain
// window, evaluated on the drain OBSERVATION rather than on a post-drain query.
// Queried afterwards it could only ever see the zeros waitForDrain had just
// waited for; read from the observation it fails exactly when the drain timed out
// with work still in flight, and reports the state at the deadline.
func checkNoWedge(obs drainObservation) invariant {
	return invariant{
		name:  "INV-NO-WEDGE",
		level: "HARD",
		pass:  obs.drained,
		detail: fmt.Sprintf("drained=%t last_waiting=%d last_running=%d polls=%d quiet_for=%s",
			obs.drained, obs.waiting, obs.running, obs.polls, obs.quietFor),
	}
}

// checkReadyNoStuck asserts the dq_ready promoter backstop left no pending job
// eligible-to-run-now but still flagged dq_ready=false. Such a row is invisible
// to Dequeue (which requires dq_ready=true) — a latent wedge the per-worker
// promoter must heal.
//
// It is evaluated on the PERSISTENCE of that state for one ROW across the drain,
// not on a single sample and not on a per-poll count. A single post-drain sample
// is empty by construction (a drained system has no pending rows at all); a single
// mid-drain sample flags a row the promoter is about to heal on the next poll; and
// a per-poll COUNT is nonzero on nearly every poll of a healthy busy run, because
// retry churn keeps feeding different rows through the promoter's ~50ms window.
// One row surviving poll after poll is the only shape of this question that both
// can fail and is true only when something is actually broken.
func checkReadyNoStuck(obs drainObservation) invariant {
	return invariant{
		name:  "INV-READY-NO-STUCK",
		level: "HARD",
		pass:  obs.stuckStreak < stuckStreakLimit,
		detail: fmt.Sprintf("longest_consecutive_polls_one_row_stayed_eligible_but_unready=%d limit=%d max_rows_in_a_poll=%d polls=%d",
			obs.stuckStreak, stuckStreakLimit, obs.maxStuck, obs.polls),
	}
}

// checkFanOutCounts asserts every fan-out's per-child counters sum to its total.
// fan_out_rows is the population guard: a regression that stops fan-outs being
// created at all leaves nothing to mismatch, and a check with no population must
// read as broken rather than clean.
func checkFanOutCounts(ctx context.Context, db *gorm.DB) invariant {
	const name = "INV-FANOUT-COUNTS"
	total, err := scanCount(ctx, db, `SELECT count(*) FROM fan_outs`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	bad, err := scanCount(ctx, db, `
		SELECT count(*) FROM fan_outs
		WHERE completed_count + failed_count + cancelled_count <> total_count`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   total > 0 && bad == 0,
		detail: fmt.Sprintf("fan_out_rows=%d mismatched_counts=%d", total, bad),
	}
}

func checkUnique(ctx context.Context, db *gorm.DB) invariant {
	const name = "INV-UNIQUE"
	count, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE unique_key = 'dup-key-1'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   count == 1,
		detail: fmt.Sprintf("jobs_with_dup_key_1=%d", count),
	}
}

func checkUniqueWindowed(ctx context.Context, db *gorm.DB) invariant {
	const name = "INV-UNIQUE-WINDOWED"
	count, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE type = 'chaos.unique_windowed'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   count == 1,
		detail: fmt.Sprintf("jobs_with_windowed_dup_key_1=%d", count),
	}
}

// checkScheduleWindow measures the steady-state fire rate over a fresh window
// while the worker replicas are still running. The window is a parameter (rather
// than the production 12-second constant inline) so a test can exercise the logic
// without a 12-second sleep — this check used to be one no test ever ran.
func checkScheduleWindow(ctx context.Context, db *gorm.DB, window, period time.Duration) invariant {
	// A correctly fleet-deduplicated scheduler fires the tick ~once per period
	// regardless of replica count; without dedup, N replicas each fire (and a
	// scheduler that boot-storms re-fires on every chaos respawn). Counting over a
	// window — rather than total ticks since seed — avoids the earlier drain-time
	// accounting error.
	before, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_ticks`)
	if err != nil {
		return checkErr("INV-SCHED", "HARD", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(window):
	}
	after, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_ticks`)
	if err != nil {
		return checkErr("INV-SCHED", "HARD", err)
	}
	got := after - before
	// One logical scheduler: floor(window/period) boundaries, +2 slack for
	// boundary alignment and a tick landing at each edge of the window.
	maxExpected := int64(window/period) + 2
	// And a floor. The bound used to be one-sided, so a scheduler that fired ZERO
	// times in the window PASSED — verified: ticks_in_12s_window=0 was reported as
	// PASS. A dead scheduler is at least as bad as a double-firing one, and this
	// check is the only thing watching for it.
	//
	// 1, not floor(window/period): the window is 12s at a 5s period, so a correct
	// scheduler fires 2-3 times, and the floor is deliberately set well below that
	// so boundary alignment, a chaos kill landing mid-window, or a slow dequeue can
	// never manufacture a failure. It catches "the scheduler stopped", which is the
	// defect, not "the scheduler is a beat late".
	const minExpected = 1
	return invariant{
		name: "INV-SCHED",
		// HARD as of the shared-anchor scheduler fix: a fresh schedule now seeds
		// a fleet-wide base (SeedScheduledFire), so skewed worker clocks can no
		// longer make replicas target different first boundaries and double-fire.
		level:  "HARD",
		pass:   got >= minExpected && got <= maxExpected,
		detail: fmt.Sprintf("ticks_in_%s_window=%d min_expected=%d max_expected_single_scheduler=%d", window, got, minExpected, maxExpected),
	}
}

// checkSignalExactlyOnce defends P1: every seeded signal is consumed exactly once
// and no waiter is left wedged. A P1 lost-signal regression manifests as
// consumed<expected AND/OR a chaos.signal_waiter row stuck in a non-completed
// status. The at-least-once re-exec count is reported for visibility only (the
// downstream effect is idempotent by design and never fails this HARD check).
// expected>0 guards against a vacuous PASS when seeding produced nothing.
func checkSignalExactlyOnce(ctx context.Context, db *gorm.DB) invariant {
	const name = "INV-SIGNAL-EXACTLY-ONCE"
	expected, err := scanCount(ctx, db, `SELECT COALESCE(SUM(sig_count),0) FROM chaos_signal_targets`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	consumed, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_effects WHERE marker LIKE 'sig-consumed:%'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	unfinished, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE type = 'chaos.signal_waiter' AND status <> 'completed'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	reexec, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_effects WHERE marker LIKE 'sig-reexec:%'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	pass := expected > 0 && consumed == expected && unfinished == 0
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("expected=%d consumed=%d unfinished_waiters=%d at_least_once_reexec=%d", expected, consumed, unfinished, reexec),
	}
}

// checkTimerExactlyOnce defends the durable-timer path and P3: each timer fires
// its effect exactly once with no re-execution and no wedge. A lost effect shows
// as fired<expected, a doubled effect as fired>expected, and a re-sleep/wedge as
// unfinished>0. expected>0 guards against a vacuous PASS.
//
// The separate timer-reexec sub-check is gone with the marker that fed it. It
// existed only because the old (job_id, marker) unique index made a second
// timer-fired row impossible to write; now the duplicate row IS the signal, and it
// lands in fired — a strictly more direct assertion than a marker the handler had
// to notice a constraint violation to produce.
func checkTimerExactlyOnce(ctx context.Context, db *gorm.DB) invariant {
	const name = "INV-TIMER-EXACTLY-ONCE"
	expected, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE type = 'chaos.timer'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	fired, err := scanCount(ctx, db, `SELECT count(*) FROM chaos_effects WHERE marker = 'timer-fired'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	unfinished, err := scanCount(ctx, db, `SELECT count(*) FROM jobs WHERE type = 'chaos.timer' AND status <> 'completed'`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	pass := expected > 0 && fired == expected && unfinished == 0
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   pass,
		detail: fmt.Sprintf("expected=%d fired=%d unfinished=%d", expected, fired, unfinished),
	}
}

// checkSlotNoLeak asserts no concurrency slot outlived the job that held it.
//
// slot_rows is the population guard. Every slot a job holds is DELETED on release,
// so after a clean drain the only rows left are the per-key sentinels — which is
// exactly why the leak count alone is vacuous: a run in which ConcurrencyCap was
// never exercised at all (a dropped option, a seed that produced no capped jobs)
// leaves an empty table and reports live_nonsentinel_slots=0, indistinguishable
// from a clean run. The sentinel is never deleted, so its presence is the proof
// the machinery ran.
func checkSlotNoLeak(ctx context.Context, db *gorm.DB, dialect string) invariant {
	const name = "INV-SLOT-NO-LEAK"
	nowExpr := "NOW()"
	var args []any
	if dialect == dialectSQLite {
		nowExpr = "?"
		args = append(args, time.Now().UTC())
	}
	var n int64
	// The sentinel slot is stored as the nil UUID (16 zero bytes); job_id is now a
	// binary uuid column, so compare against the bound nil-UUID value (its Value()
	// encodes to 16 zero bytes per dialect) rather than the literal ''. Checking the
	// error matters: a comparison that fails to typecheck must fail the invariant,
	// not silently leave n=0 and report a false pass.
	args = append(args, jobs.NilUUID)
	if err := db.WithContext(ctx).
		Raw(`SELECT count(*) FROM concurrency_slots WHERE expires_at > `+nowExpr+` AND job_id <> ?`, args...).
		Scan(&n).Error; err != nil {
		return invariant{
			name:   name,
			level:  "HARD",
			pass:   false,
			detail: fmt.Sprintf("slot-leak query failed: %v", err),
		}
	}
	slotRows, err := scanCount(ctx, db, `SELECT count(*) FROM concurrency_slots`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   slotRows > 0 && n == 0,
		detail: fmt.Sprintf("slot_rows=%d live_nonsentinel_slots=%d", slotRows, n),
	}
}

// checkRateWellFormed asserts no rate-limit window ever went negative. total_windows
// is the population guard: RateLimit("chaos", ...) is configured on every worker, so
// an empty table means the limiter never ran and there was nothing to be well-formed.
func checkRateWellFormed(ctx context.Context, db *gorm.DB, dialect string) invariant {
	const name = "INV-RATE-WELLFORMED"
	countColumn := `"count"`
	if dialect == dialectMySQL {
		countColumn = "`count`"
	}
	negs, err := scanCount(ctx, db, `SELECT count(*) FROM rate_limit_windows WHERE `+countColumn+` < 0`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	total, err := scanCount(ctx, db, `SELECT count(*) FROM rate_limit_windows`)
	if err != nil {
		return checkErr(name, "HARD", err)
	}
	return invariant{
		name:   name,
		level:  "HARD",
		pass:   total > 0 && negs == 0,
		detail: fmt.Sprintf("negative_counts=%d total_windows=%d", negs, total),
	}
}

func insertSignalTarget(ctx context.Context, db *gorm.DB, jobID jobs.UUID, n int) error {
	return db.WithContext(ctx).Exec(`INSERT INTO chaos_signal_targets (job_id, sig_count) VALUES (?, ?)`, string(jobID), n).Error
}

// insertEffect records an at-least-once effect: it carries fixedAttemptNonce, so
// a replay of the SAME (job_id, marker) still collides with the unique index and
// still surfaces as a duplicate-key error, exactly as it did before attempt_nonce
// existed. Every handler whose effect is not committed together with a checkpoint
// uses this.
func insertEffect(ctx context.Context, db *gorm.DB, jobID jobs.UUID, marker string) error {
	return insertEffectAttempt(ctx, db, jobID, marker, fixedAttemptNonce)
}

// insertEffectAttempt records an effect under a PER-EXECUTION nonce, so a second
// execution of the same (job_id, marker) is RECORDED as a second row rather than
// rejected.
//
// It is used only for effects written inside a checkpoint transaction. There the
// checkpoint that suppresses replay commits with the effect, so the effect is
// exactly-once by construction and a duplicate row can only mean the replay
// suppression failed — which is precisely what INV-EXACTLY-ONCE's
// duplicate_effect_groups is for, and what the old (job_id, marker) unique index
// made unobservable.
//
// The nonce also IDENTIFIES the exactly-once population: an effect row with a
// non-empty attempt_nonce is one that could have produced a duplicate group, which
// is what INV-EXACTLY-ONCE's population guard counts.
func insertEffectAttempt(ctx context.Context, db *gorm.DB, jobID jobs.UUID, marker, nonce string) error {
	id := string(jobID)
	if id == "" {
		id = "unknown"
	}
	return db.WithContext(ctx).Exec(
		`INSERT INTO chaos_effects (job_id, marker, attempt_nonce) VALUES (?, ?, ?)`, id, marker, nonce).Error
}

// newAttemptNonce returns a value unique to ONE handler invocation. A replay is a
// new invocation and therefore a new nonce, which is what makes a re-executed
// tx-paired effect recordable instead of a constraint violation.
func newAttemptNonce() string {
	return string(jobs.NewID())
}

func insertEffectIgnoreDuplicate(ctx context.Context, db *gorm.DB, dialect string, jobID jobs.UUID, marker string) error {
	id := string(jobID)
	if id == "" {
		id = "unknown"
	}
	// The conflict target must name the full unique key. These markers are written
	// with fixedAttemptNonce so they still collide (that is the point: one re-exec
	// marker per job, not one per retry).
	stmt := `INSERT INTO chaos_effects (job_id, marker, attempt_nonce) VALUES (?, ?, ?) ` +
		`ON CONFLICT (job_id, marker, attempt_nonce) DO NOTHING`
	if dialect == dialectMySQL {
		stmt = `INSERT IGNORE INTO chaos_effects (job_id, marker, attempt_nonce) VALUES (?, ?, ?)`
	}
	return db.WithContext(ctx).Exec(stmt, id, marker, fixedAttemptNonce).Error
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

// --- env-configurable torture knobs -----------------------------------------
//
// All default to the pre-torture baseline so `chaos-test.sh` / CI behavior is
// unchanged when these are unset (CHAOS_SCALE=1.0, baseline seed counts, 120s
// drain). scripts/torture-test.sh sets them to drive thousands of jobs.

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		log.Printf("chaostest: ignoring invalid %s=%q, using %v", key, v, def)
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("chaostest: ignoring invalid %s=%q, using %d", key, v, def)
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("chaostest: ignoring invalid %s=%q, using %s", key, v, def)
	}
	return def
}

// scaledCount returns an explicit per-type override (CHAOS_SEED_<NAME>) when set,
// otherwise round(base * CHAOS_SCALE). Never negative.
func scaledCount(envKey string, base int, scale float64) int {
	if v := os.Getenv(envKey); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			if n < 0 {
				n = 0
			}
			return n
		}
		log.Printf("chaostest: ignoring invalid %s=%q, using scaled default", envKey, v)
	}
	n := int(float64(base)*scale + 0.5)
	if n < 0 {
		n = 0
	}
	return n
}
