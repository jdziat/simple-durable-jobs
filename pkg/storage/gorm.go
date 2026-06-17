package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
)

const (
	defaultLockDuration = 45 * time.Minute
	maxResumeBatch      = 100
	deadLetterReasonKey = "__dead_letter_reason"
	// maxFanOutTreeDepth bounds the fan-out subtree walk in Requeue. Real
	// workflows nest only a few levels; this is insurance against pathologically
	// deep (or cyclically corrupt) data, not a normal limit. Per-level deletion
	// already makes a cycle impossible to loop on (visited rows are gone), so
	// this is belt-and-suspenders.
	maxFanOutTreeDepth = 256
)

var indexedMetadataKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,48}$`)

// GormStorageOption configures a GormStorage.
type GormStorageOption func(*GormStorage)

// WithStorageLockDuration sets the duration for which a job lock is held when
// dequeued or extended by a heartbeat. The default is 45 minutes.
func WithStorageLockDuration(d time.Duration) GormStorageOption {
	return func(s *GormStorage) {
		s.lockDuration.Store(int64(d))
	}
}

// WithCodec configures a payload codec for bytes stored in payload columns.
// Nil selects the default identity codec.
func WithCodec(c core.PayloadCodec) GormStorageOption {
	return func(s *GormStorage) {
		if c == nil {
			s.codec = core.IdentityCodec
			return
		}
		s.codec = c
	}
}

// WithIndexedMetadataKeys declares metadata keys that should get MySQL
// generated-column indexes for dashboard/DLQ tag filters. This is a MySQL-only
// optimization: PostgreSQL is already covered by the metadata GIN index, and
// SQLite keeps using the LIKE fallback. Declaring a key makes filters on that
// key use a generated-column index instead of a full metadata scan.
//
// Migrate only ADDS the generated column + index for each declared key; it never
// drops them. Removing a key from this list on a later boot leaves its (inert,
// narrow) meta_<key> column and idx_jobs_meta_<key> index in place — they simply
// stop being used. Drop them manually if you want them gone. Keys are validated
// against ^[A-Za-z0-9_]{1,48}$ (they are interpolated into DDL); an invalid key
// fails Migrate before any DDL runs.
func WithIndexedMetadataKeys(keys ...string) GormStorageOption {
	return func(s *GormStorage) {
		seen := make(map[string]struct{}, len(keys))
		deduped := make([]string, 0, len(keys))
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			deduped = append(deduped, key)
		}
		// Stored behind a pointer so GormStorage stays comparable (a []string
		// field would make the struct non-comparable, an api-compat break).
		s.indexedMetadataKeys = &deduped
	}
}

// indexedMetadataKeyList returns the declared indexed metadata keys (nil if none).
func (s *GormStorage) indexedMetadataKeyList() []string {
	if s.indexedMetadataKeys == nil {
		return nil
	}
	return *s.indexedMetadataKeys
}

// withoutDefaultPool disables the bounded connection pool that NewGormStorage
// applies by default. It is unexported because it exists only so
// NewGormStorageWithPool (which has already sized the pool itself, possibly to
// an explicit unlimited) can opt out of the auto-default; it is not part of the
// public API and is not re-exported through jobs.go.
func withoutDefaultPool() GormStorageOption {
	return func(s *GormStorage) {
		s.skipDefaultPool = true
	}
}

// GormStorage implements Storage using GORM.
var _ core.Storage = (*GormStorage)(nil)

type GormStorage struct {
	db                  *gorm.DB
	isSQLite            bool
	lockDuration        atomic.Int64
	codec               core.PayloadCodec
	indexedMetadataKeys *[]string
	skipDefaultPool     bool

	// deleteCheckpointsOnComplete, when true, deletes a job's checkpoints in the
	// SAME transaction as its success terminal write (Complete /
	// CompleteWithResult). It is OFF by default: the dashboard reads completed
	// jobs' checkpoints to show finished-workflow phase results, so deleting them
	// silently would blank that panel. Operators opt in via the worker's
	// RetentionDeleteCheckpointsOnComplete option (wired through
	// SetDeleteCheckpointsOnComplete) to bound the checkpoints table. Never
	// affects the failure path — retry replay reads checkpoints.
	deleteCheckpointsOnComplete atomic.Bool
}

// NewGormStorage creates a new GORM-backed storage. For file-based SQLite under
// concurrent workers, callers MUST open the database with jobs.SafeSQLiteDSN or
// an equivalent concurrency-safe DSN, e.g.
//
//	sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate")
//
// These DSN parameters are applied by the driver to every connection the pool
// opens. WAL lets readers and the single writer proceed without blocking each
// other; _busy_timeout makes writers wait for the lock instead of failing
// immediately; _txlock=immediate takes the write lock up front so deferred
// transactions cannot deadlock on a lock upgrade. Without them, concurrent
// completion writes transiently fail with SQLITE_BUSY/SQLITE_READONLY and can
// exhaust the worker retry budget. This library receives an already-opened DB
// and cannot set these itself — the PRAGMA below is only a best-effort fallback.
//
// Connection pool: NewGormStorage installs a bounded default pool via
// applyDefaultPoolIfUnset unless the pool is already sized. PG/MySQL get
// DefaultPoolConfig (MaxOpenConns 25, MaxIdleConns 10, ConnMaxLifetime 5m,
// ConnMaxIdleTime 1m); SQLite gets a small, SQLite-safe pool (4 open, 2 idle, no
// connection expiry). A freshly-opened gorm DB sits at database/sql defaults
// (unlimited), so a non-zero MaxOpenConnections in the pool's Stats means the
// caller already bounded it and we leave it untouched. The default is also
// skipped when the storage is built through NewGormStorageWithPool, which sizes
// the pool itself (including an explicit MaxOpenConns(0) for unlimited).
func NewGormStorage(db *gorm.DB, opts ...GormStorageOption) *GormStorage {
	// Detect SQLite by checking the dialect name
	isSQLite := false
	if db != nil {
		dialector := db.Name()
		isSQLite = strings.Contains(strings.ToLower(dialector), "sqlite")
	}
	s := &GormStorage{db: db, isSQLite: isSQLite, codec: core.IdentityCodec}
	s.lockDuration.Store(int64(defaultLockDuration))
	if s.isSQLite {
		// NOTE: a one-shot PRAGMA only affects the single pooled connection it
		// runs on; connections the pool opens later default to busy_timeout=0.
		// The DSN parameters above are the reliable, pool-wide mechanism.
		_ = db.Exec("PRAGMA busy_timeout = 5000").Error
	}
	for _, opt := range opts {
		opt(s)
	}
	// Apply a bounded default pool unless opted out (NewGormStorageWithPool) or
	// the caller already sized the pool. Must run after the opts loop so
	// withoutDefaultPool() is observed.
	if !s.skipDefaultPool {
		applyDefaultPoolIfUnset(db, isSQLite)
	}
	return s
}

// SetLockDuration updates storage-wide lock state; last setter wins, so use
// separate GormStorage instances for divergent per-worker config.
// This is used by the worker when WithLockDuration is passed as a worker option.
func (s *GormStorage) SetLockDuration(d time.Duration) {
	s.lockDuration.Store(int64(d))
}

// SetDeleteCheckpointsOnComplete toggles storage-wide checkpoint GC state; last
// setter wins, so use separate GormStorage instances for divergent per-worker
// config.
//
// It applies checkpoint GC on the
// success terminal write. The worker calls it when the opt-in
// RetentionDeleteCheckpointsOnComplete option is set. Default (unset) preserves
// completed-job checkpoints so the dashboard can still show their phase results.
// It never affects the failure path; retry replay always keeps checkpoints.
func (s *GormStorage) SetDeleteCheckpointsOnComplete(enabled bool) {
	s.deleteCheckpointsOnComplete.Store(enabled)
}

// IsSQLite returns true if the storage is using SQLite.
// SQLite doesn't support FOR UPDATE SKIP LOCKED and uses a fallback strategy.
func (s *GormStorage) IsSQLite() bool {
	return s.isSQLite
}

// DB returns the underlying GORM database connection.
// This is useful for running custom queries or accessing the database directly.
func (s *GormStorage) DB() *gorm.DB {
	return s.db
}

// Migrate creates the necessary tables and applies versioned schema migrations.
//
// AutoMigrate creates/extends the base tables (it only ever adds columns and
// indexes, never replaces or backfills). Anything that requires replacing an
// index, adding a generated column, or transforming data is expressed as a
// versioned migration in migrations.go and applied by runMigrations, which
// records each applied version in the schema_migrations ledger so it runs at
// most once per database.
func (s *GormStorage) Migrate(ctx context.Context) error {
	// Hold a fleet-wide lock for the whole migration so concurrent Migrate()
	// calls from every worker serialize rather than racing on DDL (gorm's
	// AutoMigrate does check-then-create, which is not concurrency-safe on its
	// own — two workers can both try to CREATE the same table/index).
	return s.withMigrationLock(ctx, func(db *gorm.DB) error {
		if s.dialect() == dialectMySQL {
			if err := requireMySQLUTCSession(ctx, db); err != nil {
				return err
			}
		}

		// Heal pre-existing NULL dispatcher columns BEFORE AutoMigrate enforces
		// their NOT NULL constraints (cheap EXISTS short-circuit on clean tables).
		if err := backfillDispatcherNulls(ctx, db, s.dialect()); err != nil {
			return err
		}
		if err := s.applyPreMigrations(ctx, db); err != nil {
			return err
		}

		if err := db.AutoMigrate(
			&core.Job{},
			&core.Checkpoint{},
			&core.FanOut{},
			&core.QueueState{},
			&core.ScheduledFire{},
			&core.Lease{},
			&core.Signal{},
			&core.ConcurrencySlot{},
			&core.RateLimitWindow{},
			&core.UniqueLock{},
			&core.SchemaMigration{},
		); err != nil {
			return err
		}

		// SQLite and PostgreSQL enforce active-job uniqueness with a partial
		// unique index, which frees the key once a job reaches a terminal
		// status. MySQL has no partial indexes; it gets an equivalent
		// generated-column unique index in migration 2 (mysql_active_unique_key).
		if s.isSQLite || s.dialect() == dialectPostgres {
			if err := db.Exec(`
				CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_unique
				ON jobs (unique_key)
				WHERE unique_key <> '' AND status IN ('pending','running')
			`).Error; err != nil {
				return err
			}
		}

		if err := s.applyPendingMigrations(ctx, db); err != nil {
			return err
		}
		return s.applyIndexedMetadataKeyIndexes(ctx, db)
	})
}

func (s *GormStorage) applyIndexedMetadataKeyIndexes(ctx context.Context, db *gorm.DB) error {
	keys := s.indexedMetadataKeyList()
	if s.dialect() != dialectMySQL || len(keys) == 0 {
		return nil
	}
	for _, key := range keys {
		if !indexedMetadataKeyPattern.MatchString(key) {
			return fmt.Errorf("storage: indexed metadata key %q must match [A-Za-z0-9_]{1,48}", key)
		}
	}
	m := db.Migrator()
	for _, key := range keys {
		indexName := "idx_jobs_meta_" + key
		columnName := mysqlMetadataGenColumn(key)
		if !m.HasColumn("jobs", columnName) {
			if err := db.WithContext(ctx).Exec(
				"ALTER TABLE jobs ADD COLUMN " + mysqlMetadataGenColumnDefinition(key),
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("add %s column: %w", columnName, err)
			}
		}
		if !m.HasIndex("jobs", indexName) {
			if err := db.WithContext(ctx).Exec(
				"ALTER TABLE jobs ADD INDEX " + indexName + " (" + columnName + ")",
			).Error; err != nil && !isBenignDDLError(err) {
				return fmt.Errorf("create %s: %w", indexName, err)
			}
		}
	}
	return nil
}

// backfillDispatcherNulls fills pre-existing NULL columns on the jobs table with
// their model defaults BEFORE AutoMigrate enforces the corresponding NOT NULL
// constraints. It runs on every Migrate call, but a cheap EXISTS probe
// short-circuits to a no-op on the overwhelmingly common clean table.
//
// The covered columns are exactly the jobs columns that the core.Job model marks
// `not null` AND that can pre-exist as nullable in a legacy baseline (so the
// constraint is later applied to an existing column — via AutoMigrate's
// `ALTER COLUMN ... SET NOT NULL`/MySQL column rebuild, or migrateIntWidthRightsizing's
// `MODIFY ... NOT NULL` — rather than a backfilling `ADD COLUMN ... NOT NULL DEFAULT`):
//   - status/queue/priority/attempt/max_retries — the original dispatcher set.
//   - timeout — shipped originally WITHOUT a gorm tag (a genuinely nullable era), then
//     later tightened to `not null;default:0`.
//   - determinism — likewise introduced untagged/nullable and later tightened to
//     `not null;default:0`. A pre-v3 baseline therefore holds NULL for both columns,
//     and migrateIntWidthRightsizing's `MODIFY determinism int NOT NULL DEFAULT 0`
//     aborts at boot on a NULL determinism (MySQL Error 1265; PG would 23502 on
//     SET NOT NULL).
//
// A baseline holding any of these NULLs would otherwise crash Migrate at boot (PG
// 23502 / MySQL 1265). type was `not null` from introduction (no baseline can hold a
// NULL); fan_outs.total_count is `not null` with no default and intrinsic to every
// fan-out row, so it is never NULL in a real baseline.
func backfillDispatcherNulls(ctx context.Context, db *gorm.DB, dialect string) error {
	if dialect != dialectPostgres && dialect != dialectMySQL {
		return nil
	}
	if !db.Migrator().HasTable(&core.Job{}) {
		return nil
	}
	// Cheap EXISTS short-circuit: on a clean table (the overwhelmingly common
	// case) this probe is sub-millisecond and avoids the full-table UPDATE on
	// every startup (R25), while still letting the heal run BEFORE AutoMigrate's
	// SET NOT NULL so a legacy nullable table that still holds NULLs is fixed
	// before the constraint is enforced (preserves the schema-campaign ordering).
	var hasNulls bool
	if err := db.WithContext(ctx).Raw(`
		SELECT EXISTS(
			SELECT 1 FROM jobs
			WHERE status IS NULL OR queue IS NULL OR priority IS NULL
			   OR attempt IS NULL OR max_retries IS NULL OR timeout IS NULL
			   OR determinism IS NULL
		)
	`).Scan(&hasNulls).Error; err != nil {
		return err
	}
	if !hasNulls {
		return nil
	}
	return db.WithContext(ctx).Exec(`
		UPDATE jobs
		SET status = COALESCE(status, 'pending'),
		    queue = COALESCE(queue, 'default'),
		    priority = COALESCE(priority, 0),
		    attempt = COALESCE(attempt, 0),
		    max_retries = COALESCE(max_retries, 3),
		    timeout = COALESCE(timeout, 0),
		    determinism = COALESCE(determinism, 0)
		WHERE status IS NULL OR queue IS NULL OR priority IS NULL
		   OR attempt IS NULL OR max_retries IS NULL OR timeout IS NULL
		   OR determinism IS NULL
	`).Error
}

func requireMySQLUTCSession(ctx context.Context, db *gorm.DB) error {
	var offsetSeconds int64
	if err := db.WithContext(ctx).Raw(
		"SELECT TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(6), NOW(6))",
	).Scan(&offsetSeconds).Error; err != nil {
		return fmt.Errorf("storage: check MySQL session clock: %w", err)
	}
	if offsetSeconds != 0 {
		return fmt.Errorf("storage: MySQL session clock is not UTC; NOW(6) drives DB-clock lock/lease writes; set time_zone='+00:00' or loc=UTC in the DSN")
	}
	return nil
}

// Enqueue adds a job to the queue.
func (s *GormStorage) Enqueue(ctx context.Context, job *core.Job) error {
	fillEnqueueDefaults(job)
	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return err
	}
	db := s.db.WithContext(ctx)
	if job.UniqueKey == "" {
		dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
		if err := db.Create(row).Error; err != nil {
			return err
		}
		return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrDuplicateJob
	}
	return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
}

// withSerializationRetry runs fn and retries it on transient serialization
// failures reported by the underlying driver. MySQL REPEATABLE READ turns
// gap-locked inserts into deadlocks (error 1213); lock wait timeouts (1205)
// surface under contention; Postgres reports serialization_failure (40001)
// and deadlock_detected (40P01) under SERIALIZABLE/READ COMMITTED mixes.
// All of these are advisory — the client is expected to re-run the tx.
func (s *GormStorage) withSerializationRetry(ctx context.Context, fn func() error) error {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isSerializationFailure(lastErr) {
			return lastErr
		}
		// Linear backoff with a small cap — the retriable errors above
		// clear on the next acquisition, so we do not need exponential. Add up
		// to 50% jitter so a herd of workers deadlocking on the same rows does
		// not retry in lockstep and re-collide on the next attempt.
		base := time.Duration(attempt+1) * 5 * time.Millisecond
		jitter := time.Duration(rand.Int64N(int64(base/2) + 1))
		time.Sleep(base + jitter)
	}
	return fmt.Errorf("transaction failed after %d retries: %w", maxAttempts, lastErr)
}

// WithSerializationRetry retries fn on transient serialization/deadlock
// failures with jittered backoff. Use it to wrap caller-owned outbox
// transactions on MySQL, for example begin -> EnqueueBatchTx -> commit, so the
// whole transaction is retried when gap-lock deadlocks surface.
func (s *GormStorage) WithSerializationRetry(ctx context.Context, fn func() error) error {
	return s.withSerializationRetry(ctx, fn)
}

// isSerializationFailure reports whether err is a transient
// serialization/deadlock failure that can be resolved by retrying the
// enclosing transaction. String matching is used deliberately to avoid
// pulling mysql and pgconn driver packages in just for error typing.
func isSerializationFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Error 1213"), // MySQL deadlock
		strings.Contains(msg, "Deadlock found"),                       // MySQL deadlock (text)
		strings.Contains(msg, "Error 1205"),                           // MySQL lock wait timeout
		strings.Contains(msg, "Lock wait timeout"),                    // MySQL (text)
		strings.Contains(msg, "SQLSTATE 40001"),                       // serialization_failure
		strings.Contains(msg, "SQLSTATE 40P01"),                       // deadlock_detected (pg)
		strings.Contains(msg, "could not serialize"),                  // pg text
		strings.Contains(msg, "deadlock detected"),                    // pg text
		strings.Contains(msg, "database is locked"),                   // SQLite SQLITE_BUSY (5)
		strings.Contains(msg, "database table is locked"),             // SQLite SQLITE_LOCKED (6)
		strings.Contains(msg, "SQLITE_BUSY"),                          // SQLite (wrapped form)
		strings.Contains(msg, "SQLITE_LOCKED"),                        // SQLite (wrapped form)
		strings.Contains(msg, "attempt to write a readonly database"), // SQLite SQLITE_READONLY (8)
		strings.Contains(msg, "SQLITE_READONLY"):                      // SQLite (wrapped form)
		// SQLite returns BUSY/LOCKED immediately on write-write contention that
		// busy_timeout cannot resolve (one writer must abort); these are
		// transient and safe to retry, mirroring the MySQL/PG deadlock handling.
		//
		// SQLITE_READONLY surfaces transiently in rollback-journal mode when a
		// connection encounters a hot journal it must roll back (a write) but
		// cannot acquire the lock to do so because a concurrent writer holds it.
		// It clears once the other writer commits, so it is retryable too. A
		// genuinely read-only database (file permissions) simply exhausts the
		// bounded retries and surfaces the same error — no worse than before.
		return true
	}
	return false
}

// EnqueueUnique adds a job only if no job with the same unique key exists in pending/running state.
// Uses SELECT FOR UPDATE to prevent TOCTOU race conditions in concurrent scenarios.
// For SQLite, relies on serializable transaction isolation (SQLite's default).
func (s *GormStorage) EnqueueUnique(ctx context.Context, job *core.Job, uniqueKey string) error {
	fillEnqueueDefaults(job)
	job.UniqueKey = uniqueKey

	// Retry on serialization/deadlock failures. Under MySQL REPEATABLE READ,
	// many callers racing the same unique key take conflicting gap locks on the
	// FOR UPDATE below and deadlock (error 1213); on retry the winner's row is
	// visible so we return ErrDuplicateJob cleanly instead of surfacing a raw
	// 1213. ErrDuplicateJob is not a serialization failure, so it is returned
	// without further retry. (Same wrapper EnqueueBatch uses — finding 2.5.)
	return s.withSerializationRetry(ctx, func() error {
		// Use transaction with row-level locking to prevent race conditions
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			query := tx.Where("unique_key = ?", uniqueKey).
				Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning})

			// SQLite doesn't support FOR UPDATE, but its serializable transactions
			// provide equivalent protection for single-process scenarios
			query = s.lockForUpdate(query, false)

			var existing core.Job
			err := query.First(&existing).Error

			if err == nil {
				// Found existing job with same unique key
				return core.ErrDuplicateJob
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				// Unexpected error
				return err
			}

			// No existing active job found. The database-level partial unique
			// index is the final backstop for PostgreSQL's absent-row FOR
			// UPDATE gap.
			row, err := s.encodedJobForCreate(job)
			if err != nil {
				return err
			}
			dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
			if result.Error != nil {
				return result.Error
			}
			if uniqueKey != "" && result.RowsAffected == 0 {
				return core.ErrDuplicateJob
			}
			return restoreDQReadyFalse(tx, dqReadyFalseIDs, dqReadyFalseRefs)
		})
	})
}

// Dequeue fetches and locks the next available job.
// Uses FOR UPDATE SKIP LOCKED to prevent multiple workers from selecting the same job.
// For SQLite, uses optimistic locking with atomic update (suitable for dev/testing).
// Dequeue claims the highest-priority eligible job. It is self-healing: if the
// first claim finds nothing, it promotes any pending job that has become
// eligible (run_at passed) but is still flagged dq_ready=false and retries once.
// This keeps dequeue correctness independent of the external ready-promoter loop
// — a storage wrapper that hides the promoter capability, or any window before
// the loop's next tick, can never leave an eligible job permanently invisible.
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	job, err := s.dequeueOnce(ctx, queues, workerID)
	if err != nil || job != nil {
		return job, err
	}
	promoted, perr := s.PromoteReadyJobs(ctx)
	if perr != nil || promoted == 0 {
		return nil, nil
	}
	return s.dequeueOnce(ctx, queues, workerID)
}

func (s *GormStorage) dequeueOnce(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	var job core.Job
	now := time.Now()
	lockDuration := time.Duration(s.lockDuration.Load())
	lockUntil := now.Add(lockDuration)

	// SQLite uses optimistic locking - no row-level locks available
	if s.isSQLite {
		return s.dequeueSQLite(ctx, queues, workerID, now, lockUntil)
	}

	// PostgreSQL/MySQL: Use FOR UPDATE SKIP LOCKED for proper distributed locking
	// Use silent logger for this transaction — "record not found" is the normal
	// idle state and spams logs every poll interval.
	//
	// Lock timing is anchored to the DATABASE clock, not this worker's wall
	// clock: the freshness predicates and the locked_until/started_at writes all
	// evaluate NOW() server-side. This is what makes the lock safe across a
	// fleet — the reaper on another worker compares locked_until against the same
	// DB clock, so clock skew can never cause a live lock to be reclaimed early.
	//
	// Note on snapshot semantics: on Postgres NOW() is transaction_timestamp(),
	// constant for the whole transaction. On MySQL NOW(6) is the per-STATEMENT
	// time, so the SELECT's freshness check and the UPDATE's locked_until read
	// slightly different instants. That is still correct: the UPDATE always runs
	// after the SELECT, so locked_until is anchored to a time >= the freshness
	// check — it can never make a just-taken lock look already expired. Do not
	// add a predicate that assumes the two reads are equal.
	nowExpr := s.nowExpr()
	lockUntilExpr := s.offsetExpr(lockDuration)
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})
	err := silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// FOR UPDATE SKIP LOCKED ensures:
		// 1. The selected row is locked for this transaction
		// 2. Other workers skip locked rows instead of waiting
		// This prevents duplicate job execution in distributed scenarios
		result := s.claimableCandidates(s.lockForUpdate(tx, true), queues, nowExpr).First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		// Re-check if queue was paused between initial check and now (race condition fix)
		var queueState core.QueueState
		if err := tx.First(&queueState, "queue = ?", job.Queue).Error; err == nil {
			if queueState.Paused {
				// Queue was paused after our initial check, skip this job
				job = core.Job{} // Clear to indicate no job found
				return nil
			}
		}

		if err := tx.Model(&core.Job{}).
			Where("id = ?", job.ID).
			Updates(map[string]any{
				"status":       core.StatusRunning,
				"locked_by":    workerID,
				"locked_until": lockUntilExpr,
				"started_at":   nowExpr,
				"attempt":      job.Attempt + 1,
				// Clear any prior run's heartbeat on claim. The stale-lock reaper
				// anchors freshness on COALESCE(last_heartbeat_at, started_at,
				// locked_until); without this reset a re-dequeued job (durable
				// sleep/signal resume, or a retry across a gap > StaleLockAge)
				// keeps the OLD heartbeat as its anchor and is reclaimed while
				// actively running — a double-execution. NULL falls back to the
				// freshly-set started_at (this claim), which is correct.
				"last_heartbeat_at": gorm.Expr("NULL"),
			}).Error; err != nil {
			return err
		}

		// Re-read the row so the returned struct reflects the DB-clock
		// locked_until/started_at the UPDATE assigned via SQL expressions
		// (the in-memory job still holds pre-update values otherwise).
		return tx.First(&job, "id = ?", job.ID).Error
	})

	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, nil
	}
	if err := s.decodeJobPayloads(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// dequeueSQLite uses optimistic locking for SQLite (dev/testing only).
// This is NOT safe for multiple concurrent workers - use PostgreSQL for production.
func (s *GormStorage) dequeueSQLite(ctx context.Context, queues []string, workerID string, now time.Time, lockUntil time.Time) (*core.Job, error) {
	var job core.Job

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Find a candidate job
		result := s.claimableCandidates(tx, queues, now).First(&job)

		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return nil
			}
			return result.Error
		}

		// Re-check if queue was paused between initial check and now (race condition fix)
		var queueState core.QueueState
		if err := tx.First(&queueState, "queue = ?", job.Queue).Error; err == nil {
			if queueState.Paused {
				// Queue was paused after our initial check, skip this job
				job = core.Job{}
				return nil
			}
		}

		// Optimistic lock: Update only if still pending (atomic check-and-set)
		// If another worker grabbed it, RowsAffected will be 0
		updateResult := tx.Model(&core.Job{}).
			Where("id = ?", job.ID).
			Where("status = ?", core.StatusPending). // Must still be pending
			Updates(map[string]any{
				"status":       core.StatusRunning,
				"locked_by":    workerID,
				"locked_until": lockUntil,
				"started_at":   now,
				"attempt":      job.Attempt + 1,
				// Clear the prior run's heartbeat on claim so the stale-lock reaper
				// anchors on this claim's started_at, not a stale heartbeat (CD-01
				// double-execution). See Dequeue.
				"last_heartbeat_at": gorm.Expr("NULL"),
			})

		if updateResult.Error != nil {
			return updateResult.Error
		}

		if updateResult.RowsAffected == 0 {
			// Another worker got it first - reset job so caller sees no job found
			job = core.Job{}
			return nil
		}

		// Update the job struct with the new values
		job.Status = core.StatusRunning
		job.LockedBy = workerID
		job.LockedUntil = &lockUntil
		job.StartedAt = &now
		job.Attempt++

		return nil
	})

	if err != nil {
		return nil, err
	}
	if job.ID == "" {
		return nil, nil
	}
	if err := s.decodeJobPayloads(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// Complete marks a job as successfully completed.
// Validates that the worker owns the job before completing.
//
// The status flip and the opt-in checkpoint GC are wrapped in one transaction so
// the delete commits atomically with success: a crash either rolls back both
// (the job stays running and is replayed) or commits both. Checkpoints are
// deleted only when SetDeleteCheckpointsOnComplete(true) was set; the default
// keeps them for the dashboard. A not-owned Complete deletes nothing.
func (s *GormStorage) Complete(ctx context.Context, jobID core.UUID, workerID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&core.Job{}).
			Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
			Updates(map[string]any{
				"status":       core.StatusCompleted,
				"completed_at": s.nowWriteValue(),
				"locked_by":    "",
				"locked_until": nil,
			})

		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return core.ErrJobNotOwned
		}
		if s.deleteCheckpointsOnComplete.Load() {
			if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// CompleteWithResult marks a job completed and, when it is a sub-job, accounts
// for its fan-out completion in the same ownership-guarded transaction.
func (s *GormStorage) CompleteWithResult(ctx context.Context, jobID core.UUID, workerID string, result []byte) (*core.FanOut, error) {
	updates := map[string]any{
		"status":       core.StatusCompleted,
		"locked_by":    "",
		"locked_until": nil,
	}
	if result != nil {
		encoded, err := s.encodePayload("job result", string(jobID), result)
		if err != nil {
			return nil, err
		}
		updates["result"] = encoded
	}
	// Success path: GC this job's checkpoints in the same tx ONLY when the
	// opt-in is enabled (default off keeps them for the dashboard).
	return s.accountTerminalWithFanOut(ctx, jobID, workerID, updates, "completed_count", s.deleteCheckpointsOnComplete.Load())
}

// Fail marks a job as failed, optionally scheduling a retry.
// Validates that the worker owns the job before failing.
// Error messages are sanitized before storage.
func (s *GormStorage) Fail(ctx context.Context, jobID core.UUID, workerID string, errMsg string, retryAt *time.Time) error {
	// Sanitize error message to prevent sensitive data leakage
	sanitizedErr := security.SanitizeErrorMessage(errMsg)
	// Encrypt the handler error text at rest when a real codec is configured.
	// PII can surface in retryable failures shown in the UI, so encode in both
	// branches. Under the default identity codec this returns plaintext verbatim.
	encErr, err := s.encodeErrorText("job last_error", string(jobID), sanitizedErr)
	if err != nil {
		return err
	}

	now := time.Now()
	updates := map[string]any{
		"last_error":   encErr,
		"locked_by":    "",
		"locked_until": nil,
	}

	if retryAt != nil {
		updates["status"] = core.StatusPending
		updates["run_at"] = retryAt
		// retryAt is non-nil in this branch, so readiness is simply retryAt<=now.
		// Compute it in Go: a bound "? IS NULL" parameter has no inferable type on
		// Postgres (SQLSTATE 42P18), and a map Update writes an explicit bool fine.
		updates["dq_ready"] = !retryAt.After(now)
	} else {
		updates["status"] = core.StatusFailed
		updates["completed_at"] = s.nowWriteValue()
		updates["dead_lettered_at"] = now
		// Label stays plaintext (fixed, non-PII); only the error suffix is
		// encrypted, preserving the attempt>=max_retries CASE in SQL.
		updates["dead_letter_reason"] = deadLetterReasonExpr(encErr)
	}

	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// FailTerminalWithResult marks a job terminally failed and, when it is a
// sub-job, accounts for its fan-out failure in the same ownership-guarded
// transaction. Retryable failures must continue to use Fail.
func (s *GormStorage) FailTerminalWithResult(ctx context.Context, jobID core.UUID, workerID, errMsg string) (*core.FanOut, error) {
	sanitizedErr := security.SanitizeErrorMessage(errMsg)
	// Encrypt the error text at rest when a real codec is configured. The
	// encoded form flows into both last_error and the dead_letter_reason suffix
	// (the label stays plaintext via deadLetterReasonExpr). Identity codec → no-op.
	encErr, err := s.encodeErrorText("job last_error", string(jobID), sanitizedErr)
	if err != nil {
		return nil, err
	}
	updates := map[string]any{
		"last_error":        encErr,
		"status":            core.StatusFailed,
		"locked_by":         "",
		"locked_until":      nil,
		deadLetterReasonKey: encErr,
	}
	// Terminal failure: NEVER delete checkpoints. They are retained for
	// debugging/inspection and are still swept by terminal-job retention GC when
	// a FailedAfter window is set.
	return s.accountTerminalWithFanOut(ctx, jobID, workerID, updates, "failed_count", false)
}

func deadLetterReason(lastError string) string {
	if lastError == "" {
		return "max retries exhausted"
	}
	return "max retries exhausted: " + lastError
}

func nonRetryableDeadLetterReason(lastError string) string {
	if lastError == "" {
		return "non-retryable failure"
	}
	return "non-retryable failure: " + lastError
}

func deadLetterReasonExpr(lastError string) clause.Expr {
	// attempt is incremented on dequeue, so attempt >= max_retries means retries were exhausted; this label also wins when NoRetry lands on the final attempt.
	return gorm.Expr(
		"CASE WHEN attempt >= max_retries THEN ? ELSE ? END",
		deadLetterReason(lastError),
		nonRetryableDeadLetterReason(lastError),
	)
}

// accountTerminalWithFanOut performs the ownership-guarded terminal job UPDATE
// described by jobUpdates and, when the job is a sub-job, resolves the fan-out
// from live child-job terminal counts after that transaction commits. Returns
// the fan-out row when the job has a fan-out, or nil. Returns core.ErrJobNotOwned
// if the worker no longer owns the running job.
//
// When deleteCheckpoints is true (success path with the opt-in enabled), this
// job's checkpoints are deleted in the SAME transaction, after the ownership
// guard confirms RowsAffected>0 and BEFORE the fan-out lookup, so even
// non-fan-out jobs are GC'd. The delete commits or rolls back with the status
// flip — a crash can never leave a completed job with orphaned checkpoints. The
// failure path passes false (retry replay needs them).
//
// Note: this builds a per-attempt copy of jobUpdates because the transaction
// body may be retried after serialization failures.
func (s *GormStorage) accountTerminalWithFanOut(ctx context.Context, jobID core.UUID, workerID string, jobUpdates map[string]any, _ string, deleteCheckpoints bool) (*core.FanOut, error) {
	var fanOut core.FanOut
	var fanOutID core.UUID

	err := s.withSerializationRetry(ctx, func() error {
		fanOutID = ""

		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			now := time.Now()
			updates := make(map[string]any, len(jobUpdates)+2)
			for key, value := range jobUpdates {
				if key == deadLetterReasonKey {
					continue
				}
				updates[key] = value
			}
			updates["completed_at"] = s.nowWriteValue()
			if lastError, ok := jobUpdates[deadLetterReasonKey].(string); ok {
				updates["dead_lettered_at"] = now
				updates["dead_letter_reason"] = deadLetterReasonExpr(lastError)
			}

			update := tx.Model(&core.Job{}).
				Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
				Updates(updates)
			if update.Error != nil {
				return update.Error
			}
			if update.RowsAffected == 0 {
				return core.ErrJobNotOwned
			}

			// Opt-in checkpoint GC on the success terminal write, committed in
			// this same tx. Placed before the fan-out early-return so non-fan-out
			// jobs are GC'd too. A lost-ownership write never reaches here.
			if deleteCheckpoints {
				if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
					return err
				}
			}

			// Release any fleet concurrency slots this job holds, atomically with
			// the terminal write. The worker also releases them best-effort after
			// processJob returns, but a crash in the window between this commit and
			// that deferred release would otherwise orphan a live slot for a
			// finished job until its TTL (~45m), silently shrinking the cap. Doing
			// it in the terminal tx makes slot release crash-safe. Keyed by job_id
			// so it covers every configured cap; the permanent per-slot-name
			// sentinel (job_id = '') is never matched.
			if err := tx.Where("job_id = ?", jobID).Delete(&core.ConcurrencySlot{}).Error; err != nil {
				return err
			}

			var job core.Job
			if err := tx.Select("fan_out_id").First(&job, "id = ?", jobID).Error; err != nil {
				return err
			}
			if job.FanOutID == nil || *job.FanOutID == "" {
				return nil
			}
			fanOutID = *job.FanOutID
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if fanOutID == "" {
		return nil, nil
	}

	err = s.withSerializationRetry(ctx, func() error {
		fanOut = core.FanOut{}
		db := s.db.WithContext(ctx)
		if err := db.First(&fanOut, "id = ?", fanOutID).Error; err != nil {
			return err
		}
		completed, failed, cancelled, err := liveFanOutCounts(db, fanOutID)
		if err != nil {
			return err
		}
		overlayFanOutCounts(&fanOut, completed, failed, cancelled)

		// Count after the child terminal transaction commits so the snapshot can
		// see concurrently-committed siblings. The pending-guarded update remains
		// the exactly-once serializer; non-terminal children perform no fan_outs
		// write at all.
		if done, termStatus := fanOut.TerminalStatus(); done {
			res := db.Model(&core.FanOut{}).
				Where("id = ? AND status = ?", fanOutID, core.FanOutPending).
				Updates(map[string]any{
					"status":          termStatus,
					"completed_count": completed,
					"failed_count":    failed,
					"cancelled_count": cancelled,
					"updated_at":      time.Now(),
				})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 1 {
				fanOut.Status = termStatus
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// SaveCheckpoint stores a checkpoint for a durable call.
// If a checkpoint with the same (job_id, call_index, call_type) already exists,
// the latest result and error fields are updated in place (upsert).
func (s *GormStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = core.NewID()
	}
	row, err := s.encodedCheckpointForSave(cp)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "job_id"}, {Name: "call_index"}, {Name: "call_type"}},
			DoUpdates: clause.AssignmentColumns([]string{"result", "error", "error_kind", "error_cause", "error_delay_nanos"}),
		}).
		Create(row).Error
}

// GetCheckpoints retrieves all checkpoints for a job.
func (s *GormStorage) GetCheckpoints(ctx context.Context, jobID core.UUID) ([]core.Checkpoint, error) {
	var checkpoints []core.Checkpoint
	err := s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Order("call_index ASC").
		Find(&checkpoints).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeCheckpointPayloads(checkpoints); err != nil {
		return nil, err
	}
	return checkpoints, nil
}

// DeleteCheckpoints removes all checkpoints for a job.
func (s *GormStorage) DeleteCheckpoints(ctx context.Context, jobID core.UUID) error {
	return s.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Delete(&core.Checkpoint{}).Error
}

// GetDueJobs returns jobs ready to run.
func (s *GormStorage) GetDueJobs(ctx context.Context, queues []string, limit int) ([]*core.Job, error) {
	var jobList []*core.Job

	// Match Dequeue's clock source so "due" means the same thing to every caller.
	var nowVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
	} else {
		nowVal = time.Now()
	}

	// Get paused queues to filter out
	pausedQueues, err := s.GetPausedQueues(ctx)
	if err != nil {
		return nil, err
	}

	// Filter out paused queues
	activeQueues := make([]string, 0, len(queues))
	pausedSet := make(map[string]bool)
	for _, q := range pausedQueues {
		pausedSet[q] = true
	}
	for _, q := range queues {
		if !pausedSet[q] {
			activeQueues = append(activeQueues, q)
		}
	}

	if len(activeQueues) == 0 {
		return nil, nil // All queues are paused
	}

	err = s.db.WithContext(ctx).
		Where("queue IN ?", activeQueues).
		Where("status = ?", core.StatusPending).
		Where(s.dequeueEligibleExpr()+" <= ?", nowVal).
		Where("(locked_until IS NULL OR locked_until < ?)", nowVal).
		Order("priority DESC, " + s.dequeueEligibleExpr() + " ASC").
		Limit(limit).
		Find(&jobList).Error

	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobList); err != nil {
		return nil, err
	}
	return jobList, nil
}

// ClaimScheduledFire atomically advances a schedule's last claimed fire time.
// Exactly one caller can claim a given (name, fireTime) boundary; later
// boundaries can claim again, while equal or earlier boundaries are refused.
func (s *GormStorage) ClaimScheduledFire(ctx context.Context, name string, fireTime time.Time) (bool, error) {
	return s.ClaimScheduledFireTx(ctx, s.db, name, fireTime)
}

// ClaimScheduledFireTx is ClaimScheduledFire performed within a caller-owned
// transaction, so the boundary claim can be committed ATOMICALLY with the
// enqueue of the fired job (and rolled back together on failure). Without this,
// a claim that durably advanced last_fire_at followed by a failed enqueue would
// silently drop a due scheduled run (teardown g8). See Queue.EnqueueScheduledFire.
func (s *GormStorage) ClaimScheduledFireTx(ctx context.Context, tx *gorm.DB, name string, fireTime time.Time) (bool, error) {
	if err := tx.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
		Create(&core.ScheduledFire{Name: name, LastFireAt: time.Unix(0, 0).UTC()}).Error; err != nil {
		return false, err
	}

	result := tx.WithContext(ctx).
		Model(&core.ScheduledFire{}).
		Where("name = ? AND last_fire_at < ?", name, fireTime).
		Updates(map[string]any{
			"last_fire_at":  fireTime,
			"last_fired_at": fireTime,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

// SeedScheduledFire establishes a shared fire-boundary anchor for a fresh
// schedule and returns the effective anchor. It inserts a row with
// last_fire_at = anchor only if none exists (insert-if-absent via ON CONFLICT
// DO NOTHING), so the first worker in the fleet to seed wins and every other
// worker reads back the same value. Unlike ClaimScheduledFire it never advances
// an existing boundary and never represents a fire — it only records the
// starting cursor so all workers compute identical fire times and the atomic
// ClaimScheduledFire then elects exactly one firing per boundary.
func (s *GormStorage) SeedScheduledFire(ctx context.Context, name string, anchor time.Time) (time.Time, error) {
	if err := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
		Create(&core.ScheduledFire{Name: name, LastFireAt: anchor}).Error; err != nil {
		return time.Time{}, err
	}
	var fire core.ScheduledFire
	if err := s.db.WithContext(ctx).First(&fire, "name = ?", name).Error; err != nil {
		return time.Time{}, err
	}
	return fire.LastFireAt, nil
}

// GetScheduledFireTime returns the latest persisted fire boundary for a schedule.
func (s *GormStorage) GetScheduledFireTime(ctx context.Context, name string) (time.Time, bool, error) {
	var fire core.ScheduledFire
	err := s.db.WithContext(ctx).First(&fire, "name = ?", name).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return fire.LastFireAt, true, nil
}

// GetScheduledFireTimes returns the latest real fire time for every schedule.
func (s *GormStorage) GetScheduledFireTimes(ctx context.Context) (map[string]time.Time, error) {
	var fires []core.ScheduledFire
	if err := s.db.WithContext(ctx).Where("last_fired_at IS NOT NULL").Find(&fires).Error; err != nil {
		return nil, err
	}
	times := make(map[string]time.Time, len(fires))
	for _, fire := range fires {
		if fire.LastFiredAt != nil {
			times[fire.Name] = *fire.LastFiredAt
		}
	}
	return times, nil
}

// Heartbeat extends the lock on a running job.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) Heartbeat(ctx context.Context, jobID core.UUID, workerID string) error {
	updates := map[string]any{}
	if s.useDBClock() {
		// Extend the lock on the DB clock so it stays comparable with the
		// reaper's stale cutoff regardless of this worker's wall clock.
		lockDuration := time.Duration(s.lockDuration.Load())
		updates["locked_until"] = s.offsetExpr(lockDuration)
		updates["last_heartbeat_at"] = s.nowExpr()
	} else {
		now := time.Now()
		lockDuration := time.Duration(s.lockDuration.Load())
		updates["locked_until"] = now.Add(lockDuration)
		updates["last_heartbeat_at"] = now
	}
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// Release returns an owned, still-running job to pending so it can be
// immediately dequeued by another worker after a local dispatch is abandoned.
func (s *GormStorage) Release(ctx context.Context, jobID core.UUID, workerID string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(map[string]any{
			"status":       core.StatusPending,
			"locked_by":    "",
			"locked_until": nil,
			"started_at":   nil,
			"attempt":      gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
			"dq_ready":     s.dqReadyExpr(now),
			"updated_at":   now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// FindOrphanedJobs returns the IDs from the input slice whose DB row
// indicates the caller no longer owns the job. See core.Storage.FindOrphanedJobs
// for the protocol — this is the implementation.
//
// A job is considered orphaned to workerID when it is NOT in a self-suspended
// state AND any of these is true:
//   - locked_by != workerID  (another worker took the lock)
//   - locked_by IS NULL      (lock was released, no new owner)
//   - status IN ('cancelled','completed','failed') AND still locked_by a real
//     worker (locked_by <> ”) — a peer terminated it (e.g. fan-out cancel)
//     while we held the lock. A terminal row whose locked_by was reset to ”
//     is this worker's OWN completion and is NOT an orphan (it has no live
//     handler to stop; flagging it fired a spurious reclaim — teardown g9).
//
// The status NOT IN ('waiting','paused') guard is essential: a parent that
// calls FanOut/Call from inside its handler suspends ITSELF via MarkWaiting,
// which sets status='waiting' and clears locked_by to the empty string. During
// the window between MarkWaiting and the handler returning the WaitingError, the
// parent is still present in the worker's runningJobs map, yet its row has
// locked_by="" — which would otherwise match "locked_by != workerID" and make
// the ownership audit cancel the worker's own in-flight handler. That spurious
// cancel turns the WaitingError into a context.Canceled failure, reschedules the
// job, and replays the whole handler from its checkpoints. A waiting/paused job
// has, by construction, no running handler to cancel, so it is never an orphan
// the audit should act on. (A paused job is in the same shape: locked_by is
// already cleared, and a paused row is not reclaimable while paused.)
//
// Returns an empty slice (not nil) when no jobs are orphaned, so callers
// can len()-test without nil-checking.
func (s *GormStorage) FindOrphanedJobs(ctx context.Context, jobIDs []core.UUID, workerID string) ([]core.UUID, error) {
	if len(jobIDs) == 0 {
		return []core.UUID{}, nil
	}
	var orphaned []core.UUID
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id IN ?", jobIDs).
		Where("status NOT IN ?", []core.JobStatus{core.StatusWaiting, core.StatusPaused}).
		Where(
			s.db.Where("locked_by IS NULL").
				Or("locked_by != ?", workerID).
				Or("status IN ?", core.TerminalJobStatuses),
		).
		// Do NOT flag a job THIS worker terminated itself. Complete/Fail clear
		// locked_by to '' and set a terminal status, which trips the
		// locked_by != workerID branch above and would fire a spurious
		// JobReclaimed/OnJobReclaimed for our own completion during the TOCTOU
		// window before the deferred runningJobs unregister (teardown g9). A
		// TERMINAL row is a genuine reclaim only if it is still locked by a real
		// worker (locked_by <> ''): a peer that cancels our running sub-job via a
		// fan-out failure (CancelSubJobs) sets status=cancelled but leaves
		// locked_by = this worker, so that legitimate stop-the-handler case is
		// preserved, while our own clean completion (locked_by reset to '') is
		// excluded. Non-terminal orphans (reclaimed/released, still active) are
		// unaffected by this clause.
		Where(
			s.db.Where("status NOT IN ?", core.TerminalJobStatuses).
				Or("locked_by <> ?", ""),
		).
		Pluck("id", &orphaned).Error
	if err != nil {
		return nil, err
	}
	if orphaned == nil {
		orphaned = []core.UUID{}
	}
	return orphaned, nil
}

// ReleaseStaleLocks releases locks on jobs that haven't had a heartbeat.
// Returns the IDs of the jobs whose locks were released so the caller can
// cancel any local in-flight handlers — see core.Storage.ReleaseStaleLocks
// for the rationale.
//
// The cutoff is compared against COALESCE(last_heartbeat_at, started_at,
// locked_until): the last *contact* the owning worker made, not its stacked
// lease expiry. last_heartbeat_at is the live anchor (refreshed every heartbeat);
// started_at is the fallback for a job that was dequeued but has not yet sent its
// first heartbeat; locked_until is the last-resort fallback for directly-inserted
// or pre-existing rows that have neither column populated. This makes reclaim
// latency ~staleDuration ("time since last contact") rather than the old
// ~lockDuration+staleDuration, which the lease-stacking Dequeue/Heartbeat pushes
// inflated. last_heartbeat_at was written for exactly this purpose but was
// previously never read by the reaper.
func (s *GormStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) ([]core.UUID, error) {
	// The cutoff is computed on the DB clock for multi-worker backends so it is
	// comparable with the DB-clock last_heartbeat_at/started_at/locked_until
	// written by Dequeue/Heartbeat. Comparing a client-computed cutoff against a
	// DB-written timestamp is exactly the skew bug this avoids: a reaper whose
	// wall clock ran ahead would otherwise reclaim a lock that is still live on
	// the owning worker.
	var now, cutoff any
	if s.useDBClock() {
		now = s.nowExpr()
		cutoff = s.offsetExpr(-staleDuration)
	} else {
		nowTime := time.Now()
		now = nowTime
		cutoff = nowTime.Add(-staleDuration)
	}

	// Two-step: first capture the IDs of stale jobs, then update them in
	// the same transaction so the reader and the writer agree. RETURNING
	// would let us do this in one statement but isn't portable across all
	// GORM drivers, so we do it the long way.
	var released []core.UUID
	err := s.withSerializationRetry(ctx, func() error {
		released = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// ORDER BY matches idx_jobs_stale_lock's freshness expression, so
			// the planner can use it while the per-tick cap bounds the update.
			q := tx.Model(&core.Job{}).
				Where("status = ?", core.StatusRunning).
				Where("COALESCE(last_heartbeat_at, started_at, locked_until) < ?", cutoff).
				Order("COALESCE(last_heartbeat_at, started_at, locked_until) ASC").
				Limit(maxResumeBatch)
			q = s.lockForUpdate(q, true)
			if err := q.Pluck("id", &released).Error; err != nil {
				return err
			}
			if len(released) == 0 {
				return nil
			}
			// Defense-in-depth: the FOR UPDATE SKIP LOCKED above already froze the
			// plucked set, so these rows cannot change under us — but re-asserting
			// the staleness predicate keeps the UPDATE self-guarding (it can never
			// revert a row that is no longer running/stale even if a future change
			// drops the row lock).
			return tx.Model(&core.Job{}).
				Where("id IN ?", released).
				Where("status = ?", core.StatusRunning).
				Where("COALESCE(last_heartbeat_at, started_at, locked_until) < ?", cutoff).
				Updates(map[string]any{
					"status":       core.StatusPending,
					"locked_by":    "",
					"locked_until": nil,
					"dq_ready":     s.dqReadyExpr(now),
				}).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return released, nil
}

// GetJob retrieves a job by ID.
func (s *GormStorage) GetJob(ctx context.Context, jobID core.UUID) (*core.Job, error) {
	var job core.Job
	err := s.db.WithContext(ctx).First(&job, "id = ?", jobID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobPayloads(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// GetJobsByStatus retrieves jobs by status.
func (s *GormStorage) GetJobsByStatus(ctx context.Context, status core.JobStatus, limit int) ([]*core.Job, error) {
	var jobList []*core.Job
	err := s.db.WithContext(ctx).
		Where("status = ?", status).
		Limit(limit).
		Find(&jobList).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobList); err != nil {
		return nil, err
	}
	return jobList, nil
}

// --- Fan-out operations ---

// CreateFanOut creates a new fan-out record.
func (s *GormStorage) CreateFanOut(ctx context.Context, fanOut *core.FanOut) error {
	if fanOut.ID == "" {
		fanOut.ID = core.NewID()
	}
	return s.db.WithContext(ctx).Create(fanOut).Error
}

func liveFanOutCounts(db *gorm.DB, fanOutID core.UUID) (completed, failed, cancelled int, err error) {
	type countRow struct {
		Status string
		Count  int64
	}
	var rows []countRow
	err = db.Model(&core.Job{}).
		Select("status, COUNT(*) AS count").
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusCancelled}).
		Group("status").
		Find(&rows).Error
	if err != nil {
		return 0, 0, 0, err
	}
	for _, row := range rows {
		switch core.JobStatus(row.Status) {
		case core.StatusCompleted:
			completed += int(row.Count)
		case core.StatusFailed:
			failed += int(row.Count)
		case core.StatusCancelled:
			cancelled += int(row.Count)
		}
	}
	return completed, failed, cancelled, nil
}

func overlayFanOutCounts(fanOut *core.FanOut, completed, failed, cancelled int) {
	fanOut.CompletedCount = completed
	fanOut.FailedCount = failed
	fanOut.CancelledCount = cancelled
}

func overlayLiveFanOutCounts(db *gorm.DB, fanOut *core.FanOut) error {
	completed, failed, cancelled, err := liveFanOutCounts(db, fanOut.ID)
	if err != nil {
		return err
	}
	overlayFanOutCounts(fanOut, completed, failed, cancelled)
	return nil
}

func overlayLiveFanOutCountsBatch(db *gorm.DB, fanOuts []*core.FanOut) error {
	if len(fanOuts) == 0 {
		return nil
	}
	ids := make([]core.UUID, 0, len(fanOuts))
	byID := make(map[core.UUID]*core.FanOut, len(fanOuts))
	for _, fanOut := range fanOuts {
		if fanOut == nil {
			continue
		}
		ids = append(ids, fanOut.ID)
		byID[fanOut.ID] = fanOut
		overlayFanOutCounts(fanOut, 0, 0, 0)
	}
	if len(ids) == 0 {
		return nil
	}

	type countRow struct {
		FanOutID core.UUID
		Status   string
		Count    int64
	}
	var rows []countRow
	if err := db.Model(&core.Job{}).
		Select("fan_out_id, status, COUNT(*) AS count").
		Where("fan_out_id IN ? AND status IN ?", ids, []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusCancelled}).
		Group("fan_out_id, status").
		Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		fanOut := byID[row.FanOutID]
		if fanOut == nil {
			continue
		}
		switch core.JobStatus(row.Status) {
		case core.StatusCompleted:
			fanOut.CompletedCount += int(row.Count)
		case core.StatusFailed:
			fanOut.FailedCount += int(row.Count)
		case core.StatusCancelled:
			fanOut.CancelledCount += int(row.Count)
		}
	}
	return nil
}

// GetFanOut retrieves a fan-out by ID.
func (s *GormStorage) GetFanOut(ctx context.Context, fanOutID core.UUID) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).First(&fanOut, "id = ?", fanOutID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCounts(s.db.WithContext(ctx), &fanOut); err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// IncrementFanOutCompleted returns the fan-out with live completed count.
func (s *GormStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID core.UUID) (*core.FanOut, error) {
	return s.getFanOutWithLiveCounts(ctx, fanOutID)
}

// IncrementFanOutFailed returns the fan-out with live failed count.
func (s *GormStorage) IncrementFanOutFailed(ctx context.Context, fanOutID core.UUID) (*core.FanOut, error) {
	return s.getFanOutWithLiveCounts(ctx, fanOutID)
}

// IncrementFanOutCancelled returns the fan-out with live cancelled count.
func (s *GormStorage) IncrementFanOutCancelled(ctx context.Context, fanOutID core.UUID) (*core.FanOut, error) {
	return s.getFanOutWithLiveCounts(ctx, fanOutID)
}

func (s *GormStorage) getFanOutWithLiveCounts(ctx context.Context, fanOutID core.UUID) (*core.FanOut, error) {
	var fanOut core.FanOut
	db := s.db.WithContext(ctx)
	if err := db.First(&fanOut, "id = ?", fanOutID).Error; err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCounts(db, &fanOut); err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// UpdateFanOutStatus atomically updates the status of a fan-out from pending.
// Returns (true, nil) if the status was updated, (false, nil) if already completed
// by another worker, or (false, error) on database error.
// This prevents race conditions where multiple workers try to complete the same fan-out.
func (s *GormStorage) UpdateFanOutStatus(ctx context.Context, fanOutID core.UUID, status core.FanOutStatus) (bool, error) {
	var updated bool
	err := s.withSerializationRetry(ctx, func() error {
		completed, failed, cancelled, err := liveFanOutCounts(s.db.WithContext(ctx), fanOutID)
		if err != nil {
			return err
		}
		result := s.db.WithContext(ctx).
			Model(&core.FanOut{}).
			Where("id = ? AND status = ?", fanOutID, core.FanOutPending).
			Updates(map[string]any{
				"status":          status,
				"completed_count": completed,
				"failed_count":    failed,
				"cancelled_count": cancelled,
				"updated_at":      time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		// RowsAffected == 0 means another worker already completed this fan-out.
		updated = result.RowsAffected > 0
		return nil
	})
	return updated, err
}

// GetFanOutsByParent retrieves all fan-outs for a parent job.
func (s *GormStorage) GetFanOutsByParent(ctx context.Context, parentJobID core.UUID) ([]*core.FanOut, error) {
	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id = ?", parentJobID).
		Order("created_at ASC").
		Find(&fanOuts).Error
	if err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCountsBatch(s.db.WithContext(ctx), fanOuts); err != nil {
		return nil, err
	}
	return fanOuts, nil
}

// --- Sub-job operations ---

// EnqueueBatch inserts multiple jobs in a single operation.
// Jobs carrying a UniqueKey are deduplicated against pending/running/completed
// rows so a parent workflow that crashes mid fan-out can replay safely. The
// dedup lookup and insert run inside a single transaction with row-level
// locking on non-SQLite backends, mirroring EnqueueUnique, so concurrent
// replays cannot produce duplicate sub-jobs.
//
// Under MySQL's default REPEATABLE READ, concurrent callers that insert the
// same UniqueKey set can deadlock on gap locks; we retry the transaction a
// bounded number of times when the driver reports a serialization failure
// or deadlock (MySQL error 1213/1205, Postgres SQLSTATE 40001/40P01).
func (s *GormStorage) EnqueueBatch(ctx context.Context, jobs []*core.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	// Fill in defaults up front so callers always see the resolved IDs/statuses
	// regardless of which rows survive the dedup pass.
	for _, job := range jobs {
		fillEnqueueDefaults(job)
	}

	return s.withSerializationRetry(ctx, func() error {
		return s.enqueueBatchTx(ctx, jobs)
	})
}

// enqueueBatchTx is the core body of EnqueueBatch, extracted so it can be
// retried on transient serialization failures without re-running the
// default-filling pass above.
func (s *GormStorage) enqueueBatchTx(ctx context.Context, jobs []*core.Job) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.enqueueBatchWithDB(tx, jobs)
	})
}

// GetSubJobs retrieves all sub-jobs for a fan-out.
func (s *GormStorage) GetSubJobs(ctx context.Context, fanOutID core.UUID) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ?", fanOutID).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetSubJobResults retrieves completed/failed sub-jobs for result collection.
func (s *GormStorage) GetSubJobResults(ctx context.Context, fanOutID core.UUID) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusCompleted, core.StatusFailed}).
		Order("fan_out_index ASC").
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// CancelSubJobs cancels pending/running sub-jobs for a fan-out and returns their
// IDs so the caller can cancel any local in-flight handlers — see
// core.Storage.CancelSubJobs for the rationale.
func (s *GormStorage) CancelSubJobs(ctx context.Context, fanOutID core.UUID) ([]core.UUID, error) {
	var cancelled []core.UUID
	err := s.withSerializationRetry(ctx, func() error {
		cancelled = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Capture the IDs of sub-jobs we're about to cancel. Done before
			// the UPDATE so we get the in-flight set, not just rows that
			// happen to remain pending after the update.
			query := tx.Model(&core.Job{}).
				Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusPending, core.StatusRunning}).
				Order("fan_out_index ASC")
			query = s.lockForUpdate(query, false)
			if err := query.Pluck("id", &cancelled).Error; err != nil {
				return err
			}
			if len(cancelled) == 0 {
				return nil
			}

			now := time.Now()
			result := tx.Model(&core.Job{}).
				Where("id IN ?", cancelled).
				Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning}).
				Updates(map[string]any{
					"status":       core.StatusCancelled,
					"completed_at": s.nowWriteValue(),
					"updated_at":   now,
				})
			if result.Error != nil {
				return result.Error
			}

			// Release any fleet concurrency slots held by the cancelled sub-jobs,
			// atomically with the cancel write, so a worker that dies before its
			// deferred release cannot orphan a live slot for a now-terminal job.
			if err := tx.Where("job_id IN ?", cancelled).Delete(&core.ConcurrencySlot{}).Error; err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return cancelled, nil
}

// CancelSubJob cancels a single sub-job and returns its fan-out with live counts.
// Returns the updated FanOut record so the caller can check for completion.
// Returns (nil, nil) if the job has no fan-out ID (not a sub-job).
func (s *GormStorage) CancelSubJob(ctx context.Context, jobID core.UUID) (*core.FanOut, error) {
	var fanOut core.FanOut
	var hasFanOut bool

	err := s.withSerializationRetry(ctx, func() error {
		hasFanOut = false
		fanOut = core.FanOut{}
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Get the job to find its fan-out ID
			var job core.Job
			if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
				return err
			}

			// Not a sub-job
			if job.FanOutID == nil || *job.FanOutID == "" {
				return nil
			}

			// Only cancel if in a cancellable state
			if job.Status == core.StatusCompleted || job.Status == core.StatusFailed || job.Status == core.StatusCancelled {
				return nil
			}

			// Cancel the job
			now := time.Now()
			result := tx.Model(&core.Job{}).
				Where("id = ? AND status NOT IN ?", jobID, []core.JobStatus{core.StatusCompleted, core.StatusFailed, core.StatusCancelled}).
				Updates(map[string]any{
					"status":       core.StatusCancelled,
					"completed_at": s.nowWriteValue(),
					"updated_at":   now,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				// Another cancel/terminal transition won after our initial read.
				// Treat this like the already-terminal no-op path above.
				return nil
			}

			// Release any fleet concurrency slots held by this now-cancelled
			// (terminal) sub-job, atomically with the cancel write — mirrors the
			// CancelSubJobs (plural) and terminal-completion paths so a worker that
			// dies before its deferred release cannot orphan a live slot.
			if err := tx.Where("job_id = ?", jobID).Delete(&core.ConcurrencySlot{}).Error; err != nil {
				return err
			}

			if err := tx.First(&fanOut, "id = ?", *job.FanOutID).Error; err != nil {
				return err
			}
			completed, failed, cancelledCount, err := liveFanOutCounts(tx, *job.FanOutID)
			if err != nil {
				return err
			}
			overlayFanOutCounts(&fanOut, completed, failed, cancelledCount)
			hasFanOut = true
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	if !hasFanOut {
		return nil, nil
	}
	return &fanOut, nil
}

// --- Waiting job operations ---

// MarkWaiting suspends a job to waiting status.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) MarkWaiting(ctx context.Context, jobID core.UUID, workerID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(map[string]any{
			"status":       core.StatusWaiting,
			"locked_by":    "",
			"locked_until": nil,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// ResumeJob resumes a waiting (or paused) job to pending status.
// Decrements the attempt counter so that the next Dequeue (which always
// increments attempt) doesn't count the fan-out suspend/resume cycle as
// a real retry. Without this, each fan-out round eats one retry attempt
// and the job exhausts max_retries before completing.
// Returns (true, nil) if resumed, (false, nil) if job was not in a resumable status.
func (s *GormStorage) ResumeJob(ctx context.Context, jobID core.UUID) (bool, error) {
	// Accept both waiting and paused status — paused jobs should also be
	// resumable when their fan-out completes. run_at is deliberately left
	// untouched so a delayed job that was paused before its run_at and then
	// resumed still honors its original schedule. (Signal-waiting jobs use
	// ResumeSignalWaitingJob instead, which clears the timeout wake deadline.)
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status IN (?, ?)", jobID, core.StatusWaiting, core.StatusPaused).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"attempt":    gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
			"dq_ready":   s.dqReadyExpr(now),
			"updated_at": now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// ResumeSignalWaitingJob resumes a job that is waiting on a signal. Unlike the
// general ResumeJob it matches StatusWaiting ONLY (never paused — a producer
// must not be able to un-pause an operator-paused job), and it clears run_at:
// WaitForSignalTimeout parks a future run_at as its wake deadline, so when a
// signal arrives early the resume must not leave a future run_at that would
// block Dequeue until that deadline. Returns whether a row was resumed.
func (s *GormStorage) ResumeSignalWaitingJob(ctx context.Context, jobID core.UUID) (bool, error) {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusWaiting).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"attempt":    gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
			"run_at":     nil,
			"dq_ready":   true,
			"updated_at": now,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// Requeue resets a terminally failed or cancelled job back to pending so it can
// run again from scratch. Failed jobs are this library's dead-letter set — there
// is no separate DLQ table; a job that exhausts its retries stays queryable via
// its dead_lettered_at/dead_letter_reason metadata and Requeue is how an
// operator replays one.
//
// The attempt counter, error, DLQ metadata, lock, and result fields are reset,
// run_at is cleared (immediately eligible), AND the job's checkpoints are
// deleted so the run starts fresh. Clearing checkpoints is deliberate: the usual
// reason to requeue is a code or dependency fix, which can change a workflow's
// Call sequence — resuming into checkpoints recorded by the old run would skip
// the wrong steps and, under Strict determinism, falsely trip the
// unconsumed-checkpoint guard. Handlers must be idempotent regardless (the
// execution contract is at-least-once), so a full replay is safe.
//
// A fan-out sub-job cannot be requeued directly (it would double-count its
// parent); ErrCannotRequeueSubJob is returned in that case. Requeuing a fan-out
// PARENT deletes its entire fan-out subtree — every descendant fan-out record,
// sub-job, and their checkpoints, at any nesting depth — so the replay
// re-dispatches a fresh tree instead of orphaning the old one.
//
// Returns true if a job was requeued, false if it was not found or not in a
// requeuable (failed/cancelled) state.
func (s *GormStorage) Requeue(ctx context.Context, jobID core.UUID) (bool, error) {
	var requeued bool
	err := s.withSerializationRetry(ctx, func() error {
		requeued = false
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var job core.Job
			if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil // not found → not requeued
				}
				return err
			}
			if job.Status != core.StatusFailed && job.Status != core.StatusCancelled {
				return nil // not in a requeuable state
			}
			if job.FanOutID != nil && *job.FanOutID != "" {
				// Requeuing a sub-job directly would re-run it and increment the
				// parent's fan-out counter a second time. Replay via the parent.
				return core.ErrCannotRequeueSubJob
			}

			result := tx.Model(&core.Job{}).
				Where("id = ?", jobID).
				Updates(map[string]any{
					"status":             core.StatusPending,
					"attempt":            0,
					"last_error":         "",
					"dead_lettered_at":   nil,
					"dead_letter_reason": "",
					"result":             nil,
					"locked_by":          "",
					"locked_until":       nil,
					"started_at":         nil,
					"completed_at":       nil,
					"run_at":             nil,
					"dq_ready":           true,
					"updated_at":         time.Now(),
				})
			if result.Error != nil {
				return result.Error
			}
			requeued = true

			// Replay from scratch: drop checkpoints recorded by the prior run.
			if err := tx.Where("job_id = ?", jobID).Delete(&core.Checkpoint{}).Error; err != nil {
				return err
			}

			// If this job is a fan-out parent, delete its entire fan-out
			// subtree (records + sub-jobs + their checkpoints, at every depth)
			// so the replay's FanOut() re-dispatches a fresh tree rather than
			// leaving orphaned rows and stale counters — including under nested
			// workflows where a sub-job is itself a fan-out parent.
			return s.deleteFanOutSubtree(tx, jobID)
		})
	})
	if err != nil {
		return false, err
	}
	return requeued, nil
}

// deleteFanOutSubtree deletes the entire fan-out tree rooted at rootJobID: every
// fan-out it spawned directly or transitively, all of those sub-jobs, and their
// checkpoints/signals. The root job row itself is NOT touched. Used by Requeue so
// replaying a parent re-dispatches a fresh tree instead of orphaning the old one
// at any depth (a sub-job may itself be a fan-out parent).
//
// Two phases. Phase 1 is a read-only level-by-level descent that collects every
// fan-out id and sub-job id in the tree WITHOUT deleting anything, so the
// parent_job_id/fan_out_id chains stay intact for the whole walk. Phase 2 then
// deletes the collected rows. This ordering is required because migration v14
// added fk_fanouts_parent (fan_outs.parent_job_id -> jobs.id ON DELETE CASCADE):
// deleting a level's sub-jobs cascade-removes the NEXT level's fan_outs rows, so
// a delete-as-you-descend walk would lose the frontier and orphan grandchildren
// (there is no FK on jobs.fan_out_id, so the cascade stops at fan_outs and the
// grandchild sub-jobs would be stranded). The depth cap bounds cyclically corrupt
// data; IN-lists stay bounded to one level's width during the walk.
func (s *GormStorage) deleteFanOutSubtree(tx *gorm.DB, rootJobID core.UUID) error {
	var allFanOutIDs, allSubJobIDs []core.UUID
	frontier := []core.UUID{rootJobID}
	for depth := 0; len(frontier) > 0 && depth < maxFanOutTreeDepth; depth++ {
		var fanOutIDs []core.UUID
		if err := tx.Model(&core.FanOut{}).
			Where("parent_job_id IN ?", frontier).
			Pluck("id", &fanOutIDs).Error; err != nil {
			return err
		}
		if len(fanOutIDs) == 0 {
			break // reached the leaves
		}

		var subJobIDs []core.UUID
		if err := tx.Model(&core.Job{}).
			Where("fan_out_id IN ?", fanOutIDs).
			Pluck("id", &subJobIDs).Error; err != nil {
			return err
		}

		allFanOutIDs = append(allFanOutIDs, fanOutIDs...)
		allSubJobIDs = append(allSubJobIDs, subJobIDs...)

		// Descend into the sub-jobs just collected; their child fan-outs are
		// still present (nothing deleted yet) and findable by parent_job_id.
		frontier = subJobIDs
	}

	if len(allSubJobIDs) > 0 {
		// Delete dependents explicitly so the behavior holds on SQLite too (no FK
		// there). On PG/MySQL deleting the sub-jobs also cascade-removes their
		// checkpoints/signals/fan_outs, making these deletes idempotent.
		if err := tx.Where("job_id IN ?", allSubJobIDs).Delete(&core.Checkpoint{}).Error; err != nil {
			return err
		}
		if err := tx.Where("job_id IN ?", allSubJobIDs).Delete(&core.Signal{}).Error; err != nil {
			return err
		}
		if err := tx.Where("id IN ?", allSubJobIDs).Delete(&core.Job{}).Error; err != nil {
			return err
		}
	}
	if len(allFanOutIDs) > 0 {
		// Root-level fan_outs (parent_job_id = the untouched root) are not reached
		// by the sub-job cascade, so delete the collected fan_outs explicitly.
		if err := tx.Where("id IN ?", allFanOutIDs).Delete(&core.FanOut{}).Error; err != nil {
			return err
		}
	}
	return nil
}

// GetWaitingJobsToResume finds waiting jobs whose fan-out is complete.
//
// Subtle: a parent that does sequential fan-outs (e.g. a workflow that
// dispatches fan_out #1, suspends, resumes, dispatches fan_out #2, suspends
// again) has multiple rows in fan_outs. The old query joined any
// completed/failed fan-out without checking whether a later fan-out was
// still pending — so once fan_out #1 finished, the polling fallback would
// keep returning the parent every tick and ResumeJob would spuriously
// pull the parent off its #2 wait, replay the workflow, and re-suspend.
// The NOT EXISTS guard restricts the result to parents that have at least
// one terminated fan-out AND no pending fan-outs. DISTINCT keeps the
// result one-row-per-parent when there are multiple terminated fan-outs.
func (s *GormStorage) GetWaitingJobsToResume(ctx context.Context) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).Raw(`
		SELECT DISTINCT j.* FROM jobs j
		INNER JOIN fan_outs f ON j.id = f.parent_job_id
		WHERE j.status = ?
		AND f.status IN (?, ?)
		AND NOT EXISTS (
			SELECT 1 FROM fan_outs f2
			WHERE f2.parent_job_id = j.id
			AND f2.status = ?
		)
		LIMIT ?
	`, core.StatusWaiting, core.FanOutCompleted, core.FanOutFailed, core.FanOutPending, maxResumeBatch).Scan(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetStalledFanOutParents finds waiting parents whose pending fan-out never
// finished persisting its sub-jobs. The caller supplies olderThan so the query
// stays portable across SQLite, PostgreSQL, and MySQL.
//
// The LIMIT is intentionally unordered: recovery is idempotent and self-draining
// (a qualifying parent stays waiting+pending until resumed, so any row not picked
// this tick is picked on a later one), so a per-tick cap bounds work without
// needing a stable sort. maxResumeBatch matches GetWaitingJobsToResume.
func (s *GormStorage) GetStalledFanOutParents(ctx context.Context, olderThan time.Time) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).Raw(`
		SELECT DISTINCT j.* FROM jobs j
		INNER JOIN fan_outs f ON j.id = f.parent_job_id
		WHERE j.status = ?
		AND f.status = ?
		AND f.created_at < ?
		AND (
			SELECT count(*) FROM jobs s
			WHERE s.fan_out_id = f.id
		) < f.total_count
		LIMIT ?
	`, core.StatusWaiting, core.FanOutPending, olderThan, maxResumeBatch).Scan(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// GetCompletablePendingFanOuts finds fan-outs that are still status='pending'
// but whose live child-job terminal counts already satisfy a terminal-condition
// superset while their parent job sits in StatusWaiting.
//
// The child-count predicate (terminal>=total) OR failed>0 OR cancelled>0 is a
// portable SUPERSET of every FanOut.TerminalStatus() terminal condition
// (collect_all/threshold resolve at accounted>=total; the default fail_fast and
// threshold early-fail resolve at failed>0/cancelled>0).
// The final terminal decision is made in Go by the caller via TerminalStatus(),
// so a non-terminal row that slips through is a harmless no-op. Restricting to
// parent status='waiting' avoids touching fan-outs whose parent is still
// running (that parent self-resolves via its own checkFanOutCompletion).
//
// olderThan keeps the query portable across SQLite/PostgreSQL/MySQL and avoids
// racing the live completion path on freshly-terminal fan-outs. The LIMIT
// matches GetStalledFanOutParents: recovery is idempotent and self-draining.
func (s *GormStorage) GetCompletablePendingFanOuts(ctx context.Context, olderThan time.Time) ([]*core.FanOut, error) {
	var fanOuts []*core.FanOut
	// The terminal-child counts are computed by correlated subqueries scoped to
	// each candidate fan-out (f.id), AFTER the outer WHERE has narrowed to the
	// small pending+waiting+old working set. This makes the child scan O(children
	// of the few candidate fan-outs) — an index lookup on (fan_out_id, status) per
	// candidate — instead of aggregating terminal children of every fan-out ever.
	// Correlated scalar subqueries are portable across SQLite/PostgreSQL/MySQL.
	// Equivalence with the prior derived-table form: terminal_count>=total_count
	// OR failed_count>0 OR cancelled_count>0  ⟺  count(completed|failed|cancelled)
	// >= total_count OR count(failed|cancelled) > 0.
	err := s.db.WithContext(ctx).Raw(`
		SELECT f.* FROM fan_outs f
		INNER JOIN jobs j ON j.id = f.parent_job_id
		WHERE f.status = ?
		AND j.status = ?
		AND f.created_at < ?
		AND (
			(SELECT COUNT(*) FROM jobs c WHERE c.fan_out_id = f.id AND c.status IN (?, ?, ?)) >= f.total_count
			OR (SELECT COUNT(*) FROM jobs c WHERE c.fan_out_id = f.id AND c.status IN (?, ?)) > 0
		)
		LIMIT ?
	`,
		core.FanOutPending, core.StatusWaiting, olderThan,
		core.StatusCompleted, core.StatusFailed, core.StatusCancelled,
		core.StatusFailed, core.StatusCancelled,
		maxResumeBatch,
	).Scan(&fanOuts).Error
	if err != nil {
		return nil, err
	}
	if err := overlayLiveFanOutCountsBatch(s.db.WithContext(ctx), fanOuts); err != nil {
		return nil, err
	}
	return fanOuts, nil
}

// CleanAbandonedFanOuts deletes pending fan_outs that were abandoned mid-creation
// (crash between CreateFanOut and EnqueueBatch): the parent is already TERMINAL,
// the fan_out has ZERO sub-jobs, and it is older than olderThan. Such a row can
// never complete and blocks the terminal parent's retention forever (the
// retention guard refuses to delete a parent with any pending fan_out). Restricting
// to a TERMINAL parent makes deletion safe - a terminal parent never replays, so
// removing its orphan fan_out cannot break fan-out replay; and ZERO sub-jobs means
// the delete strands nothing. Bounded by maxResumeBatch per call. Returns the count
// deleted.
func (s *GormStorage) CleanAbandonedFanOuts(ctx context.Context, olderThan time.Time) (int64, error) {
	var deleted int64
	err := s.withSerializationRetry(ctx, func() error {
		deleted = 0
		silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})
		return silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var ids []core.UUID
			if err := tx.Model(&core.FanOut{}).
				Select("fan_outs.id").
				Joins("INNER JOIN jobs j ON j.id = fan_outs.parent_job_id").
				Where("fan_outs.status = ?", core.FanOutPending).
				Where("j.status IN ?", core.TerminalJobStatuses).
				Where("fan_outs.created_at < ?", olderThan).
				Where("NOT EXISTS (SELECT 1 FROM jobs c WHERE c.fan_out_id = fan_outs.id)").
				Limit(maxResumeBatch).
				Pluck("fan_outs.id", &ids).Error; err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			result := tx.Where("id IN ?", ids).
				Where("status = ?", core.FanOutPending).
				Delete(&core.FanOut{})
			if result.Error != nil {
				return result.Error
			}
			deleted = result.RowsAffected
			return nil
		})
	})
	return deleted, err
}

// SaveJobResult stores the serialized result for a job.
func (s *GormStorage) SaveJobResult(ctx context.Context, jobID core.UUID, workerID string, result []byte) error {
	encoded, err := s.encodePayload("job result", string(jobID), result)
	if err != nil {
		return err
	}
	update := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Update("result", encoded)
	if update.Error != nil {
		return update.Error
	}
	if update.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// --- Job pause operations ---

// PauseJob pauses a job, preventing it from being picked up.
// It preserves the legacy storage behavior: running jobs are cancelled.
func (s *GormStorage) PauseJob(ctx context.Context, jobID core.UUID) error {
	return s.PauseJobWithMode(ctx, jobID, core.PauseModeAggressive)
}

// PauseJobWithMode pauses or cancels a job according to mode.
// Graceful pause only transitions pending/waiting jobs to paused. Aggressive
// mode preserves PauseJob's legacy behavior and cancels running jobs.
func (s *GormStorage) PauseJobWithMode(ctx context.Context, jobID core.UUID, mode core.PauseMode) error {
	if mode == core.PauseModeGraceful {
		return s.pauseJobGraceful(ctx, jobID)
	}
	return s.pauseJobAggressive(ctx, jobID)
}

func (s *GormStorage) pauseJobGraceful(ctx context.Context, jobID core.UUID) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			result := tx.Model(&core.Job{}).
				Where("id = ? AND status IN ?", jobID, []core.JobStatus{core.StatusPending, core.StatusWaiting}).
				Updates(map[string]any{
					"status":          core.StatusPaused,
					"previous_status": gorm.Expr("status"),
					"updated_at":      time.Now(),
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				return nil
			}

			var job core.Job
			if err := tx.Select("status").First(&job, "id = ?", jobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("job not found: %s", jobID)
				}
				return err
			}
			if job.Status == core.StatusPaused {
				return core.ErrJobAlreadyPaused
			}
			return core.ErrCannotPauseStatus
		})
	})
}

func (s *GormStorage) pauseJobAggressive(ctx context.Context, jobID core.UUID) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			now := time.Now()
			pauseResult := tx.Model(&core.Job{}).
				Where("id = ? AND status IN ?", jobID, []core.JobStatus{core.StatusPending, core.StatusWaiting}).
				Updates(map[string]any{
					"status":          core.StatusPaused,
					"previous_status": gorm.Expr("status"),
					"updated_at":      now,
				})
			if pauseResult.Error != nil {
				return pauseResult.Error
			}
			if pauseResult.RowsAffected > 0 {
				return nil
			}

			cancelResult := tx.Model(&core.Job{}).
				Where("id = ? AND status = ?", jobID, core.StatusRunning).
				Updates(map[string]any{
					"status":       core.StatusCancelled,
					"completed_at": s.nowWriteValue(),
					"updated_at":   now,
					// non-PII constant; intentionally stored plaintext
					"last_error":   "cancelled by user",
					"locked_by":    "",
					"locked_until": nil,
				})
			if cancelResult.Error != nil {
				return cancelResult.Error
			}
			if cancelResult.RowsAffected > 0 {
				// The job was running and is now cancelled (terminal). Release any
				// fleet concurrency slots it held, in this tx — the worker's
				// deferred release won't run for a job cancelled out from under it,
				// so without this the slot would orphan until its TTL (~45m),
				// silently shrinking the cap. Mirrors CancelSubJobs / terminal
				// completion (keyed by job_id; the per-slot-name sentinel job_id=''
				// is never matched).
				if err := tx.Where("job_id = ?", jobID).Delete(&core.ConcurrencySlot{}).Error; err != nil {
					return err
				}
				return nil
			}

			var job core.Job
			if err := tx.Select("status").First(&job, "id = ?", jobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("job not found: %s", jobID)
				}
				return err
			}
			if job.Status == core.StatusPaused {
				return core.ErrJobAlreadyPaused
			}
			return core.ErrCannotPauseStatus
		})
	})
}

// UnpauseJob resumes a paused job back to its previous status.
func (s *GormStorage) UnpauseJob(ctx context.Context, jobID core.UUID) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var job core.Job
			if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("job not found: %s", jobID)
				}
				return err
			}

			if job.Status != core.StatusPaused {
				return core.ErrJobNotPaused
			}

			// Restore to previous status, default to pending if not set
			restoreStatus := job.PreviousStatus
			if restoreStatus == "" {
				restoreStatus = core.StatusPending
			}

			now := time.Now()
			updates := map[string]any{
				"status":          restoreStatus,
				"previous_status": "",
				"updated_at":      now,
			}
			if restoreStatus == core.StatusPending {
				updates["dq_ready"] = s.dqReadyExpr(now)
			}

			return tx.Model(&core.Job{}).
				Where("id = ?", jobID).
				Updates(updates).Error
		})
	})
}

// GetPausedJobs returns all paused jobs in a queue.
func (s *GormStorage) GetPausedJobs(ctx context.Context, queue string) ([]*core.Job, error) {
	var jobs []*core.Job
	err := s.db.WithContext(ctx).
		Where("queue = ? AND status = ?", queue, core.StatusPaused).
		Order("created_at ASC").
		Find(&jobs).Error
	if err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// IsJobPaused checks if a job is paused.
func (s *GormStorage) IsJobPaused(ctx context.Context, jobID core.UUID) (bool, error) {
	var job core.Job
	err := s.db.WithContext(ctx).
		Select("status").
		First(&job, "id = ?", jobID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return job.Status == core.StatusPaused, nil
}

// --- Queue pause operations ---

// PauseQueue marks a queue as paused.
func (s *GormStorage) PauseQueue(ctx context.Context, queue string) error {
	now := time.Now()

	var existing core.QueueState
	err := s.db.WithContext(ctx).First(&existing, "queue = ?", queue).Error

	if err == nil {
		if existing.Paused {
			return core.ErrQueueAlreadyPaused
		}
		return s.db.WithContext(ctx).
			Model(&core.QueueState{}).
			Where("queue = ?", queue).
			Updates(map[string]any{
				"paused":    true,
				"paused_at": now,
			}).Error
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	return s.db.WithContext(ctx).Create(&core.QueueState{
		Queue:    queue,
		Paused:   true,
		PausedAt: &now,
	}).Error
}

// UnpauseQueue unpauses a queue.
func (s *GormStorage) UnpauseQueue(ctx context.Context, queue string) error {
	result := s.db.WithContext(ctx).
		Model(&core.QueueState{}).
		Where("queue = ? AND paused = ?", queue, true).
		Updates(map[string]any{
			"paused":    false,
			"paused_at": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrQueueNotPaused
	}
	return nil
}

// GetPausedQueues returns all paused queue names.
func (s *GormStorage) GetPausedQueues(ctx context.Context) ([]string, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).
		Where("paused = ?", true).
		Find(&states).Error
	if err != nil {
		return nil, err
	}

	queues := make([]string, len(states))
	for i, state := range states {
		queues[i] = state.Queue
	}
	return queues, nil
}

// IsQueuePaused checks if a queue is paused.
func (s *GormStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var state core.QueueState
	err := s.db.WithContext(ctx).First(&state, "queue = ?", queue).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return state.Paused, nil
}

// RefreshQueueStates returns a map of queue names to their paused state.
func (s *GormStorage) RefreshQueueStates(ctx context.Context) (map[string]bool, error) {
	var states []core.QueueState
	err := s.db.WithContext(ctx).Find(&states).Error
	if err != nil {
		return nil, err
	}

	result := make(map[string]bool)
	for _, state := range states {
		result[state.Queue] = state.Paused
	}
	return result, nil
}
