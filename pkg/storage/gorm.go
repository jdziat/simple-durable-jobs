// Package storage provides storage implementations for the jobs package.
package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

const (
	defaultLockDuration = 45 * time.Minute
	maxResumeBatch      = 100
	// maxFanOutTreeDepth bounds the fan-out subtree walk in Requeue. Real
	// workflows nest only a few levels; this is insurance against pathologically
	// deep (or cyclically corrupt) data, not a normal limit. Per-level deletion
	// already makes a cycle impossible to loop on (visited rows are gone), so
	// this is belt-and-suspenders.
	maxFanOutTreeDepth = 256
)

// GormStorageOption configures a GormStorage.
type GormStorageOption func(*GormStorage)

// WithStorageLockDuration sets the duration for which a job lock is held when
// dequeued or extended by a heartbeat. The default is 45 minutes.
func WithStorageLockDuration(d time.Duration) GormStorageOption {
	return func(s *GormStorage) {
		s.lockDuration = d
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

// GormStorage implements Storage using GORM.
type GormStorage struct {
	db           *gorm.DB
	isSQLite     bool
	lockDuration time.Duration
	codec        core.PayloadCodec
}

// NewGormStorage creates a new GORM-backed storage. For SQLite under concurrent
// workers, callers MUST open the database with a concurrency-safe DSN, e.g.
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
func NewGormStorage(db *gorm.DB, opts ...GormStorageOption) *GormStorage {
	// Detect SQLite by checking the dialect name
	isSQLite := false
	if db != nil {
		dialector := db.Name()
		isSQLite = strings.Contains(strings.ToLower(dialector), "sqlite")
	}
	s := &GormStorage{db: db, isSQLite: isSQLite, lockDuration: defaultLockDuration, codec: core.IdentityCodec}
	if s.isSQLite {
		// NOTE: a one-shot PRAGMA only affects the single pooled connection it
		// runs on; connections the pool opens later default to busy_timeout=0.
		// The DSN parameters above are the reliable, pool-wide mechanism.
		_ = db.Exec("PRAGMA busy_timeout = 5000").Error
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SetLockDuration updates the lock duration after construction.
// This is used by the worker when WithLockDuration is passed as a worker option.
func (s *GormStorage) SetLockDuration(d time.Duration) {
	s.lockDuration = d
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
		if err := db.AutoMigrate(
			&core.Job{},
			&core.Checkpoint{},
			&core.FanOut{},
			&core.QueueState{},
			&core.ScheduledFire{},
			&core.Lease{},
			&core.Signal{},
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

		return s.applyPendingMigrations(ctx, db)
	})
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
		return db.Create(row).Error
	}
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrDuplicateJob
	}
	return nil
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
			if !s.isSQLite {
				query = query.Clauses(clause.Locking{Strength: "UPDATE"})
			}

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
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
			if result.Error != nil {
				return result.Error
			}
			if uniqueKey != "" && result.RowsAffected == 0 {
				return core.ErrDuplicateJob
			}
			return nil
		})
	})
}

// Dequeue fetches and locks the next available job.
// Uses FOR UPDATE SKIP LOCKED to prevent multiple workers from selecting the same job.
// For SQLite, uses optimistic locking with atomic update (suitable for dev/testing).
func (s *GormStorage) Dequeue(ctx context.Context, queues []string, workerID string) (*core.Job, error) {
	var job core.Job
	now := time.Now()
	lockUntil := now.Add(s.lockDuration)

	// Get paused queues
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

	// SQLite uses optimistic locking - no row-level locks available
	if s.isSQLite {
		return s.dequeueSQLite(ctx, activeQueues, workerID, now, lockUntil)
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
	lockUntilExpr := s.offsetExpr(s.lockDuration)
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})
	err = silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// FOR UPDATE SKIP LOCKED ensures:
		// 1. The selected row is locked for this transaction
		// 2. Other workers skip locked rows instead of waiting
		// This prevents duplicate job execution in distributed scenarios
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("queue IN ?", activeQueues).
			Where("status = ?", core.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", nowExpr).
			Where("(locked_until IS NULL OR locked_until < ?)", nowExpr).
			Order("priority DESC, created_at ASC").
			First(&job)

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
		result := tx.
			Where("queue IN ?", queues).
			Where("status = ?", core.StatusPending).
			Where("(run_at IS NULL OR run_at <= ?)", now).
			Where("(locked_until IS NULL OR locked_until < ?)", now).
			Order("priority DESC, created_at ASC").
			First(&job)

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
func (s *GormStorage) Complete(ctx context.Context, jobID string, workerID string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(map[string]any{
			"status":       core.StatusCompleted,
			"completed_at": now,
			"locked_by":    "",
			"locked_until": nil,
		})

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrJobNotOwned
	}
	return nil
}

// CompleteWithResult marks a job completed and, when it is a sub-job, accounts
// for its fan-out completion in the same ownership-guarded transaction.
func (s *GormStorage) CompleteWithResult(ctx context.Context, jobID, workerID string, result []byte) (*core.FanOut, error) {
	updates := map[string]any{
		"status":       core.StatusCompleted,
		"locked_by":    "",
		"locked_until": nil,
	}
	if result != nil {
		encoded, err := s.encodePayload("job result", jobID, result)
		if err != nil {
			return nil, err
		}
		updates["result"] = encoded
	}
	return s.accountTerminalWithFanOut(ctx, jobID, workerID, updates, "completed_count")
}

// Fail marks a job as failed, optionally scheduling a retry.
// Validates that the worker owns the job before failing.
// Error messages are sanitized before storage.
func (s *GormStorage) Fail(ctx context.Context, jobID string, workerID string, errMsg string, retryAt *time.Time) error {
	// Sanitize error message to prevent sensitive data leakage
	sanitizedErr := security.SanitizeErrorMessage(errMsg)

	updates := map[string]any{
		"last_error":   sanitizedErr,
		"locked_by":    "",
		"locked_until": nil,
	}

	if retryAt != nil {
		updates["status"] = core.StatusPending
		updates["run_at"] = retryAt
	} else {
		updates["status"] = core.StatusFailed
		now := time.Now()
		updates["completed_at"] = now
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
func (s *GormStorage) FailTerminalWithResult(ctx context.Context, jobID, workerID, errMsg string) (*core.FanOut, error) {
	sanitizedErr := security.SanitizeErrorMessage(errMsg)
	updates := map[string]any{
		"last_error":   sanitizedErr,
		"status":       core.StatusFailed,
		"locked_by":    "",
		"locked_until": nil,
	}
	return s.accountTerminalWithFanOut(ctx, jobID, workerID, updates, "failed_count")
}

// fanOutCounterStmt returns the constant UPDATE statement that increments the
// named fan-out counter, or ok=false for any unrecognized column. This is an
// allow-list: the only valid columns are the two terminal counters, and each
// maps to a fixed string literal so no caller value is ever interpolated into
// SQL. The placeholders bind (updated_at, fan_out_id, status).
func fanOutCounterStmt(col string) (stmt string, ok bool) {
	switch col {
	case "completed_count":
		return "UPDATE fan_outs SET completed_count = completed_count + 1, updated_at = ? WHERE id = ? AND status = ?", true
	case "failed_count":
		return "UPDATE fan_outs SET failed_count = failed_count + 1, updated_at = ? WHERE id = ? AND status = ?", true
	default:
		return "", false
	}
}

// accountTerminalWithFanOut performs the ownership-guarded terminal job UPDATE
// described by jobUpdates and, when the job is a sub-job of a still-pending
// fan-out, atomically increments fanOutCounterCol in the same transaction
// (liveness-guarded on status='pending'). Returns the fan-out row when the job
// has a fan-out, or nil. Increments nothing and returns core.ErrJobNotOwned if the
// worker no longer owns the running job.
//
// Note: this mutates jobUpdates by setting "completed_at"; callers must pass a
// freshly-allocated map (not a shared or package-level one).
func (s *GormStorage) accountTerminalWithFanOut(ctx context.Context, jobID, workerID string, jobUpdates map[string]any, fanOutCounterCol string) (*core.FanOut, error) {
	var fanOut core.FanOut
	var hasFanOut bool

	err := s.withSerializationRetry(ctx, func() error {
		fanOut = core.FanOut{}
		hasFanOut = false

		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			now := time.Now()
			jobUpdates["completed_at"] = now

			update := tx.Model(&core.Job{}).
				Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
				Updates(jobUpdates)
			if update.Error != nil {
				return update.Error
			}
			if update.RowsAffected == 0 {
				return core.ErrJobNotOwned
			}

			var job core.Job
			if err := tx.Select("fan_out_id").First(&job, "id = ?", jobID).Error; err != nil {
				return err
			}
			if job.FanOutID == nil || *job.FanOutID == "" {
				return nil
			}

			// Map the caller's intent to a constant SQL statement via an
			// allow-list. The column name is never interpolated from a
			// variable — even though callers only pass hardcoded constants
			// today, building the statement with fmt.Sprintf made the call
			// site one careless edit away from SQL injection. The switch
			// makes that impossible and is checked at compile time.
			counterSQL, ok := fanOutCounterStmt(fanOutCounterCol)
			if !ok {
				return fmt.Errorf("storage: invalid fan-out counter column %q", fanOutCounterCol)
			}
			if err := tx.Exec(
				counterSQL,
				now, *job.FanOutID, core.FanOutPending,
			).Error; err != nil {
				return err
			}
			if err := tx.First(&fanOut, "id = ?", *job.FanOutID).Error; err != nil {
				return err
			}
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

// SaveCheckpoint stores a checkpoint for a durable call.
// If a checkpoint with the same (job_id, call_index, call_type) already exists,
// the latest result and error fields are updated in place (upsert).
func (s *GormStorage) SaveCheckpoint(ctx context.Context, cp *core.Checkpoint) error {
	if cp.ID == "" {
		cp.ID = uuid.New().String()
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
func (s *GormStorage) GetCheckpoints(ctx context.Context, jobID string) ([]core.Checkpoint, error) {
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
func (s *GormStorage) DeleteCheckpoints(ctx context.Context, jobID string) error {
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
		Where("(run_at IS NULL OR run_at <= ?)", nowVal).
		Where("(locked_until IS NULL OR locked_until < ?)", nowVal).
		Order("priority DESC, created_at ASC").
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
	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
		Create(&core.ScheduledFire{Name: name, LastFireAt: time.Unix(0, 0).UTC()}).Error
	if err != nil {
		return false, err
	}

	result := s.db.WithContext(ctx).
		Model(&core.ScheduledFire{}).
		Where("name = ? AND last_fire_at < ?", name, fireTime).
		Update("last_fire_at", fireTime)
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

// Heartbeat extends the lock on a running job.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) Heartbeat(ctx context.Context, jobID string, workerID string) error {
	updates := map[string]any{}
	if s.useDBClock() {
		// Extend the lock on the DB clock so it stays comparable with the
		// reaper's stale cutoff regardless of this worker's wall clock.
		updates["locked_until"] = s.offsetExpr(s.lockDuration)
		updates["last_heartbeat_at"] = s.nowExpr()
	} else {
		now := time.Now()
		updates["locked_until"] = now.Add(s.lockDuration)
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
func (s *GormStorage) Release(ctx context.Context, jobID, workerID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ? AND status = ?", jobID, workerID, core.StatusRunning).
		Updates(map[string]any{
			"status":       core.StatusPending,
			"locked_by":    "",
			"locked_until": nil,
			"started_at":   nil,
			"attempt":      gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
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

// FindOrphanedJobs returns the IDs from the input slice whose DB row
// indicates the caller no longer owns the job. See core.Storage.FindOrphanedJobs
// for the protocol — this is the implementation.
//
// A job is considered orphaned to workerID when it is NOT in a self-suspended
// state AND any of these is true:
//   - locked_by != workerID  (another worker took the lock)
//   - locked_by IS NULL      (lock was released, no new owner)
//   - status IN ('cancelled','completed','failed')  (job is done by some path)
//
// The status NOT IN ('waiting','paused') guard is essential: a parent that
// calls FanOut/Call from inside its handler suspends ITSELF via SuspendJob,
// which sets status='waiting' and clears locked_by to the empty string. During
// the window between SuspendJob and the handler returning the WaitingError, the
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
func (s *GormStorage) FindOrphanedJobs(ctx context.Context, jobIDs []string, workerID string) ([]string, error) {
	if len(jobIDs) == 0 {
		return []string{}, nil
	}
	var orphaned []string
	err := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id IN ?", jobIDs).
		Where("status NOT IN ?", []core.JobStatus{core.StatusWaiting, core.StatusPaused}).
		Where(
			s.db.Where("locked_by IS NULL").
				Or("locked_by != ?", workerID).
				Or("status IN ?", []core.JobStatus{core.StatusCancelled, core.StatusCompleted, core.StatusFailed}),
		).
		Pluck("id", &orphaned).Error
	if err != nil {
		return nil, err
	}
	if orphaned == nil {
		orphaned = []string{}
	}
	return orphaned, nil
}

// ReleaseStaleLocks releases locks on jobs that haven't had a heartbeat.
// Returns the IDs of the jobs whose locks were released so the caller can
// cancel any local in-flight handlers — see core.Storage.ReleaseStaleLocks
// for the rationale.
func (s *GormStorage) ReleaseStaleLocks(ctx context.Context, staleDuration time.Duration) ([]string, error) {
	// The cutoff is computed on the DB clock for multi-worker backends so it is
	// comparable with the DB-clock locked_until written by Dequeue/Heartbeat.
	// Comparing a client-computed cutoff against a DB-written lock is exactly the
	// skew bug this avoids: a reaper whose wall clock ran ahead would otherwise
	// reclaim a lock that is still live on the owning worker.
	var cutoff any
	if s.useDBClock() {
		cutoff = s.offsetExpr(-staleDuration)
	} else {
		cutoff = time.Now().Add(-staleDuration)
	}

	// Two-step: first capture the IDs of stale jobs, then update them in
	// the same transaction so the reader and the writer agree. RETURNING
	// would let us do this in one statement but isn't portable across all
	// GORM drivers, so we do it the long way.
	var released []string
	err := s.withSerializationRetry(ctx, func() error {
		released = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Unordered LIMIT: the reaper is idempotent and self-draining (a
			// stale lock stays running+expired until released), so a per-tick
			// cap bounds the IN-list update without needing a stable sort.
			if err := tx.Model(&core.Job{}).
				Where("status = ?", core.StatusRunning).
				Where("locked_until < ?", cutoff).
				Limit(maxResumeBatch).
				Pluck("id", &released).Error; err != nil {
				return err
			}
			if len(released) == 0 {
				return nil
			}
			return tx.Model(&core.Job{}).
				Where("id IN ?", released).
				Updates(map[string]any{
					"status":       core.StatusPending,
					"locked_by":    "",
					"locked_until": nil,
				}).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return released, nil
}

// GetJob retrieves a job by ID.
func (s *GormStorage) GetJob(ctx context.Context, jobID string) (*core.Job, error) {
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
		fanOut.ID = uuid.New().String()
	}
	return s.db.WithContext(ctx).Create(fanOut).Error
}

// GetFanOut retrieves a fan-out by ID.
func (s *GormStorage) GetFanOut(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.db.WithContext(ctx).First(&fanOut, "id = ?", fanOutID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &fanOut, err
}

// IncrementFanOutCompleted atomically increments the completed count.
func (s *GormStorage) IncrementFanOutCompleted(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(
				"UPDATE fan_outs SET completed_count = completed_count + 1, updated_at = ? WHERE id = ?",
				time.Now(), fanOutID,
			).Error; err != nil {
				return err
			}
			return tx.First(&fanOut, "id = ?", fanOutID).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// IncrementFanOutFailed atomically increments the failed count.
func (s *GormStorage) IncrementFanOutFailed(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(
				"UPDATE fan_outs SET failed_count = failed_count + 1, updated_at = ? WHERE id = ?",
				time.Now(), fanOutID,
			).Error; err != nil {
				return err
			}
			return tx.First(&fanOut, "id = ?", fanOutID).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// IncrementFanOutCancelled atomically increments the cancelled count.
func (s *GormStorage) IncrementFanOutCancelled(ctx context.Context, fanOutID string) (*core.FanOut, error) {
	var fanOut core.FanOut
	err := s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(
				"UPDATE fan_outs SET cancelled_count = cancelled_count + 1, updated_at = ? WHERE id = ?",
				time.Now(), fanOutID,
			).Error; err != nil {
				return err
			}
			return tx.First(&fanOut, "id = ?", fanOutID).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return &fanOut, nil
}

// UpdateFanOutStatus atomically updates the status of a fan-out from pending.
// Returns (true, nil) if the status was updated, (false, nil) if already completed
// by another worker, or (false, error) on database error.
// This prevents race conditions where multiple workers try to complete the same fan-out.
func (s *GormStorage) UpdateFanOutStatus(ctx context.Context, fanOutID string, status core.FanOutStatus) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&core.FanOut{}).
		Where("id = ? AND status = ?", fanOutID, core.FanOutPending).
		Update("status", status)
	if result.Error != nil {
		return false, result.Error
	}
	// RowsAffected == 0 means another worker already completed this fan-out
	return result.RowsAffected > 0, nil
}

// GetFanOutsByParent retrieves all fan-outs for a parent job.
func (s *GormStorage) GetFanOutsByParent(ctx context.Context, parentJobID string) ([]*core.FanOut, error) {
	var fanOuts []*core.FanOut
	err := s.db.WithContext(ctx).
		Where("parent_job_id = ?", parentJobID).
		Order("created_at ASC").
		Find(&fanOuts).Error
	return fanOuts, err
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
func (s *GormStorage) GetSubJobs(ctx context.Context, fanOutID string) ([]*core.Job, error) {
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
func (s *GormStorage) GetSubJobResults(ctx context.Context, fanOutID string) ([]*core.Job, error) {
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

// CancelSubJobs cancels pending/running sub-jobs for a fan-out.
// Updates the fan-out's cancelled_count to reflect the number of cancelled
// jobs and returns the IDs of the cancelled sub-jobs so the caller can
// cancel any local in-flight handlers — see core.Storage.CancelSubJobs
// for the rationale.
func (s *GormStorage) CancelSubJobs(ctx context.Context, fanOutID string) ([]string, error) {
	var cancelled []string
	err := s.withSerializationRetry(ctx, func() error {
		cancelled = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Capture the IDs of sub-jobs we're about to cancel. Done before
			// the UPDATE so we get the in-flight set, not just rows that
			// happen to remain pending after the update.
			query := tx.Model(&core.Job{}).
				Where("fan_out_id = ? AND status IN ?", fanOutID, []core.JobStatus{core.StatusPending, core.StatusRunning}).
				Order("fan_out_index ASC")
			if !s.isSQLite {
				query = query.Clauses(clause.Locking{Strength: "UPDATE"})
			}
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
					"completed_at": now,
					"updated_at":   now,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return nil
			}

			// Update the fan-out's cancelled count to match.
			return tx.Exec(
				"UPDATE fan_outs SET cancelled_count = cancelled_count + ?, updated_at = ? WHERE id = ?",
				result.RowsAffected, now, fanOutID,
			).Error
		})
	})
	if err != nil {
		return nil, err
	}
	return cancelled, nil
}

// CancelSubJob cancels a single sub-job and updates its fan-out's cancelled count.
// Returns the updated FanOut record so the caller can check for completion.
// Returns (nil, nil) if the job has no fan-out ID (not a sub-job).
func (s *GormStorage) CancelSubJob(ctx context.Context, jobID string) (*core.FanOut, error) {
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
					"completed_at": now,
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

			// Increment cancelled count on the fan-out
			if err := tx.Exec(
				"UPDATE fan_outs SET cancelled_count = cancelled_count + 1, updated_at = ? WHERE id = ?",
				now, *job.FanOutID,
			).Error; err != nil {
				return err
			}

			if err := tx.First(&fanOut, "id = ?", *job.FanOutID).Error; err != nil {
				return err
			}
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

// SuspendJob suspends a job to waiting status.
// Returns ErrJobNotOwned if the job is no longer owned by this worker.
func (s *GormStorage) SuspendJob(ctx context.Context, jobID string, workerID string) error {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND locked_by = ?", jobID, workerID).
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
func (s *GormStorage) ResumeJob(ctx context.Context, jobID string) (bool, error) {
	// Accept both waiting and paused status — paused jobs should also be
	// resumable when their fan-out completes. run_at is deliberately left
	// untouched so a delayed job that was paused before its run_at and then
	// resumed still honors its original schedule. (Signal-waiting jobs use
	// ResumeSignalWaitingJob instead, which clears the timeout wake deadline.)
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status IN (?, ?)", jobID, core.StatusWaiting, core.StatusPaused).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"attempt":    gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
			"updated_at": time.Now(),
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
func (s *GormStorage) ResumeSignalWaitingJob(ctx context.Context, jobID string) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&core.Job{}).
		Where("id = ? AND status = ?", jobID, core.StatusWaiting).
		Updates(map[string]any{
			"status":     core.StatusPending,
			"attempt":    gorm.Expr("CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END"),
			"run_at":     nil,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// Requeue resets a terminally failed or cancelled job back to pending so it can
// run again from scratch. Failed jobs are this library's dead-letter set — there
// is no separate DLQ table; a job that exhausts its retries (or is cancelled)
// stays queryable via GetJobsByStatus(StatusFailed) and Requeue is how an
// operator replays one.
//
// The attempt counter, error, lock, and result fields are reset, run_at is
// cleared (immediately eligible), AND the job's checkpoints are deleted so the
// run starts fresh. Clearing checkpoints is deliberate: the usual reason to
// requeue is a code or dependency fix, which can change a workflow's Call
// sequence — resuming into checkpoints recorded by the old run would skip the
// wrong steps and, under Strict determinism, falsely trip the
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
func (s *GormStorage) Requeue(ctx context.Context, jobID string) (bool, error) {
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
					"status":       core.StatusPending,
					"attempt":      0,
					"last_error":   "",
					"result":       nil,
					"locked_by":    "",
					"locked_until": nil,
					"started_at":   nil,
					"completed_at": nil,
					"run_at":       nil,
					"updated_at":   time.Now(),
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
// checkpoints. The root job row itself is NOT touched. Used by Requeue so
// replaying a parent re-dispatches a fresh tree instead of orphaning the old one
// at any depth (a sub-job may itself be a fan-out parent).
//
// It descends level by level, deleting each level before expanding the next.
// parent_job_id/fan_out_id are plain columns (no FK), so a child fan-out is
// still findable by its (now-deleted) parent sub-job's id; and because each
// level's rows are deleted as it is visited, even cyclically corrupt data cannot
// loop (the depth cap is just extra insurance). IN-lists stay bounded to one
// level's width rather than the whole tree.
func (s *GormStorage) deleteFanOutSubtree(tx *gorm.DB, rootJobID string) error {
	frontier := []string{rootJobID}
	for depth := 0; len(frontier) > 0 && depth < maxFanOutTreeDepth; depth++ {
		var fanOutIDs []string
		if err := tx.Model(&core.FanOut{}).
			Where("parent_job_id IN ?", frontier).
			Pluck("id", &fanOutIDs).Error; err != nil {
			return err
		}
		if len(fanOutIDs) == 0 {
			return nil // reached the leaves
		}

		var subJobIDs []string
		if err := tx.Model(&core.Job{}).
			Where("fan_out_id IN ?", fanOutIDs).
			Pluck("id", &subJobIDs).Error; err != nil {
			return err
		}

		if len(subJobIDs) > 0 {
			if err := tx.Where("job_id IN ?", subJobIDs).Delete(&core.Checkpoint{}).Error; err != nil {
				return err
			}
			if err := tx.Where("id IN ?", subJobIDs).Delete(&core.Job{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("id IN ?", fanOutIDs).Delete(&core.FanOut{}).Error; err != nil {
			return err
		}

		// Sub-jobs just deleted may themselves have spawned fan-outs; expand
		// into them next. Their child fan-out rows are still present and remain
		// findable by parent_job_id (a plain column).
		frontier = subJobIDs
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

// SaveJobResult stores the serialized result for a job.
func (s *GormStorage) SaveJobResult(ctx context.Context, jobID string, workerID string, result []byte) error {
	encoded, err := s.encodePayload("job result", jobID, result)
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
// Only pending and waiting jobs can be paused. Running jobs cannot be paused
// because the worker holds the lock and clearing it would corrupt state.
func (s *GormStorage) PauseJob(ctx context.Context, jobID string) error {
	return s.withSerializationRetry(ctx, func() error {
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var job core.Job
			if err := tx.First(&job, "id = ?", jobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return fmt.Errorf("job not found: %s", jobID)
				}
				return err
			}

			// Check if already paused
			if job.Status == core.StatusPaused {
				return core.ErrJobAlreadyPaused
			}

			now := time.Now()

			switch job.Status {
			case core.StatusPending, core.StatusWaiting:
				// Pause: store previous status for restoration on unpause
				return tx.Model(&core.Job{}).
					Where("id = ?", jobID).
					Updates(map[string]any{
						"status":          core.StatusPaused,
						"previous_status": job.Status,
						"updated_at":      now,
					}).Error
			case core.StatusRunning:
				// Cancel: running jobs transition directly to cancelled
				return tx.Model(&core.Job{}).
					Where("id = ?", jobID).
					Updates(map[string]any{
						"status":       core.StatusCancelled,
						"completed_at": now,
						"updated_at":   now,
						"last_error":   "cancelled by user",
						"locked_by":    "",
						"locked_until": nil,
					}).Error
			default:
				return core.ErrCannotPauseStatus
			}
		})
	})
}

// UnpauseJob resumes a paused job back to its previous status.
func (s *GormStorage) UnpauseJob(ctx context.Context, jobID string) error {
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

			return tx.Model(&core.Job{}).
				Where("id = ?", jobID).
				Updates(map[string]any{
					"status":          restoreStatus,
					"previous_status": "",
					"updated_at":      time.Now(),
				}).Error
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
func (s *GormStorage) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
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
