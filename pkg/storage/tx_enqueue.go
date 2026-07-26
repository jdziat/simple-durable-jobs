package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TxEnqueuer is the optional storage capability for persisting jobs through a
// caller-owned GORM transaction. Implementations must not commit or roll back
// the supplied transaction.
type TxEnqueuer interface {
	EnqueueTx(ctx context.Context, tx *gorm.DB, job *core.Job) error
	EnqueueUniqueTx(ctx context.Context, tx *gorm.DB, job *core.Job, uniqueKey string) error
	EnqueueBatchTx(ctx context.Context, tx *gorm.DB, jobs []*core.Job) error
}

// TxUniqueLockEnqueuer is the optional storage capability for atomic windowed
// enqueue deduplication inside a caller-owned GORM transaction.
type TxUniqueLockEnqueuer interface {
	EnqueueWithUniqueLockTx(ctx context.Context, tx *gorm.DB, job *core.Job, scopeHash string, ttl time.Duration) (core.UUID, error)
}

// ScheduledFireTxClaimer claims a schedule's fire boundary within a caller-owned
// transaction, so the durable claim can be committed atomically with the enqueue
// of the fired job (rolling both back together on failure). Implemented by
// GormStorage; consumed by Queue.EnqueueScheduledFire to avoid a silently dropped
// scheduled run when the enqueue fails after the claim (teardown g8).
type ScheduledFireTxClaimer interface {
	ClaimScheduledFireTx(ctx context.Context, tx *gorm.DB, name string, fireTime time.Time) (bool, error)
}

var _ TxEnqueuer = (*GormStorage)(nil)
var _ TxUniqueLockEnqueuer = (*GormStorage)(nil)
var _ ScheduledFireTxClaimer = (*GormStorage)(nil)

// EnqueueTx adds a job using the caller-supplied transaction handle.
//
// Under MySQL, callers MUST wrap the owning transaction in
// serialization-failure retry. The in-transaction unique-key FOR UPDATE dedup
// can gap-lock deadlock under contention (surfaced as error 1213). Prefer
// GormStorage.WithSerializationRetry around the full begin -> EnqueueTx ->
// commit transaction.
func (s *GormStorage) EnqueueTx(ctx context.Context, tx *gorm.DB, job *core.Job) error {
	fillEnqueueDefaults(job)
	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return err
	}
	db := tx.WithContext(ctx)
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

// EnqueueUniqueTx adds a unique job using the caller-supplied transaction handle.
//
// Under MySQL, callers MUST wrap the owning transaction in
// serialization-failure retry. The in-transaction unique-key FOR UPDATE dedup
// can gap-lock deadlock under contention (surfaced as error 1213). Prefer
// GormStorage.WithSerializationRetry around the full begin -> EnqueueUniqueTx
// -> commit transaction.
func (s *GormStorage) EnqueueUniqueTx(ctx context.Context, tx *gorm.DB, job *core.Job, uniqueKey string) error {
	fillEnqueueDefaults(job)
	job.UniqueKey = uniqueKey
	db := tx.WithContext(ctx)

	query := db.Where("unique_key = ?", uniqueKey).
		Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning})
	query = s.lockForUpdate(query, false)

	var existing core.Job
	err := query.First(&existing).Error
	if err == nil {
		return core.ErrDuplicateJob
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return err
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error != nil {
		return result.Error
	}
	if uniqueKey != "" && result.RowsAffected == 0 {
		return core.ErrDuplicateJob
	}
	return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
}

// EnqueueWithUniqueLockTx adds a job under a time-bounded unique lock using
// the caller-supplied transaction handle.
func (s *GormStorage) EnqueueWithUniqueLockTx(ctx context.Context, tx *gorm.DB, job *core.Job, scopeHash string, ttl time.Duration) (core.UUID, error) {
	if scopeHash == "" || ttl <= 0 {
		return core.NilUUID, core.ErrStorageNoUniqueLocks
	}
	fillEnqueueDefaults(job)
	return s.enqueueWithUniqueLockDB(ctx, tx.WithContext(ctx), job, scopeHash, ttl)
}

// EnqueueBatchTx inserts multiple jobs using the caller-supplied transaction handle.
//
// Under MySQL, callers MUST wrap the owning transaction in
// serialization-failure retry. The in-transaction unique-key FOR UPDATE dedup
// can gap-lock deadlock under contention (surfaced as error 1213). Prefer
// GormStorage.WithSerializationRetry around the full begin -> EnqueueBatchTx
// -> commit transaction.
func (s *GormStorage) EnqueueBatchTx(ctx context.Context, tx *gorm.DB, jobs []*core.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		fillEnqueueDefaults(job)
	}
	return s.enqueueBatchWithDB(tx.WithContext(ctx), jobs)
}

func fillEnqueueDefaults(job *core.Job) {
	if job.ID == "" {
		job.ID = core.NewID()
	}
	if job.Status == "" {
		job.Status = core.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
	normalizeRunAtZone(job)
	setDQReadyForCreate(job, time.Now())
}

// normalizeRunAtZone re-points job.RunAt at the SAME INSTANT rendered in this
// process's local zone. It changes no instant — only the clock face the instant
// is written on.
//
// This exists for SQLite, which has no datetime type: mattn/go-sqlite3 binds
// every time.Time as TEXT using "2006-01-02 15:04:05.999999999-07:00" (note the
// trailing offset, and that a UTC value renders "+00:00", never "Z"), and SQLite
// compares those strings LEXICALLY. Every "is this job due yet" predicate binds
// this process's wall clock, carrying the LOCAL offset — so a run_at handed in
// on a different clock face (queue.At with a UTC time, a parsed RFC 3339 "...Z",
// a protobuf timestamp) is compared character-by-character against a
// differently-offset string and mis-orders by the full delta between the zones,
// up to 14 hours, in either direction.
//
// It REPOINTS rather than writing through the pointer: job.RunAt aliases the
// *time.Time inside queue.At's Option, and mutating it would corrupt an Option
// the caller may reuse across enqueues.
//
// time.Local, not UTC, is the target on purpose. UTC would put newly written
// rows on a clock face that every ALREADY-STORED row does not share — including
// GORM's autoCreateTime created_at — so in a positive-offset zone a UTC-bound
// comparison reads a locally-written created_at as still in the future, which
// would strand every pre-existing pending job. Local keeps new rows on the same
// face as old rows and as the binds.
//
// TWO RESIDUALS, both SQLite-only and both strictly smaller than the bug this
// removes. (1) In a DST zone "local" is not ONE offset year-round: Go renders the
// offset for the instant, so a job scheduled across a boundary is written -05:00
// while the now-bind renders -04:00, and the lexical compare can still mis-order
// inside that ~1h fold. (2) Across the upgrade a deployment that consistently
// passed one foreign zone ends up with MIXED faces in run_at — old rows on the
// foreign offset, new ones local — which perturbs the COALESCE(run_at,
// created_at) FIFO ordering until the old rows drain. Neither makes eligibility
// worse than it was; a full fix needs a stored ordering key, which is a v5
// schema change.
//
// A no-op on Postgres (timestamptz stores the instant) and MySQL (the driver
// re-renders in the DSN location), so no dialect gate is needed.
func normalizeRunAtZone(job *core.Job) {
	if job.RunAt == nil || job.RunAt.Location() == time.Local {
		return
	}
	local := job.RunAt.In(time.Local)
	job.RunAt = &local
}

func setDQReadyForCreate(job *core.Job, now time.Time) {
	job.DQReady = job.Status == core.StatusPending && (job.RunAt == nil || !job.RunAt.After(now))
}

func (s *GormStorage) enqueueBatchWithDB(db *gorm.DB, jobs []*core.Job) error {
	keys := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if job.UniqueKey != "" {
			keys = append(keys, job.UniqueKey)
		}
	}

	existing := make(map[string]core.UUID, len(keys))
	if len(keys) > 0 {
		query := db.Model(&core.Job{}).
			Select("id", "unique_key").
			Where("unique_key IN ? AND status IN ?", keys,
				[]core.JobStatus{core.StatusPending, core.StatusRunning})
		query = s.lockForUpdate(query, false)

		var found []struct {
			ID        core.UUID
			UniqueKey string
		}
		if err := query.Find(&found).Error; err != nil {
			return err
		}
		for _, row := range found {
			existing[row.UniqueKey] = row.ID
		}
	}

	toCreate := make([]*core.Job, 0, len(jobs))
	for _, job := range jobs {
		if job.UniqueKey != "" {
			if existingID, seen := existing[job.UniqueKey]; seen {
				job.ID = existingID
				continue
			}
			existing[job.UniqueKey] = job.ID
		}
		toCreate = append(toCreate, job)
	}

	if len(toCreate) == 0 {
		return nil
	}
	rows, err := s.encodedJobsForCreate(toCreate)
	if err != nil {
		return err
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs(toCreate)
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error; err != nil {
		return err
	}
	return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
}

func dqReadyFalseJobs(jobs []*core.Job) ([]core.UUID, []*core.Job) {
	ids := make([]core.UUID, 0)
	refs := make([]*core.Job, 0)
	for _, job := range jobs {
		if job != nil && !job.DQReady {
			ids = append(ids, job.ID)
			refs = append(refs, job)
		}
	}
	return ids, refs
}

func restoreDQReadyFalse(db *gorm.DB, ids []core.UUID, jobs []*core.Job) error {
	if len(ids) == 0 {
		return nil
	}
	err := db.Model(&core.Job{}).
		Where("id IN ?", ids).
		UpdateColumn("dq_ready", false).Error
	for _, job := range jobs {
		job.DQReady = false
	}
	return err
}
