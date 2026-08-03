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
		zeroRetryIDs := explicitZeroRetryIDs(row)
		if err := db.Create(row).Error; err != nil {
			return err
		}
		if err := applyExplicitZeroRetries(db, zeroRetryIDs); err != nil {
			return err
		}
		return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
	}
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs([]*core.Job{job})
	zeroRetryIDs := explicitZeroRetryIDs(row)
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error == nil && result.RowsAffected > 0 {
		if err := applyExplicitZeroRetries(db, zeroRetryIDs); err != nil {
			return err
		}
	}
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
		Where("status IN ?", core.ActiveDedupStatuses)
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
	zeroRetryIDs := explicitZeroRetryIDs(row)
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error == nil && result.RowsAffected > 0 {
		if err := applyExplicitZeroRetries(db, zeroRetryIDs); err != nil {
			return err
		}
	}
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
// THREE RESIDUALS, all SQLite-only. The first two are strictly smaller than the
// bug this removes; the THIRD is a genuine trade, and it is the one to know about.
//
// (1) In a DST zone "local" is not ONE offset year-round: Go renders the offset
// for the instant, so a job scheduled across a boundary is written -05:00 while
// the now-bind renders -04:00, and the lexical compare can still mis-order inside
// that ~1h fold.
//
// (2) Across the upgrade a deployment that consistently passed one foreign zone
// ends up with MIXED faces in run_at — old rows on the foreign offset, new ones
// local — which perturbs the COALESCE(run_at, created_at) FIFO ordering until the
// old rows drain.
//
// (3) THIS MAKES CORRECTNESS DEPEND ON THE WRITER'S TZ MATCHING THE READER'S, and
// unlike (1) and (2) it does not drain. Normalizing to the WRITING process's
// time.Local is right when one process, or a TZ-homogeneous fleet, does both the
// enqueue and the dequeue — which is the overwhelmingly common shape, and the one
// that was BROKEN before this fix whenever the caller supplied a foreign face.
// But a fleet whose processes run in DIFFERENT zones now stores faces the readers
// do not share. Reproduced: a TZ=Asia/Tokyo process enqueues with jobs.At(t.UTC())
// and a TZ=UTC process does not dequeue the job for ~9 hours after it is due; the
// mirror direction fires early. v4.7.0 stored whatever face the APPLICATION
// supplied, so a UTC-supplying app with UTC readers happened to be correct there
// — that specific combination regresses.
//
// The same applies to a single process whose host TZ changes between writing and
// reading a row (a base image bump, adding Environment=TZ=).
//
// Removing residual (3) means comparing INSTANTS rather than rendered text, which
// this package already does for scheduled_fires (see scheduleCursorLess's
// face-aware predicate). Doing it for run_at means touching the hot dequeue
// predicate, where created_at is also local-faced and shares the COALESCE — so it
// is a v5-sized change, not a late edit to a release branch. Documented in
// UPGRADE.md instead.
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
				core.ActiveDedupStatuses)
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
	// The two statements after the insert are blind `UPDATE ... WHERE id IN (...)`,
	// re-applying an explicit Retries(0) and a delayed job's dq_ready=false over
	// the column defaults. This insert is ON CONFLICT DO NOTHING, so an id in
	// those lists may name a row this statement did NOT create — and the UPDATE
	// would then rewrite a live stranger's configuration. Two narrowings make the
	// lists safe. Both are LIST-ONLY: the rows inserted, the errors returned and
	// the ids the caller reads back are all byte-identical to before.
	//
	// That property is the point. Two previous fixes tried to prevent the bad
	// insert instead — one dropped the collapsed job in pkg/queue before storage
	// could repoint its id (the caller got an id naming no row), the other dropped
	// it here before the insert (the job was lost outright when its id-claiming
	// sibling was itself suppressed by the active-unique index). Both predicted
	// what a later step would do and were wrong. Narrowing an UPDATE predicate
	// cannot lose a job or dangle an id, because it changes no control flow.
	preexisting, err := s.batchIDsAlreadyPersisted(db, toCreate)
	if err != nil {
		return err
	}
	correctable := correctableBatchJobs(toCreate, preexisting)
	dqReadyFalseIDs, dqReadyFalseRefs := dqReadyFalseJobs(correctable)
	zeroRetryIDs := explicitZeroRetryIDs(s.encodedSubsetForCorrection(rows, correctable)...)
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error; err != nil {
		return err
	}
	if err := applyExplicitZeroRetries(db, zeroRetryIDs); err != nil {
		return err
	}
	return restoreDQReadyFalse(db, dqReadyFalseIDs, dqReadyFalseRefs)
}

// batchIDsAlreadyPersisted reads which of the batch's ids already name a row
// BEFORE the insert runs. Such a row is not ours: either a stranger holding that
// primary key, or our own row from an at-least-once replay of this same batch.
// Either way the corrective UPDATEs must not touch it — the stranger because
// rewriting it is the defect, the replayed row because it already carries the
// values a previous successful call gave it.
//
// A concurrent unique-key dedup is deliberately NOT covered here and does not
// need to be: that conflict is on the key, not the primary key, so our id was
// never inserted and simply matches no row, leaving the UPDATE a no-op.
func (s *GormStorage) batchIDsAlreadyPersisted(db *gorm.DB, jobs []*core.Job) (map[core.UUID]bool, error) {
	ids := make([]core.UUID, 0, len(jobs))
	for _, job := range jobs {
		if job != nil {
			ids = append(ids, job.ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	seen := make(map[core.UUID]bool, len(ids))
	for _, chunk := range chunkIDs(ids, retentionDeleteChunkSize) {
		var found []core.UUID
		if err := db.Model(&core.Job{}).Where("id IN ?", chunk).Pluck("id", &found).Error; err != nil {
			return nil, err
		}
		for _, id := range found {
			seen[id] = true
		}
	}
	return seen, nil
}

// correctableBatchJobs returns the jobs whose post-insert corrections may safely
// run: those whose id did not already exist, keeping only the FIRST job for any
// id the batch repeats.
//
// The first-wins rule mirrors what the database does. A multi-row INSERT ... ON
// CONFLICT DO NOTHING keeps the first occurrence of a duplicated primary key and
// suppresses the rest, so the surviving row is the first job's. Taking a later
// duplicate's intent would write ITS Retries(0) onto the first job's row — which
// is the reported defect, reachable when queue-level Unique collapse gives two
// entries one id and enqueue middleware then rewrites their keys apart.
func correctableBatchJobs(jobs []*core.Job, preexisting map[core.UUID]bool) []*core.Job {
	out := make([]*core.Job, 0, len(jobs))
	claimed := make(map[core.UUID]bool, len(jobs))
	for _, job := range jobs {
		if job == nil || preexisting[job.ID] || claimed[job.ID] {
			continue
		}
		claimed[job.ID] = true
		out = append(out, job)
	}
	return out
}

// encodedSubsetForCorrection selects the encoded rows matching the given jobs,
// preserving the encoded values explicitZeroRetryIDs inspects.
func (s *GormStorage) encodedSubsetForCorrection(rows []*core.Job, jobs []*core.Job) []*core.Job {
	if len(jobs) == len(rows) {
		return rows
	}
	keep := make(map[core.UUID]bool, len(jobs))
	for _, job := range jobs {
		keep[job.ID] = true
	}
	out := make([]*core.Job, 0, len(jobs))
	for _, row := range rows {
		if row != nil && keep[row.ID] {
			out = append(out, row)
			delete(keep, row.ID)
		}
	}
	return out
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
