package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
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

var _ TxEnqueuer = (*GormStorage)(nil)
var _ TxUniqueLockEnqueuer = (*GormStorage)(nil)

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
	setDQReadyForCreate(job, time.Now())
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

	existing := make(map[string]struct{}, len(keys))
	if len(keys) > 0 {
		query := db.Model(&core.Job{}).
			Select("unique_key").
			Where("unique_key IN ? AND status IN ?", keys,
				[]core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted})
		query = s.lockForUpdate(query, false)

		var found []string
		if err := query.Pluck("unique_key", &found).Error; err != nil {
			return err
		}
		for _, k := range found {
			existing[k] = struct{}{}
		}
	}

	toCreate := make([]*core.Job, 0, len(jobs))
	for _, job := range jobs {
		if job.UniqueKey != "" {
			if _, seen := existing[job.UniqueKey]; seen {
				continue
			}
			existing[job.UniqueKey] = struct{}{}
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
