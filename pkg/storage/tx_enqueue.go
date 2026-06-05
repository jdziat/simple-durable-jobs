package storage

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// TxEnqueuer is the optional storage capability for persisting jobs through a
// caller-owned GORM transaction. Implementations must not commit or roll back
// the supplied transaction.
type TxEnqueuer interface {
	EnqueueTx(ctx context.Context, tx *gorm.DB, job *core.Job) error
	EnqueueUniqueTx(ctx context.Context, tx *gorm.DB, job *core.Job, uniqueKey string) error
	EnqueueBatchTx(ctx context.Context, tx *gorm.DB, jobs []*core.Job) error
}

var _ TxEnqueuer = (*GormStorage)(nil)

// EnqueueTx adds a job using the caller-supplied transaction handle.
func (s *GormStorage) EnqueueTx(ctx context.Context, tx *gorm.DB, job *core.Job) error {
	fillEnqueueDefaults(job)
	row, err := s.encodedJobForCreate(job)
	if err != nil {
		return err
	}
	db := tx.WithContext(ctx)
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

// EnqueueUniqueTx adds a unique job using the caller-supplied transaction handle.
func (s *GormStorage) EnqueueUniqueTx(ctx context.Context, tx *gorm.DB, job *core.Job, uniqueKey string) error {
	fillEnqueueDefaults(job)
	job.UniqueKey = uniqueKey
	db := tx.WithContext(ctx)

	query := db.Where("unique_key = ?", uniqueKey).
		Where("status IN ?", []core.JobStatus{core.StatusPending, core.StatusRunning})
	if !s.isSQLite {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}

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
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
	if result.Error != nil {
		return result.Error
	}
	if uniqueKey != "" && result.RowsAffected == 0 {
		return core.ErrDuplicateJob
	}
	return nil
}

// EnqueueBatchTx inserts multiple jobs using the caller-supplied transaction handle.
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
		job.ID = uuid.New().String()
	}
	if job.Status == "" {
		job.Status = core.StatusPending
	}
	if job.Queue == "" {
		job.Queue = "default"
	}
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
		if !s.isSQLite {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}

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
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(rows).Error
}
