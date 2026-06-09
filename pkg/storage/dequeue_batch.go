package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

const maxDequeueBatch = 1000

const maxLockedBatchEmptyRetries = 20

// DequeueBatch fetches and locks up to limit due jobs in the same ordering as
// Dequeue. This is an optional storage capability; it is intentionally not part
// of core.Storage so external storage implementations remain source-compatible.
func (s *GormStorage) DequeueBatch(ctx context.Context, queues []string, workerID string, limit int) ([]*core.Job, error) {
	return s.dequeueBatch(ctx, queues, workerID, limit, nil)
}

// DequeueBatchPerQueue fetches and locks due jobs for the provided queues,
// claiming at most budgets[q] jobs from each queue while preserving the same
// global ordering as Dequeue.
func (s *GormStorage) DequeueBatchPerQueue(ctx context.Context, workerID string, budgets map[string]int) ([]*core.Job, error) {
	if len(budgets) == 0 {
		return []*core.Job{}, nil
	}
	queues := make([]string, 0, len(budgets))
	limit := 0
	normalizedBudgets := make(map[string]int, len(budgets))
	for queueName, budget := range budgets {
		if budget <= 0 {
			continue
		}
		queues = append(queues, queueName)
		normalizedBudgets[queueName] = budget
		limit += budget
		if limit > maxDequeueBatch {
			limit = maxDequeueBatch
		}
	}
	return s.dequeueBatch(ctx, queues, workerID, limit, normalizedBudgets)
}

func (s *GormStorage) dequeueBatch(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	if limit <= 0 {
		return []*core.Job{}, nil
	}
	if limit > maxDequeueBatch {
		limit = maxDequeueBatch
	}

	pausedQueues, err := s.GetPausedQueues(ctx)
	if err != nil {
		return nil, err
	}
	activeQueues := activeQueuesExcludingPaused(queues, pausedQueues)
	if len(activeQueues) == 0 {
		return []*core.Job{}, nil
	}

	if s.isSQLite {
		return s.dequeueBatchSQLite(ctx, activeQueues, workerID, limit, perQueueBudgets)
	}
	return s.dequeueBatchLocked(ctx, activeQueues, workerID, limit, perQueueBudgets)
}

func activeQueuesExcludingPaused(queues, pausedQueues []string) []string {
	activeQueues := make([]string, 0, len(queues))
	pausedSet := make(map[string]bool, len(pausedQueues))
	for _, q := range pausedQueues {
		pausedSet[q] = true
	}
	for _, q := range queues {
		if !pausedSet[q] {
			activeQueues = append(activeQueues, q)
		}
	}
	return activeQueues
}

func (s *GormStorage) dequeueBatchLocked(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	nowExpr := s.nowExpr()
	lockUntilExpr := s.offsetExpr(s.lockDuration)
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})

	claimedIDs := make([]string, 0, limit)
	claimedPerQueue := make(map[string]int, len(perQueueBudgets))
	emptyRetries := 0
	for len(claimedIDs) < limit {
		var batchIDs []string
		batchQueueCounts := make(map[string]int, len(perQueueBudgets))
		err := s.withSerializationRetry(ctx, func() error {
			batchIDs = nil
			batchQueueCounts = make(map[string]int, len(perQueueBudgets))
			return silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				remaining := limit - len(claimedIDs)
				batchIDs = make([]string, 0, remaining)
				txClaimedPerQueue := make(map[string]int, len(claimedPerQueue)+len(perQueueBudgets))
				for queueName, count := range claimedPerQueue {
					txClaimedPerQueue[queueName] = count
				}
				claimedInTx := make(map[string]struct{}, remaining)
				for _, id := range claimedIDs {
					claimedInTx[id] = struct{}{}
				}
				skippedIDs := make([]string, 0)

				for len(batchIDs) < remaining {
					var candidates []*core.Job
					needed := remaining - len(batchIDs)
					query := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
						Where("queue IN ?", queues).
						Where("status = ?", core.StatusPending).
						Where("(run_at IS NULL OR run_at <= ?)", nowExpr).
						Where("(locked_until IS NULL OR locked_until < ?)", nowExpr)
					if len(skippedIDs) > 0 {
						query = query.Where("id NOT IN ?", skippedIDs)
					}
					if len(claimedInTx) > 0 {
						ids := make([]string, 0, len(claimedInTx))
						for id := range claimedInTx {
							ids = append(ids, id)
						}
						query = query.Where("id NOT IN ?", ids)
					}
					result := query.
						Order("priority DESC, created_at ASC").
						Limit(needed).
						Find(&candidates)
					if result.Error != nil {
						return result.Error
					}
					if len(candidates) == 0 {
						break
					}

					claimIDs := make([]string, 0, len(candidates))
					claimQueues := make(map[string]int, len(candidates))
					for _, job := range candidates {
						var queueState core.QueueState
						if err := tx.First(&queueState, "queue = ?", job.Queue).Error; err == nil && queueState.Paused {
							skippedIDs = append(skippedIDs, job.ID)
							continue
						} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
							return err
						}
						if budget, ok := perQueueBudgets[job.Queue]; ok && txClaimedPerQueue[job.Queue] >= budget {
							skippedIDs = append(skippedIDs, job.ID)
							continue
						}
						claimIDs = append(claimIDs, job.ID)
						claimQueues[job.Queue]++
						txClaimedPerQueue[job.Queue]++
					}
					if len(claimIDs) == 0 {
						continue
					}

					if err := tx.Model(&core.Job{}).
						Where("id IN ?", claimIDs).
						Updates(map[string]any{
							"status":       core.StatusRunning,
							"locked_by":    workerID,
							"locked_until": lockUntilExpr,
							"started_at":   nowExpr,
							"attempt":      gorm.Expr("attempt + 1"),
						}).Error; err != nil {
						return err
					}
					for _, id := range claimIDs {
						batchIDs = append(batchIDs, id)
						claimedInTx[id] = struct{}{}
					}
					for queueName, count := range claimQueues {
						batchQueueCounts[queueName] += count
					}
				}
				return nil
			})
		})
		if err != nil {
			return nil, err
		}

		if len(batchIDs) > 0 {
			claimedIDs = append(claimedIDs, batchIDs...)
			for queueName, count := range batchQueueCounts {
				claimedPerQueue[queueName] += count
			}
			emptyRetries = 0
			continue
		}
		if len(perQueueBudgets) > 0 {
			break
		}

		hasMore, err := s.hasClaimableBatchJob(ctx, queues)
		if err != nil {
			return nil, err
		}
		if !hasMore {
			break
		}
		emptyRetries++
		if emptyRetries >= maxLockedBatchEmptyRetries {
			break
		}
		if err := sleepDequeueBatchRetry(ctx, time.Duration(emptyRetries)*2*time.Millisecond); err != nil {
			return nil, err
		}
	}

	var jobs []*core.Job
	if len(claimedIDs) == 0 {
		jobs = []*core.Job{}
	} else if err := s.db.WithContext(ctx).
		Where("id IN ?", claimedIDs).
		Order("priority DESC, created_at ASC").
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *GormStorage) hasClaimableBatchJob(ctx context.Context, queues []string) (bool, error) {
	var job core.Job
	err := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)}).
		WithContext(ctx).
		Select("id").
		Where("queue IN ?", queues).
		Where("status = ?", core.StatusPending).
		Where("(run_at IS NULL OR run_at <= ?)", s.nowExpr()).
		Where("(locked_until IS NULL OR locked_until < ?)", s.nowExpr()).
		Order("priority DESC, created_at ASC").
		First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	return err == nil, err
}

func sleepDequeueBatchRetry(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *GormStorage) dequeueBatchSQLite(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	now := time.Now()
	lockUntil := now.Add(s.lockDuration)

	var jobs []*core.Job
	err := s.withSerializationRetry(ctx, func() error {
		jobs = nil
		claimedIDs := make(map[string]struct{}, limit)
		claimedPerQueue := make(map[string]int, len(perQueueBudgets))
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			for len(jobs) < limit {
				var job core.Job
				query := tx.
					Where("queue IN ?", queues).
					Where("status = ?", core.StatusPending).
					Where("(run_at IS NULL OR run_at <= ?)", now).
					Where("(locked_until IS NULL OR locked_until < ?)", now)
				if len(claimedIDs) > 0 {
					ids := make([]string, 0, len(claimedIDs))
					for id := range claimedIDs {
						ids = append(ids, id)
					}
					query = query.Where("id NOT IN ?", ids)
				}
				result := query.Order("priority DESC, created_at ASC").First(&job)
				if result.Error != nil {
					if errors.Is(result.Error, gorm.ErrRecordNotFound) {
						return nil
					}
					return result.Error
				}

				var queueState core.QueueState
				if err := tx.First(&queueState, "queue = ?", job.Queue).Error; err == nil && queueState.Paused {
					claimedIDs[job.ID] = struct{}{}
					continue
				} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if budget, ok := perQueueBudgets[job.Queue]; ok && claimedPerQueue[job.Queue] >= budget {
					claimedIDs[job.ID] = struct{}{}
					continue
				}

				updateResult := tx.Model(&core.Job{}).
					Where("id = ?", job.ID).
					Where("status = ?", core.StatusPending).
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
					claimedIDs[job.ID] = struct{}{}
					continue
				}

				job.Status = core.StatusRunning
				job.LockedBy = workerID
				job.LockedUntil = &lockUntil
				job.StartedAt = &now
				job.Attempt++
				jobs = append(jobs, &job)
				claimedIDs[job.ID] = struct{}{}
				claimedPerQueue[job.Queue]++
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if jobs == nil {
		jobs = []*core.Job{}
	}
	if err := s.decodeJobListPayloads(jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
