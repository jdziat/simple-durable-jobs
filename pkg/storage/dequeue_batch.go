package storage

import (
	"context"
	"errors"
	"sort"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
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

	jobs, err := s.dequeueBatchOnce(ctx, queues, workerID, limit, perQueueBudgets)
	if err != nil || len(jobs) > 0 {
		return jobs, err
	}
	// Self-heal: nothing claimable this pass. Promote any pending job that has
	// become eligible but is still flagged dq_ready=false and retry once, so
	// batch dequeue correctness does not depend on the external promoter loop
	// (see Dequeue).
	promoted, perr := s.PromoteReadyJobs(ctx)
	if perr != nil || promoted == 0 {
		return jobs, nil
	}
	return s.dequeueBatchOnce(ctx, queues, workerID, limit, perQueueBudgets)
}

func (s *GormStorage) dequeueBatchOnce(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	// MySQL has no RETURNING and cannot reference the UPDATE's target table in a
	// subquery, so it keeps the candidate-SELECT -> UPDATE path. Postgres and
	// SQLite (>= 3.35) claim with one UPDATE ... RETURNING per queue.
	if s.dialect() == dialectMySQL {
		return s.dequeueBatchLocked(ctx, queues, workerID, limit, perQueueBudgets)
	}
	return s.dequeueBatchReturning(ctx, queues, workerID, limit, perQueueBudgets)
}

// dequeueBatchReturning claims due jobs with a SINGLE atomic UPDATE ... RETURNING
// per queue, on dialects that support RETURNING with an UPDATE whose subquery
// reads the same table AND the AS MATERIALIZED CTE fence below (Postgres >= 12,
// SQLite >= 3.35 — MATERIALIZED landed in both by those versions; SQLite < 3.35
// also lacks RETURNING). It replaces dequeueBatchLocked's
// candidate-SELECT -> per-candidate paused/budget loop -> UPDATE -> re-fetch
// sequence with:
//
//	WITH claimed AS MATERIALIZED (
//	    SELECT id FROM jobs <claimableCandidates predicate> ORDER BY ... LIMIT n
//	    FOR UPDATE SKIP LOCKED          -- a no-op on SQLite (single writer)
//	)
//	UPDATE jobs SET status='running', ... WHERE id IN (SELECT id FROM claimed)
//	RETURNING *
//
// Correctness invariants preserved:
//   - The locking candidate subquery MUST be fenced in a MATERIALIZED CTE, not
//     inlined as `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT
//     n)`. Postgres is free to place a bare locking subquery on the INNER side of
//     a nested-loop semi-join and RE-EVALUATE it per candidate row, applying LIMIT
//     n each iteration; because every UPDATE writes a new row version, each
//     re-execution locks a different row first and the LIMIT is effectively
//     ignored — a runaway that claims far more than n (observed: 10 for LIMIT 5,
//     so a worker over-dispatches its whole batch). MATERIALIZED is an
//     optimization fence forcing single evaluation, so exactly n ids are locked
//     and updated. See CYBERTEC, "How to do UPDATE ... LIMIT in PostgreSQL", and
//     TestDequeueBatch_NeverOverDispatchesUnderChurn.
//   - The paused-queue exclusion lives in claimableCandidates' uncorrelated
//     subquery, evaluated within the single claim statement, so this path does not
//     need dequeueBatchLocked's separate per-candidate First() paused re-check
//     (which exists only because that path is multi-statement). This NARROWS the
//     paused race — it does not close it: under READ COMMITTED the subquery is an
//     InitPlan evaluated once at the statement's command snapshot, and PG's
//     EvalPlanQual rechecks only the locked jobs row, not the queue_states
//     subquery, so a PauseQueue that commits AFTER that snapshot can still admit a
//     job from the just-paused queue. That residual window (one sub-millisecond
//     autocommit statement) is inherent to READ COMMITTED and existed identically
//     on the old path (its re-check also missed a pause committing after its own
//     snapshot); a pause committed BEFORE the claim is always excluded. PauseQueue
//     carries no atomic-fence contract (in-flight jobs keep running; it only stops
//     new dispatch), so this is acceptable, not a regression.
//   - Per-queue budgets are exact: each queue is claimed in its own statement with
//     LIMIT = budget.
//   - SKIP LOCKED still partitions concurrent claimers (no-op under SQLite's
//     single writer). DB-clock leases (Postgres) / wall clock (SQLite) unchanged.
//   - decodeClaimedBatch still releases+excludes poison rows.
//
// The single autocommit statement also removes the per-batch BEGIN/COMMIT.
func (s *GormStorage) dequeueBatchReturning(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	dur := time.Duration(s.lockDuration.Load())
	var nowVal, lockUntilVal any
	if s.useDBClock() {
		nowVal = s.nowExpr()
		lockUntilVal = s.offsetExpr(dur)
	} else {
		now := time.Now()
		nowVal = now
		lockUntilVal = now.Add(dur)
	}
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})

	// One claim unit per queue when per-queue budgets apply (LIMIT = budget),
	// otherwise a single flat claim over all queues with the global limit.
	type claimUnit struct {
		queues []string
		limit  int
	}
	var units []claimUnit
	if len(perQueueBudgets) > 0 {
		units = make([]claimUnit, 0, len(queues))
		for _, q := range queues {
			if b := perQueueBudgets[q]; b > 0 {
				units = append(units, claimUnit{queues: []string{q}, limit: b})
			}
		}
	} else {
		units = []claimUnit{{queues: queues, limit: limit}}
	}

	claimed := make([]*core.Job, 0, limit)
	for _, u := range units {
		if len(claimed) >= limit {
			break
		}
		n := u.limit
		if rem := limit - len(claimed); n > rem {
			n = rem
		}
		var rows []*core.Job
		err := s.withSerializationRetry(ctx, func() error {
			rows = nil
			// The claimable-candidate SELECT carries the LIMIT and, on Postgres,
			// FOR UPDATE SKIP LOCKED.
			sub := s.claimableCandidates(
				s.lockForUpdate(silentDB.WithContext(ctx).Model(&core.Job{}).Select("id"), true),
				u.queues, nowVal,
			).Limit(n)
			// Wrap the locking subquery in a MATERIALIZED CTE so the planner
			// evaluates it EXACTLY ONCE. A bare `UPDATE ... WHERE id IN (SELECT
			// ... FOR UPDATE SKIP LOCKED LIMIT n)` lets Postgres place the locking
			// subquery on the INNER side of a nested loop and re-evaluate it per
			// candidate row, applying LIMIT n per iteration — a runaway that claims
			// far more than n rows (observed: 10 rows for LIMIT 5, so a worker
			// over-dispatches its whole batch). MATERIALIZED is an optimization
			// fence that forces single evaluation, so exactly n ids are locked and
			// updated. See CYBERTEC, "How to do UPDATE ... LIMIT in PostgreSQL".
			// SQLite (>= 3.35, this path's floor) accepts AS MATERIALIZED too; it
			// has no row locks (single writer), so the fence only pins the LIMIT
			// there. The SET list matches the prior GORM .Updates(map) claim column
			// for column, INCLUDING updated_at (Job.UpdatedAt is autoUpdateTime, so
			// the map form bumped it implicitly; the raw statement bypasses that
			// callback and must set it explicitly to stay consistent with every
			// other claim path) and last_heartbeat_at=NULL (CD-01: clear the stale
			// heartbeat so the reaper anchors on this claim's started_at).
			return silentDB.WithContext(ctx).Raw(
				"WITH claimed AS MATERIALIZED (?) "+
					"UPDATE jobs SET status = ?, locked_by = ?, locked_until = ?, "+
					"started_at = ?, updated_at = ?, attempt = attempt + 1, "+
					"last_heartbeat_at = NULL "+
					"WHERE id IN (SELECT id FROM claimed) RETURNING *",
				sub, core.StatusRunning, workerID, lockUntilVal, nowVal, nowVal,
			).Scan(&rows).Error
		})
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, rows...)
	}

	if len(claimed) == 0 {
		return []*core.Job{}, nil
	}
	// RETURNING gives no ordering guarantee; restore the dequeue priority order
	// (claimed rows are status='running', so order by the always-defined
	// COALESCE(run_at, created_at), matching dequeueBatchLocked's final sort).
	sortClaimedBatch(claimed)
	return s.decodeClaimedBatch(ctx, claimed, workerID)
}

// sortClaimedBatch orders a claimed batch by priority DESC, then
// COALESCE(run_at, created_at) ASC — the same deterministic order Dequeue uses.
func sortClaimedBatch(jobs []*core.Job) {
	sort.SliceStable(jobs, func(i, k int) bool {
		a, b := jobs[i], jobs[k]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return claimOrderKey(a).Before(claimOrderKey(b))
	})
}

func claimOrderKey(j *core.Job) time.Time {
	if j.RunAt != nil {
		return *j.RunAt
	}
	return j.CreatedAt
}

func (s *GormStorage) dequeueBatchLocked(ctx context.Context, queues []string, workerID string, limit int, perQueueBudgets map[string]int) ([]*core.Job, error) {
	nowExpr := s.nowExpr()
	lockUntilExpr := s.offsetExpr(time.Duration(s.lockDuration.Load()))
	silentDB := s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)})

	claimedIDs := make([]core.UUID, 0, limit)
	claimedPerQueue := make(map[string]int, len(perQueueBudgets))
	emptyRetries := 0
	for len(claimedIDs) < limit {
		var batchIDs []core.UUID
		batchQueueCounts := make(map[string]int, len(perQueueBudgets))
		err := s.withSerializationRetry(ctx, func() error {
			batchIDs = nil
			batchQueueCounts = make(map[string]int, len(perQueueBudgets))
			return silentDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				remaining := limit - len(claimedIDs)
				batchIDs = make([]core.UUID, 0, remaining)
				txClaimedPerQueue := make(map[string]int, len(claimedPerQueue)+len(perQueueBudgets))
				for queueName, count := range claimedPerQueue {
					txClaimedPerQueue[queueName] = count
				}
				claimedInTx := make(map[core.UUID]struct{}, remaining)
				for _, id := range claimedIDs {
					claimedInTx[id] = struct{}{}
				}
				skippedIDs := make([]core.UUID, 0)
				// Queues that have hit their per-queue budget this tx. Excluding them
				// from subsequent scans — instead of fetching, SKIP-LOCK-locking, and
				// skipping each of their rows — stops the loop from walking a
				// budget-exhausted queue's entire backlog, an O(P) ordered-scan +
				// O(P) held-lock cliff that pinned hot queues and starved other
				// workers (teardown g3).
				exhausted := make(map[string]bool)

				for len(batchIDs) < remaining {
					activeQueues := queues
					if len(perQueueBudgets) > 0 && len(exhausted) > 0 {
						activeQueues = make([]string, 0, len(queues))
						for _, qn := range queues {
							if !exhausted[qn] {
								activeQueues = append(activeQueues, qn)
							}
						}
						if len(activeQueues) == 0 {
							break
						}
					}
					var candidates []*core.Job
					needed := remaining - len(batchIDs)
					query := s.claimableCandidates(s.lockForUpdate(tx, true), activeQueues, nowExpr)
					if len(skippedIDs) > 0 {
						query = query.Where("id NOT IN ?", skippedIDs)
					}
					if len(claimedInTx) > 0 {
						ids := make([]core.UUID, 0, len(claimedInTx))
						for id := range claimedInTx {
							ids = append(ids, id)
						}
						query = query.Where("id NOT IN ?", ids)
					}
					result := query.
						Limit(needed).
						Find(&candidates)
					if result.Error != nil {
						return result.Error
					}
					if len(candidates) == 0 {
						break
					}

					claimIDs := make([]core.UUID, 0, len(candidates))
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
							exhausted[job.Queue] = true // stop scanning this queue's backlog
							skippedIDs = append(skippedIDs, job.ID)
							continue
						}
						claimIDs = append(claimIDs, job.ID)
						claimQueues[job.Queue]++
						txClaimedPerQueue[job.Queue]++
						if budget, ok := perQueueBudgets[job.Queue]; ok && txClaimedPerQueue[job.Queue] >= budget {
							exhausted[job.Queue] = true // just reached budget
						}
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
							// Clear the prior run's heartbeat on claim so the stale-lock
							// reaper anchors on this claim's started_at, not a stale
							// heartbeat (CD-01 double-execution). See Dequeue.
							"last_heartbeat_at": gorm.Expr("NULL"),
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
		// Claimed rows are status='running', so the MySQL dq_eligible_at generated
		// column is NULL for them; order the returned batch by the always-defined
		// COALESCE(run_at, created_at) for a deterministic priority,time ordering.
		Order("priority DESC, COALESCE(run_at, created_at) ASC").
		Find(&jobs).Error; err != nil {
		return nil, err
	}
	decoded, err := s.decodeClaimedBatch(ctx, jobs, workerID)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// decodeClaimedBatch decodes each freshly-claimed job's payloads. A job whose
// payload cannot be decoded (e.g. a custom codec failing on a poison row) is
// RELEASED back to pending and EXCLUDED from the returned batch, rather than
// failing the whole call and stranding EVERY claimed row (locked until the
// stale-lock reaper, ~45m by default) on a single bad payload (teardown g3). The
// successfully-decoded jobs are returned so good work is never blocked by a poison
// sibling. The released poison row is re-claimable (and will re-fail decode) — a
// visible, self-contained symptom of a misconfigured codec, not a fleet-wide stall.
func (s *GormStorage) decodeClaimedBatch(ctx context.Context, jobs []*core.Job, workerID string) ([]*core.Job, error) {
	out := jobs[:0]
	for _, job := range jobs {
		if err := s.decodeJobPayloads(job); err != nil {
			if rerr := s.Release(ctx, job.ID, workerID); rerr != nil && !errors.Is(rerr, core.ErrJobNotOwned) {
				return nil, rerr
			}
			continue
		}
		out = append(out, job)
	}
	return out, nil
}

func (s *GormStorage) hasClaimableBatchJob(ctx context.Context, queues []string) (bool, error) {
	var job core.Job
	err := s.claimableCandidates(s.db.Session(&gorm.Session{Logger: s.db.Logger.LogMode(logger.Silent)}), queues, s.nowExpr()).
		WithContext(ctx).
		Select("id").
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
