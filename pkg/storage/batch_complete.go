package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// BatchCompleteItem is one leaf job to complete in a batch.
type BatchCompleteItem struct {
	JobID  core.UUID
	Result []byte // raw handler result; encoded by BatchComplete via the codec
}

// BatchComplete completes a batch of OWNED running LEAF jobs in a single
// transaction — one commit (one fsync) per batch instead of one per job, which is
// the dominant cost of high-throughput leaf-job completion. It is an optional
// storage capability (not on core.Storage); callers degrade to per-job completion
// when a backend does not implement it.
//
// Each job is completed only if still owned (locked_by = workerID AND status =
// 'running'); a row reclaimed by the stale-lock reaper is excluded. The returned
// slice is the ids that actually committed (in no particular order) — the caller
// fires completion hooks for those and treats the rest as not-owned. Concurrency
// slots, and optionally checkpoints, for the committed ids are deleted in the SAME
// transaction so a crash cannot orphan a slot.
//
// Fan-out sub-jobs MUST NOT be passed here: their parent-counter accounting is
// per-job and order-sensitive. The caller routes them to CompleteWithResult.
func (s *GormStorage) BatchComplete(ctx context.Context, workerID string, items []BatchCompleteItem, deleteCheckpoints bool) ([]core.UUID, error) {
	if len(items) == 0 {
		return nil, nil
	}
	// Encode each result up front so codec errors surface here (on the caller's
	// goroutine) before any DB write, never mid-flush.
	encoded := make([][]byte, len(items))
	for i, it := range items {
		enc, err := s.encodePayload("job result", string(it.JobID), it.Result)
		if err != nil {
			return nil, fmt.Errorf("batch complete: encode result %s: %w", it.JobID, err)
		}
		encoded[i] = enc
	}

	var committed []core.UUID
	err := s.withSerializationRetry(ctx, func() error {
		committed = nil
		return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			ids, err := s.batchCompleteFlip(tx, workerID, items, encoded)
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			// Crash-safe: release fleet concurrency slots and (opt-in) GC checkpoints
			// for the jobs that actually completed, in the same tx as the status flip.
			// Keyed by job_id, so the per-slot-name sentinel (job_id='') is never hit.
			if err := tx.Where("job_id IN ?", ids).Delete(&core.ConcurrencySlot{}).Error; err != nil {
				return err
			}
			if deleteCheckpoints {
				if err := tx.Where("job_id IN ?", ids).Delete(&core.Checkpoint{}).Error; err != nil {
					return err
				}
			}
			committed = ids
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return committed, nil
}

// batchCompleteFlip runs the dialect-specific batched terminal UPDATE and returns
// the ids that committed (still owned + running). The ownership CAS rides in every
// dialect's WHERE, so a reaper-reclaimed row is excluded.
func (s *GormStorage) batchCompleteFlip(tx *gorm.DB, workerID string, items []BatchCompleteItem, encoded [][]byte) ([]core.UUID, error) {
	switch s.dialect() {
	case dialectMySQL:
		return s.batchCompleteFlipMySQL(tx, workerID, items, encoded)
	case dialectSQLite:
		return s.batchCompleteFlipSQLite(tx, workerID, items, encoded)
	default:
		return s.batchCompleteFlipPostgres(tx, workerID, items, encoded)
	}
}

// Postgres: UPDATE ... FROM (VALUES ...) ... RETURNING. The id/result column types
// are pinned by ::uuid/::bytea casts on the FIRST VALUES row (Postgres infers the
// rest). Ids are passed as canonical TEXT (raw 16 bytes will not cast to `uuid`).
func (s *GormStorage) batchCompleteFlipPostgres(tx *gorm.DB, workerID string, items []BatchCompleteItem, encoded [][]byte) ([]core.UUID, error) {
	rows := make([]string, len(items))
	args := make([]any, 0, len(items)*2+1)
	for i, it := range items {
		if i == 0 {
			rows[i] = "(?::uuid,?::bytea)"
		} else {
			rows[i] = "(?,?)"
		}
		args = append(args, string(it.JobID), encoded[i])
	}
	args = append(args, workerID)
	sql := `UPDATE jobs AS j SET status = 'completed', locked_by = '', locked_until = NULL, ` +
		`result = v.result, completed_at = NOW() ` +
		`FROM (VALUES ` + strings.Join(rows, ",") + `) AS v(id, result) ` +
		`WHERE j.id = v.id AND j.locked_by = ? AND j.status = 'running' RETURNING j.id`
	var committed []core.UUID
	if err := tx.Raw(sql, args...).Scan(&committed).Error; err != nil {
		return nil, err
	}
	return committed, nil
}

// SQLite: UPDATE ... FROM (SELECT ? AS id, ? AS result UNION ALL ...) ... RETURNING.
// The `AS v(id,result)` column-alias form is rejected by SQLite, so a UNION-ALL
// derived table is used. completed_at is a bound wall-clock value (single-clock
// backend). Raw 16-byte blob ids match jobs.id directly (no cast).
func (s *GormStorage) batchCompleteFlipSQLite(tx *gorm.DB, workerID string, items []BatchCompleteItem, encoded [][]byte) ([]core.UUID, error) {
	selects := make([]string, len(items))
	args := make([]any, 0, len(items)*2+2)
	now := time.Now().UTC()
	args = append(args, now) // leading completed_at bind
	for i, it := range items {
		if i == 0 {
			selects[i] = "SELECT ? AS id, ? AS result"
		} else {
			selects[i] = "SELECT ?, ?"
		}
		args = append(args, it.JobID, encoded[i])
	}
	args = append(args, workerID)
	sql := `UPDATE jobs SET status = 'completed', locked_by = '', locked_until = NULL, ` +
		`result = v.result, completed_at = ? ` +
		`FROM (` + strings.Join(selects, " UNION ALL ") + `) AS v ` +
		`WHERE jobs.id = v.id AND jobs.locked_by = ? AND jobs.status = 'running' RETURNING jobs.id`
	var committed []core.UUID
	if err := tx.Raw(sql, args...).Scan(&committed).Error; err != nil {
		return nil, err
	}
	return committed, nil
}

// MySQL has no RETURNING and no UPDATE...FROM, so it JOINs a UNION-ALL derived
// table and learns the committed set from a scoped follow-up SELECT in the same
// tx. CAST(? AS BINARY) per row is mandatory — otherwise the derived id column
// types as utf8mb4 and silently never joins the binary(16) jobs.id. The follow-up
// SELECT is anchored to a DB-clock batch-start so it cannot match jobs completed
// by an earlier batch.
func (s *GormStorage) batchCompleteFlipMySQL(tx *gorm.DB, workerID string, items []BatchCompleteItem, encoded [][]byte) ([]core.UUID, error) {
	var batchStart time.Time
	if err := tx.Raw("SELECT NOW(6)").Scan(&batchStart).Error; err != nil {
		return nil, err
	}

	selects := make([]string, len(items))
	ids := make([]core.UUID, len(items))
	args := make([]any, 0, len(items)*2+1)
	for i, it := range items {
		if i == 0 {
			selects[i] = "SELECT CAST(? AS BINARY) AS id, ? AS result"
		} else {
			selects[i] = "SELECT CAST(? AS BINARY), ?"
		}
		args = append(args, it.JobID, encoded[i])
		ids[i] = it.JobID
	}
	args = append(args, workerID)
	sql := `UPDATE jobs j JOIN (` + strings.Join(selects, " UNION ALL ") + `) v ON j.id = v.id ` +
		`SET j.status = 'completed', j.locked_by = '', j.locked_until = NULL, ` +
		`j.result = v.result, j.completed_at = NOW(6) ` +
		`WHERE j.locked_by = ? AND j.status = 'running'`
	if err := tx.Exec(sql, args...).Error; err != nil {
		return nil, err
	}

	// Which of the requested ids did THIS batch commit? Tightened predicate so a
	// job completed by a prior batch (already locked_by='', status='completed')
	// is not double-counted: it must have completed at/after this batch started.
	var committed []core.UUID
	if err := tx.Model(&core.Job{}).
		Where("id IN ? AND status = ? AND locked_by = ? AND completed_at >= ?",
			ids, core.StatusCompleted, "", batchStart).
		Pluck("id", &committed).Error; err != nil {
		return nil, err
	}
	return committed, nil
}
