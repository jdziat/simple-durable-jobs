---
title: "Transactional Checkpoints"
description: "Commit a phase checkpoint in the same transaction as the business effect it protects."
weight: 64
toc: true
---

Use transactional checkpoints when a workflow phase writes application data and
the checkpoint that marks that phase complete must commit atomically with that
write. `SavePhaseCheckpointTx` persists the same phase checkpoint as
`SavePhaseCheckpoint`, but through a caller-owned `*gorm.DB` transaction; the
library does not commit or roll back that transaction.

## The window it closes

Plain `SavePhaseCheckpoint` is durable, but it is a separate write from your
business side effect:

1. The handler writes the business effect.
2. The effect commits.
3. The process crashes before `SavePhaseCheckpoint` commits.
4. The job replays, `LoadPhaseCheckpoint` misses, and the handler executes the
   effect again.

That is the normal at-least-once contract. For naturally idempotent effects,
this is fine and the plain checkpoint API is simpler.

For non-idempotent phases that live in the same database as the jobs storage,
put the effect and checkpoint in one transaction. Then either both commits are
visible on replay, or neither is.

## Example

```go
type OrderLedgerEntry struct {
	ID      uint `gorm:"primaryKey"`
	OrderID string
	Kind    string
}

queue.Register("orders.capture", func(ctx context.Context, orderID string) error {
	if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "ledger-entry"); ok {
		return nil
	}

	tx := db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer tx.Rollback()

	entry := OrderLedgerEntry{OrderID: orderID, Kind: "captured"}
	if err := tx.Create(&entry).Error; err != nil {
		return err
	}

	if err := jobs.SavePhaseCheckpointTx(ctx, tx, "ledger-entry", "done"); err != nil {
		return err
	}

	return tx.Commit().Error
})
```

On the first run, the ledger row and the phase checkpoint are written through
the same transaction. If the process exits before `Commit`, neither row is
visible and the retry runs the phase again. If `Commit` succeeds and the handler
crashes later, replay loads the checkpoint with `LoadPhaseCheckpoint`, skips the
phase, and does not insert a second ledger entry.

## Constraints

- This makes only **this phase's effect** exactly-once. Other phases, `Call()`
  steps, and the handler as a whole remain at-least-once and must stay
  idempotent.
- The transaction must be for the same GORM database used by the job storage.
  If it belongs to a different database, the checkpoint is committed somewhere
  replay never reads — the phase will silently re-run on every retry, and the
  library cannot detect this.
- This is implemented by `GormStorage`. Custom storage backends that do not
  implement the optional capability return `jobs.ErrStorageNoTxCheckpoint`.
- The checkpoint is invisible to replay until the transaction commits. That is
  the guarantee: rolled-back effects do not leave checkpoints behind.
- `SavePhaseCheckpointTx` must be called from inside a job handler. Outside a
  handler it returns an error instead of silently skipping the write, because a
  skipped transactional checkpoint would break atomicity.

## When not to bother

If the effect is already idempotent, such as an upsert guarded by a unique key,
plain `SavePhaseCheckpoint` is usually enough and keeps the handler smaller.
