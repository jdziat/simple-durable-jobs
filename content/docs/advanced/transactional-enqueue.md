---
title: "Transactional Enqueue"
weight: 65
toc: true
---

Use transactional enqueue when a producer must write application data and create
a job atomically. `Queue.EnqueueTx` and `Queue.EnqueueBatchTx` persist normal
`jobs` rows through a caller-supplied `*gorm.DB` transaction; the library does
not commit or roll back that transaction.

```go
tx := db.Begin()
if tx.Error != nil {
	return tx.Error
}
defer tx.Rollback()

order := Order{ID: orderID, Status: "pending"}
if err := tx.WithContext(ctx).Create(&order).Error; err != nil {
	return err
}

jobID, err := q.EnqueueTx(ctx, tx, "orders.process", ProcessOrderArgs{
	OrderID: order.ID,
}, jobs.Unique("order:"+order.ID))
if err != nil {
	return err
}

if err := tx.Commit().Error; err != nil {
	return err
}
log.Printf("created job %s", jobID)
```

The visibility guarantee is the same as the surrounding database transaction:
the job becomes visible only if the caller commits, and it disappears on
rollback. PostgreSQL and MySQL provide the full transactional guarantee. SQLite
works under its single-writer model; use a WAL/busy-timeout DSN such as
`jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate` for concurrent
producers.

Transactional enqueue is an optional storage capability implemented by
`GormStorage`. Custom storage backends that do not implement it return
`jobs.ErrStorageNoTxEnqueue`, so producers can fall back to a regular enqueue or
their own outbox table.
