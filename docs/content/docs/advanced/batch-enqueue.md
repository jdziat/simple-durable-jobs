---
title: "Batch Enqueue"
weight: 6
---

Use `Queue.EnqueueBatch` when a producer needs to insert many independent jobs at once. It applies the same validation, JSON marshaling, job options, and enqueue middleware as single-job enqueue, then writes the surviving jobs through one storage batch operation.

```go
package main

import (
	"context"
	"log"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type EmailArgs struct {
	To      string
	Subject string
}

func main() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	q := jobs.New(store)
	ids, err := q.EnqueueBatch(ctx, []jobs.BatchEntry{
		jobs.Batch("send-email", EmailArgs{To: "a@example.com", Subject: "Welcome"}, jobs.QueueOpt("emails")),
		jobs.Batch("send-email", EmailArgs{To: "b@example.com", Subject: "Reminder"}, jobs.Delay(5*time.Minute)),
		jobs.Batch("send-email", EmailArgs{To: "c@example.com", Subject: "Receipt"}, jobs.Unique("email:c:receipt")),
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, id := range ids {
		log.Printf("enqueued job %s", id)
	}
}
```

## Semantics

- `EnqueueBatch` persists jobs with a single storage transaction through the existing storage batch primitive.
- Each `Batch(...)` entry has its own job name, arguments, and options. Options such as `QueueOpt`, `Priority`, `Retries`, `Delay`, `At`, `Timeout`, `Unique`, and `Determinism` are resolved per entry.
- A validation or marshal error on any entry fails the whole call before anything is persisted.
- Unique-key collisions are deduplicated silently by the storage layer. The returned slice still has one ID per input entry; for a deduped entry, the ID refers to the existing job represented by that unique key.
- No per-job enqueue event is emitted, matching single `Enqueue`.
- This is not a caller-supplied transaction API. It does not let application code combine job enqueueing with arbitrary database writes in the same transaction; that remains a separate transactional-enqueue gap.
