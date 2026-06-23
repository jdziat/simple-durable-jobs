---
title: "Testing"
weight: 18
---

The `jobstest` package provides test fixtures and enqueue assertions for code
that produces or consumes jobs. It is imported directly from the module root:

```go
import "github.com/jdziat/simple-durable-jobs/v4/jobstest"
```

## Queue fixture

Use `jobstest.NewQueue` when a test needs a real queue without managing a
database fixture. It creates a unique SQLite database under `t.TempDir()`, opens
it with WAL, `busy_timeout`, and `txlock=immediate`, migrates the schema, and
closes the database with `t.Cleanup`.

```go
package email_test

import (
	"context"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/jobstest"
)

type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func TestProducerEnqueuesEmail(t *testing.T) {
	t.Parallel()

	q := jobstest.NewQueue(t)
	q.Register("send-email", func(context.Context, EmailArgs) error {
		return nil
	})

	_, err := q.Enqueue(context.Background(), "send-email", EmailArgs{
		To:      "user@example.com",
		Subject: "Welcome",
	})
	if err != nil {
		t.Fatal(err)
	}

	jobstest.RequireEnqueued(t, q, "send-email",
		jobstest.WithArgs(EmailArgs{To: "user@example.com", Subject: "Welcome"}),
	)
}
```

If a test also needs direct access to storage, use `NewQueueWithStorage`:

```go
q, store := jobstest.NewQueueWithStorage(t)
_ = q
_ = store
```

Each fixture owns its own database, so it is safe to use from parallel tests.

## Assertions

`AssertEnqueued` checks that at least one matching job was inserted and returns
the first match:

```go
job := jobstest.AssertEnqueued(t, q, "send-email")
if job != nil {
	t.Log(job.ID)
}
```

`RequireEnqueued` is the fatal variant:

```go
job := jobstest.RequireEnqueued(t, q, "send-email")
_ = job
```

`AssertEnqueuedCount` checks an exact count and returns all matches:

```go
jobs := jobstest.AssertEnqueuedCount(t, q, "send-email", 2)
_ = jobs
```

`AssertNotEnqueued` checks that no matching job was inserted:

```go
jobstest.AssertNotEnqueued(t, q, "send-email")
```

Assertions look across pending, running, completed, failed, retrying, waiting,
cancelled, and paused jobs. They verify that a job was inserted, regardless of
whether a worker has already processed it.

## Matchers

Use `InQueue` to restrict a match to a queue:

```go
jobstest.RequireEnqueued(t, q, "send-email", jobstest.InQueue("mail"))
```

Use `WithArgs` to match JSON arguments. The matcher marshals the expected value
the same way `Queue.Enqueue` does, decodes both JSON payloads, and compares the
decoded values:

```go
jobstest.RequireEnqueued(t, q, "send-email",
	jobstest.InQueue("mail"),
	jobstest.WithArgs(EmailArgs{To: "user@example.com", Subject: "Welcome"}),
)
```

## Current limits

`jobstest` does not provide an injectable clock or time-skipping helpers. For
time-sensitive behavior, prefer short real durations, bounded contexts, and the
isolated `jobstest` queue/storage fixtures so tests stay fast without depending
on shared wall-clock state.
