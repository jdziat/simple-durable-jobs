---
title: "Error Handling"
weight: 9
---

### `NoRetry(err error) error`

Wraps an error to indicate the job should not be retried.

### `RetryAfter(d time.Duration, err error) error`

Wraps an error to indicate the job should be retried after a specific duration.
