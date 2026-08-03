package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDocumentedWorkerDefaultsMatchTheDocs pins the worker defaults that the
// documentation prints as literal numbers:
//
//	docs/content/docs/api-reference/worker.md      (batch size, poll interval, clamp range)
//	docs/content/docs/production-ops.md            (batch size)
//	docs/content/docs/advanced/batch-dequeue.md    (batch size)
//	docs/content/docs/benchmarks.md                (batch size, poll interval)
//
// The batch-size default was raised from 10 to 50 in 21a5546 and all four pages
// kept printing 10 for six weeks, because a default can change without anything
// failing. If you change a value here, update the pages above in the same commit
// — and if you change the batch size, note that benchmarks.md's published tables
// were measured at 10 and would need re-running, not just re-wording.
func TestDocumentedWorkerDefaultsMatchTheDocs(t *testing.T) {
	w := NewWorker(nil)
	require.NotNil(t, w, "NewWorker(nil) must build a config we can inspect")

	require.Equal(t, 50, w.config.DequeueBatchSize, "documented default dequeue batch size")
	require.Equal(t, 100*time.Millisecond, w.config.PollInterval, "documented default poll interval")
	require.Equal(t, 30*time.Second, w.config.DrainTimeout, "documented default drain timeout")

	// The documented clamp range [1, 1000] and floor.
	require.Equal(t, 50*time.Millisecond, minPollInterval, "documented poll-interval floor")
	require.Equal(t, 1000, maxDequeueBatch, "documented upper clamp for WithDequeueBatchSize")

	clamp := func(n int) int {
		c := WorkerConfig{}
		WithDequeueBatchSize(n).ApplyWorker(&c)
		return c.DequeueBatchSize
	}
	require.Equal(t, 1, clamp(0), "documented: values below 1 clamp up to 1")
	require.Equal(t, 1, clamp(-5), "documented: values below 1 clamp up to 1")
	require.Equal(t, 1000, clamp(99999), "documented: values above 1000 clamp down to 1000")

	// worker.md and batch-dequeue.md both now state that the claim is additionally
	// capped at the worker's free concurrency slots, so the default concurrency of
	// 10 bounds the effective per-poll claim below the batch size of 50.
	require.Equal(t, 10, w.config.Queues["default"],
		"documented default concurrency for the implicit `default` queue")
	require.Less(t, w.config.Queues["default"], w.config.DequeueBatchSize,
		"the docs explain the effective claim is concurrency-bound; that only holds while concurrency < batch size")
}
