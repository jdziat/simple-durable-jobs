package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/fanout"
)

// Reproduces the EXACT call shape the hivemind fix uses:
//
//	subJob := jobs.Sub(name, args, [jobs.Timeout?])
//	jobs.FanOut(ctx, []SubJob{subJob}, fanout.WithRetries(N))
//
// and measures total sub-job executions on persistent failure.
func TestFanOutWithRetries_IgnoredBecauseSubDefault(t *testing.T) {
	for _, n := range []int{1, 5} {
		n := n
		t.Run(fmt.Sprintf("WithRetries=%d", n), func(t *testing.T) {
			queue, store := setupIntegrationQueue(t)
			var subExec atomic.Int32

			queue.Register("zz-child", func(ctx context.Context, _ struct{}) error {
				subExec.Add(1)
				return errors.New("child boom")
			})
			// Parent fans out exactly one sub-job, mirroring ExecuteChildWorkflow.
			queue.Register("zz-parent", func(ctx context.Context, _ struct{}) error {
				sub := jobs.Sub("zz-child", struct{}{}) // no Retries opt, like the fix (only Timeout sometimes)
				_, err := fanout.FanOut[json.RawMessage](ctx, []jobs.SubJob{sub}, fanout.WithRetries(n))
				return err
			})

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			parentID, err := queue.Enqueue(ctx, "zz-parent", struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			worker := queue.NewWorker()
			go func() { _ = worker.Start(ctx) }()

			deadline := time.Now().Add(28 * time.Second)
			for time.Now().Before(deadline) {
				pj, _ := store.GetJob(context.Background(), parentID)
				if pj != nil && (pj.Status == jobs.StatusFailed || pj.Status == jobs.StatusCompleted) {
					// give a beat for any final sub retry to settle
					time.Sleep(500 * time.Millisecond)
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Logf(">>> fanout.WithRetries(%d): child TOTAL_EXECUTIONS=%d (expected %d if WithRetries honored; 2 == default-2 means IGNORED)",
				n, subExec.Load(), n)
		})
	}
}
