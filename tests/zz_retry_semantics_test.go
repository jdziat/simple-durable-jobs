package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
)

// Empirically determine: for jobs.Retries(N), how many TOTAL executions happen
// when the handler ALWAYS fails? This pins the ground-truth semantics that the
// hivemind ChildWorkflowOptions off-by-one fix depends on.
func TestRetrySemantics_TotalRunsForAlwaysFail(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3} {
		n := n
		t.Run(fmt.Sprintf("Retries=%d", n), func(t *testing.T) {
			queue, store := setupIntegrationQueue(t)
			var attempts atomic.Int32
			queue.Register("always-fail-zz", func(ctx context.Context, _ struct{}) error {
				attempts.Add(1)
				return errors.New("boom")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			jobID, err := queue.Enqueue(ctx, "always-fail-zz", struct{}{}, jobs.Retries(n))
			if err != nil {
				t.Fatal(err)
			}
			worker := queue.NewWorker()
			go func() { _ = worker.Start(ctx) }()
			for i := 0; i < 250; i++ {
				job, _ := store.GetJob(context.Background(), jobID)
				if job != nil && job.Status == jobs.StatusFailed {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			job, _ := store.GetJob(context.Background(), jobID)
			status, mr, at := "<nil>", -1, int32(-1)
			if job != nil {
				status = string(job.Status)
				mr = job.MaxRetries
				at = int32(job.Attempt)
			}
			t.Logf(">>> Retries(%d): TOTAL_EXECUTIONS=%d final_status=%s job.MaxRetries=%d job.Attempt=%d",
				n, attempts.Load(), status, mr, at)
		})
	}
}
