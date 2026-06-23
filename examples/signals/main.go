// Package main demonstrates durable workflow signals.
//
// Signals are buffered in storage, so an approval sent while a workflow is
// waiting is not lost. Like all jobs, handlers are at-least-once: keep real side
// effects behind jobs.Call checkpoints or make them idempotent.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sdj-signals-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			log.Printf("failed to remove temp dir: %v", err)
		}
	}()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN(filepath.Join(dir, "jobs.db"))), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatal(err)
	}

	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)

	queue.Register("prepare-request", func(ctx context.Context, request ApprovalRequest) (string, error) {
		fmt.Printf("  [call] Prepared approval packet for %s\n", request.ID)
		return "packet-" + request.ID, nil
	})

	queue.Register("approval-workflow", func(ctx context.Context, request ApprovalRequest) error {
		fmt.Printf("[workflow] Starting approval workflow %s\n", request.ID)

		packetID, err := jobs.Call[string](ctx, "prepare-request", request)
		if err != nil {
			return err
		}
		fmt.Printf("[workflow] Checkpointed preparation result: %s\n", packetID)

		if note, ok, err := jobs.CheckSignal[string](ctx, "audit-note"); err != nil {
			return err
		} else if ok {
			fmt.Printf("[workflow] Audit note signal is already buffered: %s\n", note)
		}

		fmt.Println("[workflow] Waiting for human approval...")
		// Timeout path: if no approval arrives before the deadline, ok is false.
		// A production workflow might escalate, send a reminder, or fail with
		// jobs.NoRetry so it does not retry forever.
		approved, ok, err := jobs.WaitForSignalTimeout[bool](ctx, "approval", 20*time.Second)
		if err != nil {
			return err
		}
		if !ok {
			return jobs.NoRetry(errors.New("approval timed out"))
		}
		if !approved {
			return jobs.NoRetry(errors.New("approval rejected"))
		}

		notes, err := jobs.DrainSignals[string](ctx, "audit-note")
		if err != nil {
			return err
		}
		for _, note := range notes {
			fmt.Printf("[workflow] Drained audit note: %s\n", note)
		}

		fmt.Printf("[workflow] Approved %s; final side effect would run here idempotently\n", request.ID)
		return nil
	})

	jobID, err := queue.Enqueue(ctx, "approval-workflow", ApprovalRequest{ID: "REQ-1001", Amount: 1250})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Enqueued approval workflow %s\n", jobID[:8])

	if err := queue.Signal(ctx, jobID, "audit-note", "approved by alice@example.com"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Main pre-buffered audit note before the workflow checked signals")

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(1)))
	go func() {
		fmt.Println("Starting worker...")
		_ = worker.Start(workerCtx)
	}()

	if err := waitForStatus(ctx, storage, jobID, jobs.StatusWaiting, 10*time.Second); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Main observed workflow waiting; sending audit note and approval")

	if err := queue.Signal(ctx, jobID, "audit-note", "approval confirmed while waiting"); err != nil {
		log.Fatal(err)
	}
	if err := queue.Signal(ctx, jobID, "approval", true); err != nil {
		log.Fatal(err)
	}

	if err := waitForStatus(ctx, storage, jobID, jobs.StatusCompleted, 15*time.Second); err != nil {
		log.Fatal(err)
	}

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Signals example complete: approval workflow finished successfully")
}

func waitForStatus(ctx context.Context, storage jobs.Storage, jobID jobs.UUID, want jobs.JobStatus, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := storage.GetJob(ctx, jobID)
		if err != nil {
			return err
		}
		if job != nil && job.Status == want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	job, _ := storage.GetJob(ctx, jobID)
	if job == nil {
		return fmt.Errorf("timed out waiting for job %s to reach %s; job not found", jobID, want)
	}
	return fmt.Errorf("timed out waiting for job %s to reach %s; current status is %s", jobID, want, job.Status)
}

type ApprovalRequest struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
}
