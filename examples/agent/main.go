// Package main demonstrates a durable AI-agent style workflow.
//
// The "LLM" calls here are mocked and require no API key. In a real agent,
// durability matters because process restarts can resume from the last
// checkpoint, timer deadlines survive restarts, and approval signals are stored
// until the workflow consumes them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sdj-agent-*")
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

	queue.Register("mock-llm", func(ctx context.Context, call LLMCall) (Completion, error) {
		fmt.Printf("  [llm] %s iteration %d\n", call.TaskID, call.Iteration)
		switch call.Iteration {
		case 0:
			return Completion{Text: "Need product policy context.", NeedMoreResearch: true}, nil
		case 1:
			return Completion{Text: "Need customer history context.", NeedMoreResearch: true}, nil
		default:
			return Completion{Text: "Draft response with refund eligibility and next action.", NeedMoreResearch: false}, nil
		}
	})

	queue.Register("act-on-plan", func(ctx context.Context, args ActArgs) error {
		fmt.Printf("  [act] Sending final answer for %s: %s\n", args.TaskID, args.Plan)
		return nil
	})

	queue.Register("agent-workflow", func(ctx context.Context, request AgentRequest) error {
		fmt.Printf("[agent] Starting task %s\n", request.TaskID)
		observations := make([]string, 0, request.MaxIterations)

		for i := 0; i < request.MaxIterations; i++ {
			// Each jobs.Call records a distinct checkpoint in this bounded loop.
			// On replay, completed LLM calls are read from the checkpoint table
			// instead of being executed again.
			completion, err := jobs.Call[Completion](ctx, "mock-llm", LLMCall{
				TaskID:    request.TaskID,
				Question:  request.Question,
				Iteration: i,
			})
			if err != nil {
				return err
			}
			observations = append(observations, completion.Text)
			fmt.Printf("[agent] Iteration %d result: %s\n", i, completion.Text)

			if !completion.NeedMoreResearch {
				break
			}

			// Sleep is durable: the original deadline is checkpointed, the job
			// does not hold a worker slot while waiting, and replay does not
			// restart the duration. Wakeups are coarse, currently about a 5s
			// polling backstop plus normal dispatch polling, so this is for
			// pacing/backoff rather than precise timing.
			fmt.Println("[agent] Pacing the research loop with jobs.Sleep...")
			if err := jobs.Sleep(ctx, 2*time.Second); err != nil {
				return err
			}
		}

		plan := strings.Join(observations, " | ")
		fmt.Println("[agent] Waiting for human approval before acting...")
		approved, ok, err := jobs.WaitForSignalTimeout[bool](ctx, "approval", 15*time.Second)
		if err != nil {
			return err
		}
		if !ok {
			return jobs.NoRetry(errors.New("agent approval timed out"))
		}
		if !approved {
			return jobs.NoRetry(errors.New("agent action rejected"))
		}

		_, err = jobs.Call[any](ctx, "act-on-plan", ActArgs{
			TaskID: request.TaskID,
			Plan:   plan,
		})
		return err
	})

	jobID, err := queue.Enqueue(ctx, "agent-workflow", AgentRequest{
		TaskID:        "CASE-42",
		Question:      "Can this customer receive a refund?",
		MaxIterations: 3,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Enqueued agent workflow %s\n", jobID[:8])

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(1)))
	go func() {
		fmt.Println("Starting worker...")
		_ = worker.Start(workerCtx)
	}()

	go func() {
		// This may arrive before the workflow reaches the approval gate. Signals
		// are durable and FIFO per job/name, so the approval is still consumed.
		time.Sleep(8 * time.Second)
		fmt.Println("Main sending human approval signal")
		if err := jobs.Signal(ctx, queue, jobID, "approval", true); err != nil {
			log.Printf("failed to send approval: %v", err)
		}
	}()

	if err := waitForStatus(ctx, storage, jobID, jobs.StatusCompleted, 25*time.Second); err != nil {
		log.Fatal(err)
	}

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Agent example complete: durable agent reached the final action")
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
		time.Sleep(200 * time.Millisecond)
	}
	job, _ := storage.GetJob(ctx, jobID)
	if job == nil {
		return fmt.Errorf("timed out waiting for job %s to reach %s; job not found", jobID, want)
	}
	return fmt.Errorf("timed out waiting for job %s to reach %s; current status is %s", jobID, want, job.Status)
}

type AgentRequest struct {
	TaskID        string `json:"task_id"`
	Question      string `json:"question"`
	MaxIterations int    `json:"max_iterations"`
}

type LLMCall struct {
	TaskID    string `json:"task_id"`
	Question  string `json:"question"`
	Iteration int    `json:"iteration"`
}

type Completion struct {
	Text             string `json:"text"`
	NeedMoreResearch bool   `json:"need_more_research"`
}

type ActArgs struct {
	TaskID string `json:"task_id"`
	Plan   string `json:"plan"`
}
