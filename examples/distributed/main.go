// Package main demonstrates distributed job processing with multiple workers.
//
// In production, you would run multiple instances of this worker connecting
// to the same database (PostgreSQL recommended for concurrent access).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// Parse command line flags
	workerID := flag.String("worker", "worker-1", "Worker ID")
	enqueue := flag.Bool("enqueue", false, "Enqueue jobs instead of processing")
	numJobs := flag.Int("jobs", 10, "Number of jobs to enqueue")
	flag.Parse()

	// Setup database (use PostgreSQL in production for better concurrency)
	db, err := gorm.Open(sqlite.Open("distributed.db?_journal_mode=WAL"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(context.Background()); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)

	// Register job handler
	queue.Register("process-item", func(ctx context.Context, item Item) error {
		fmt.Printf("[%s] Processing item %d: %s\n", *workerID, item.ID, item.Name)
		// Simulate varying processing times
		time.Sleep(time.Duration(100+item.ID%200) * time.Millisecond)
		fmt.Printf("[%s] Completed item %d\n", *workerID, item.ID)
		return nil
	})

	// Register hooks to track which worker processes each job
	queue.OnJobStart(func(ctx context.Context, job *jobs.Job) {
		fmt.Printf("[%s] Started job %s\n", *workerID, job.ID[:8])
	})

	queue.OnJobComplete(func(ctx context.Context, job *jobs.Job) {
		fmt.Printf("[%s] Completed job %s\n", *workerID, job.ID[:8])
	})

	ctx := context.Background()

	if *enqueue {
		// Enqueue mode: add jobs to the queue
		fmt.Printf("Enqueueing %d jobs...\n", *numJobs)
		for i := 0; i < *numJobs; i++ {
			_, err := queue.Enqueue(ctx, "process-item", Item{
				ID:   i,
				Name: fmt.Sprintf("Item-%d", i),
			})
			if err != nil {
				log.Printf("Failed to enqueue: %v", err)
			}
		}
		fmt.Printf("Enqueued %d jobs\n", *numJobs)
		return
	}

	// Worker mode: process jobs
	workerCtx, cancel := context.WithCancel(ctx)

	// Create worker with unique ID for distributed locking
	worker := queue.NewWorker(
		jobs.WorkerQueue("default", jobs.Concurrency(3)),
	)

	go func() {
		fmt.Printf("[%s] Starting worker...\n", *workerID)
		worker.Start(workerCtx)
	}()

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	fmt.Printf("\n[%s] Shutting down...\n", *workerID)
	cancel()
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("[%s] Done!\n", *workerID)
}

type Item struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

/*
Usage:

1. First, enqueue some jobs:
   go run main.go -enqueue -jobs 100

2. Then start multiple workers in different terminals:
   Terminal 1: go run main.go -worker worker-1
   Terminal 2: go run main.go -worker worker-2
   Terminal 3: go run main.go -worker worker-3

The jobs will be distributed across all workers. Each job is processed
by exactly one worker due to the locking mechanism.

Note: SQLite has limited concurrency. For production distributed
systems, use PostgreSQL:

   db, err := gorm.Open(postgres.Open("host=localhost user=app dbname=jobs"), &gorm.Config{})
*/
