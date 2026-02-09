// Package main demonstrates basic job queue usage.
package main

import (
	"context"
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
	// Setup database
	db, err := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Create storage and queue
	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(context.Background()); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)

	// Register a simple job handler
	queue.Register("send-email", func(ctx context.Context, args EmailArgs) error {
		fmt.Printf("Sending email to %s: %s\n", args.To, args.Subject)
		// Simulate email sending
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Email sent to %s\n", args.To)
		return nil
	})

	// Register hooks for observability
	queue.OnJobStart(func(ctx context.Context, job *jobs.Job) {
		fmt.Printf("[START] Job %s (%s)\n", job.ID[:8], job.Type)
	})

	queue.OnJobComplete(func(ctx context.Context, job *jobs.Job) {
		fmt.Printf("[DONE] Job %s completed\n", job.ID[:8])
	})

	queue.OnJobFail(func(ctx context.Context, job *jobs.Job, err error) {
		fmt.Printf("[FAIL] Job %s failed: %v\n", job.ID[:8], err)
	})

	// Enqueue some jobs
	ctx := context.Background()

	emails := []EmailArgs{
		{To: "alice@example.com", Subject: "Welcome!"},
		{To: "bob@example.com", Subject: "Your order shipped"},
		{To: "charlie@example.com", Subject: "Password reset"},
	}

	for _, email := range emails {
		id, err := queue.Enqueue(ctx, "send-email", email)
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("Enqueued job %s\n", id[:8])
	}

	// Enqueue a delayed job
	id, _ := queue.Enqueue(ctx, "send-email", EmailArgs{
		To:      "delayed@example.com",
		Subject: "This was delayed",
	}, jobs.Delay(5*time.Second))
	fmt.Printf("Enqueued delayed job %s (runs in 5s)\n", id[:8])

	// Enqueue a high-priority job
	id, _ = queue.Enqueue(ctx, "send-email", EmailArgs{
		To:      "urgent@example.com",
		Subject: "URGENT: Action required",
	}, jobs.Priority(100))
	fmt.Printf("Enqueued high-priority job %s\n", id[:8])

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(2)))

	go func() {
		fmt.Println("Starting worker...")
		worker.Start(workerCtx)
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait a bit then shutdown
	go func() {
		time.Sleep(10 * time.Second)
		sigCh <- syscall.SIGTERM
	}()

	<-sigCh
	fmt.Println("\nShutting down...")
	cancel()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done!")
}

type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}
