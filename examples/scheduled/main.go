// Package main demonstrates scheduled and recurring jobs.
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
	db, err := gorm.Open(sqlite.Open("scheduled.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(context.Background()); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)

	// Register job handlers
	queue.Register("health-check", func(ctx context.Context, _ struct{}) error {
		fmt.Printf("[%s] Health check: OK\n", time.Now().Format("15:04:05"))
		return nil
	})

	queue.Register("cleanup-temp-files", func(ctx context.Context, _ struct{}) error {
		fmt.Printf("[%s] Cleaning up temporary files...\n", time.Now().Format("15:04:05"))
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("[%s] Cleanup complete\n", time.Now().Format("15:04:05"))
		return nil
	})

	queue.Register("send-daily-report", func(ctx context.Context, _ struct{}) error {
		fmt.Printf("[%s] Generating daily report...\n", time.Now().Format("15:04:05"))
		time.Sleep(200 * time.Millisecond)
		fmt.Printf("[%s] Daily report sent\n", time.Now().Format("15:04:05"))
		return nil
	})

	queue.Register("backup-database", func(ctx context.Context, _ struct{}) error {
		fmt.Printf("[%s] Starting database backup...\n", time.Now().Format("15:04:05"))
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("[%s] Backup complete\n", time.Now().Format("15:04:05"))
		return nil
	})

	// Schedule recurring jobs

	// Health check every 5 seconds
	queue.Schedule("health-check", jobs.Every(5*time.Second))
	fmt.Println("Scheduled: health-check (every 5 seconds)")

	// Cleanup every 10 seconds
	queue.Schedule("cleanup-temp-files", jobs.Every(10*time.Second))
	fmt.Println("Scheduled: cleanup-temp-files (every 10 seconds)")

	// For demo purposes, using short intervals
	// In production, you'd use Daily, Weekly, or Cron schedules:
	//
	// Daily report at 9:00 AM
	// queue.Schedule("send-daily-report", jobs.Daily(9, 0))
	//
	// Weekly backup on Sunday at 2:00 AM
	// queue.Schedule("backup-database", jobs.Weekly(time.Sunday, 2, 0))
	//
	// Using cron expression (every hour at minute 0)
	// queue.Schedule("health-check", jobs.Cron("0 * * * *"))

	// For demo, schedule these more frequently
	queue.Schedule("send-daily-report", jobs.Every(15*time.Second))
	fmt.Println("Scheduled: send-daily-report (every 15 seconds for demo)")

	queue.Schedule("backup-database", jobs.Every(20*time.Second))
	fmt.Println("Scheduled: backup-database (every 20 seconds for demo)")

	// Start worker with scheduler enabled
	ctx, cancel := context.WithCancel(context.Background())
	worker := queue.NewWorker(
		jobs.WorkerQueue("default", jobs.Concurrency(2)),
		jobs.WithScheduler(true), // Enable the scheduler
	)

	go func() {
		fmt.Println("\nStarting worker with scheduler...")
		worker.Start(ctx)
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Run for 60 seconds then exit
	go func() {
		time.Sleep(60 * time.Second)
		sigCh <- syscall.SIGTERM
	}()

	fmt.Println("Running for 60 seconds (Ctrl+C to stop early)...")
	<-sigCh

	fmt.Println("\nShutting down...")
	cancel()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done!")
}
