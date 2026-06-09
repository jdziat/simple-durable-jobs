// Package main demonstrates transactional enqueue with application data.
//
// EnqueueTx writes the jobs row through the caller's GORM transaction. The job
// becomes visible only if that transaction commits, and disappears on rollback.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sdj-tx-enqueue-*")
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
	if err := db.AutoMigrate(&Order{}); err != nil {
		log.Fatal(err)
	}

	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)
	queue.Register("fulfill-order", func(ctx context.Context, args FulfillOrderArgs) error {
		fmt.Printf("[worker] Fulfilling order %s\n", args.OrderID)
		// Handlers are at-least-once. This update is intentionally idempotent:
		// setting the same status twice is safe if the job is retried.
		return db.WithContext(ctx).Model(&Order{}).
			Where("id = ?", args.OrderID).
			Update("status", "fulfilled").Error
	})

	committedJobID, err := createOrderAndJob(ctx, db, queue, Order{
		ID:     "ORD-COMMIT",
		Email:  "alice@example.com",
		Status: "created",
	}, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Committed order and job %s\n", committedJobID[:8])

	rolledBackJobID, err := createOrderAndJob(ctx, db, queue, Order{
		ID:     "ORD-ROLLBACK",
		Email:  "bob@example.com",
		Status: "created",
	}, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Rolled back transaction that had staged job %s\n", rolledBackJobID[:8])

	printCounts(ctx, db, "After commit and rollback")

	rolledBackJob, err := storage.GetJob(ctx, rolledBackJobID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Rolled-back job visible after rollback: %v\n", rolledBackJob != nil)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(1)))
	go func() {
		fmt.Println("Starting worker...")
		_ = worker.Start(workerCtx)
	}()

	if err := waitForStatus(ctx, storage, committedJobID, jobs.StatusCompleted, 10*time.Second); err != nil {
		log.Fatal(err)
	}

	var order Order
	if err := db.WithContext(ctx).First(&order, "id = ?", "ORD-COMMIT").Error; err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Committed order final status: %s\n", order.Status)
	printCounts(ctx, db, "After worker")

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Transactional enqueue example complete: only the committed job ran")
}

func createOrderAndJob(ctx context.Context, db *gorm.DB, queue *jobs.Queue, order Order, commit bool) (string, error) {
	tx := db.Begin()
	if tx.Error != nil {
		return "", tx.Error
	}
	defer tx.Rollback()

	if err := tx.WithContext(ctx).Create(&order).Error; err != nil {
		return "", err
	}

	jobID, err := queue.EnqueueTx(ctx, tx, "fulfill-order", FulfillOrderArgs{OrderID: order.ID}, jobs.Unique("order:"+order.ID))
	if err != nil {
		return "", err
	}

	if !commit {
		return jobID, tx.Rollback().Error
	}
	return jobID, tx.Commit().Error
}

func printCounts(ctx context.Context, db *gorm.DB, label string) {
	var orderCount int64
	var jobCount int64
	if err := db.WithContext(ctx).Model(&Order{}).Count(&orderCount).Error; err != nil {
		log.Fatal(err)
	}
	if err := db.WithContext(ctx).Model(&jobs.Job{}).Count(&jobCount).Error; err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: orders=%d jobs=%d\n", label, orderCount, jobCount)
}

func waitForStatus(ctx context.Context, storage jobs.Storage, jobID string, want jobs.JobStatus, timeout time.Duration) error {
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

type Order struct {
	ID     string `gorm:"primaryKey" json:"id"`
	Email  string `json:"email"`
	Status string `json:"status"`
}

type FulfillOrderArgs struct {
	OrderID string `json:"order_id"`
}
