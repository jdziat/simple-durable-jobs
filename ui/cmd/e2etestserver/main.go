package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/ui"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	port := flag.String("port", "0", "Port to listen on (0 for random)")
	flag.Parse()

	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Limit connections so all use the same in-memory database
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Failed to get sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	store := storage.NewGormStorage(db)
	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	if err := seedE2EData(context.Background(), store, db); err != nil {
		log.Fatalf("Failed to seed data: %v", err)
	}

	q := queue.New(store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := ui.Handler(store, ui.WithQueue(q), ui.WithContext(ctx))

	ln, err := net.Listen("tcp", "127.0.0.1:"+*port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	addr := ln.Addr().(*net.TCPAddr)
	fmt.Fprintf(os.Stdout, "E2E_BASE_URL=http://127.0.0.1:%d\n", addr.Port)
	os.Stdout.Sync()

	srv := &http.Server{Handler: handler}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func seedE2EData(ctx context.Context, store *storage.GormStorage, db *gorm.DB) error {
	now := time.Now()

	// Pending jobs (5 total: 3 in default, 2 in emails)
	pendingJobs := []*core.Job{
		{ID: "e2e-pending-001", Type: "SendEmail", Queue: "default", Status: core.StatusPending, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "user@example.com"}), CreatedAt: now.Add(-10 * time.Minute)},
		{ID: "e2e-pending-002", Type: "GenerateReport", Queue: "default", Status: core.StatusPending, Priority: 3, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 42}), CreatedAt: now.Add(-9 * time.Minute)},
		{ID: "e2e-pending-003", Type: "ProcessOrder", Queue: "default", Status: core.StatusPending, Priority: 7, MaxRetries: 5, Args: mustJSON(map[string]any{"order_id": 1001}), CreatedAt: now.Add(-8 * time.Minute)},
		{ID: "e2e-pending-004", Type: "SendEmail", Queue: "emails", Status: core.StatusPending, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "admin@example.com"}), CreatedAt: now.Add(-7 * time.Minute)},
		{ID: "e2e-pending-005", Type: "SendNotification", Queue: "emails", Status: core.StatusPending, Priority: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"channel": "slack"}), CreatedAt: now.Add(-6 * time.Minute)},
	}

	// Running jobs (3 total)
	runningStart1 := now.Add(-5 * time.Minute)
	runningStart2 := now.Add(-3 * time.Minute)
	runningStart3 := now.Add(-2 * time.Minute)
	lockedUntil := now.Add(5 * time.Minute)
	runningJobs := []*core.Job{
		{ID: "e2e-running-001", Type: "GenerateReport", Queue: "default", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 99}), CreatedAt: now.Add(-6 * time.Minute), StartedAt: &runningStart1, LockedBy: "worker-1", LockedUntil: &lockedUntil},
		{ID: "e2e-running-002", Type: "ProcessOrder", Queue: "default", Status: core.StatusRunning, Priority: 8, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"order_id": 2002}), CreatedAt: now.Add(-4 * time.Minute), StartedAt: &runningStart2, LockedBy: "worker-2", LockedUntil: &lockedUntil},
		{ID: "e2e-running-003", Type: "SendEmail", Queue: "emails", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "test@example.com"}), CreatedAt: now.Add(-3 * time.Minute), StartedAt: &runningStart3, LockedBy: "worker-1", LockedUntil: &lockedUntil},
	}

	// Completed jobs (10 total)
	completedJobs := make([]*core.Job, 10)
	for i := 0; i < 10; i++ {
		createdAt := now.Add(-time.Duration(24-i) * time.Hour)
		startedAt := createdAt.Add(5 * time.Second)
		completedAt := startedAt.Add(time.Duration(10+i*5) * time.Second)
		queueName := "default"
		if i%3 == 0 {
			queueName = "emails"
		}
		jobType := "ProcessOrder"
		if i%2 == 0 {
			jobType = "GenerateReport"
		}
		completedJobs[i] = &core.Job{
			ID:          fmt.Sprintf("e2e-completed-%03d", i+1),
			Type:        jobType,
			Queue:       queueName,
			Status:      core.StatusCompleted,
			Priority:    5,
			Attempt:     1,
			MaxRetries:  3,
			Args:        mustJSON(map[string]any{"batch": i + 1}),
			CreatedAt:   createdAt,
			StartedAt:   &startedAt,
			CompletedAt: &completedAt,
		}
	}

	// Failed jobs (5 total)
	failedErrors := []string{
		"connection timeout after 30s",
		"record not found: user_id=12345",
		"rate limit exceeded, retry after 60s",
		"invalid input: email format",
		"external API returned 503",
	}
	failedJobs := make([]*core.Job, 5)
	for i := 0; i < 5; i++ {
		createdAt := now.Add(-time.Duration(12-i) * time.Hour)
		startedAt := createdAt.Add(5 * time.Second)
		completedAt := startedAt.Add(30 * time.Second)
		failedJobs[i] = &core.Job{
			ID:          fmt.Sprintf("e2e-failed-%03d", i+1),
			Type:        "SendEmail",
			Queue:       "default",
			Status:      core.StatusFailed,
			Priority:    5,
			Attempt:     3,
			MaxRetries:  3,
			Args:        mustJSON(map[string]any{"to": fmt.Sprintf("user%d@example.com", i+1)}),
			LastError:   failedErrors[i],
			CreatedAt:   createdAt,
			StartedAt:   &startedAt,
			CompletedAt: &completedAt,
		}
	}

	// Paused jobs (2 total)
	pausedJobs := []*core.Job{
		{ID: "e2e-paused-001", Type: "SendEmail", Queue: "default", Status: core.StatusPaused, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "paused@example.com"}), CreatedAt: now.Add(-15 * time.Minute)},
		{ID: "e2e-paused-002", Type: "GenerateReport", Queue: "emails", Status: core.StatusPaused, Priority: 3, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 77}), CreatedAt: now.Add(-14 * time.Minute)},
	}

	// Cancelled job (1 total)
	cancelledStart := now.Add(-20 * time.Minute)
	cancelledComplete := cancelledStart.Add(10 * time.Second)
	cancelledJobs := []*core.Job{
		{ID: "e2e-cancelled-001", Type: "ProcessOrder", Queue: "default", Status: core.StatusCancelled, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"order_id": 9999}), LastError: "cancelled by user", CreatedAt: now.Add(-25 * time.Minute), StartedAt: &cancelledStart, CompletedAt: &cancelledComplete},
	}

	// Workflow: parent + 3 children + FanOut
	wfCreated := now.Add(-2 * time.Hour)
	wfStarted := wfCreated.Add(5 * time.Second)
	fanOutID := "e2e-fanout-001"
	workflowJobs := []*core.Job{
		{ID: "e2e-workflow-root", Type: "BatchProcess", Queue: "default", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 1, Args: mustJSON(map[string]any{"batch_size": 3}), CreatedAt: wfCreated, StartedAt: &wfStarted, LockedBy: "worker-1", LockedUntil: &lockedUntil},
	}
	rootID := "e2e-workflow-root"
	childStatuses := []core.JobStatus{core.StatusCompleted, core.StatusRunning, core.StatusPending}
	for i := 0; i < 3; i++ {
		childCreated := wfCreated.Add(time.Duration(i+1) * 10 * time.Second)
		child := &core.Job{
			ID:          fmt.Sprintf("e2e-workflow-child-%03d", i+1),
			Type:        "ProcessItem",
			Queue:       "default",
			Status:      childStatuses[i],
			Priority:    5,
			Attempt:     1,
			MaxRetries:  3,
			Args:        mustJSON(map[string]any{"item_index": i}),
			CreatedAt:   childCreated,
			ParentJobID: &rootID,
			RootJobID:   &rootID,
			FanOutID:    &fanOutID,
			FanOutIndex: i,
		}
		if childStatuses[i] == core.StatusCompleted || childStatuses[i] == core.StatusRunning {
			started := childCreated.Add(2 * time.Second)
			child.StartedAt = &started
			if childStatuses[i] == core.StatusCompleted {
				completed := started.Add(15 * time.Second)
				child.CompletedAt = &completed
			} else {
				child.LockedBy = "worker-2"
				child.LockedUntil = &lockedUntil
			}
		}
		workflowJobs = append(workflowJobs, child)
	}

	// Enqueue all jobs
	allJobs := make([]*core.Job, 0, 30)
	allJobs = append(allJobs, pendingJobs...)
	allJobs = append(allJobs, runningJobs...)
	allJobs = append(allJobs, completedJobs...)
	allJobs = append(allJobs, failedJobs...)
	allJobs = append(allJobs, pausedJobs...)
	allJobs = append(allJobs, cancelledJobs...)
	allJobs = append(allJobs, workflowJobs...)

	for _, job := range allJobs {
		if err := store.Enqueue(ctx, job); err != nil {
			return fmt.Errorf("failed to enqueue job %s: %w", job.ID, err)
		}
	}

	// Create FanOut record directly via GORM
	fanOut := &core.FanOut{
		ID:             fanOutID,
		ParentJobID:    rootID,
		TotalCount:     3,
		CompletedCount: 1,
		FailedCount:    0,
		CancelledCount: 0,
		Strategy:       core.FanOutStrategy("fail_fast"),
		Threshold:      1.0,
		Status:         core.FanOutStatus("pending"),
		CancelOnFail:   true,
		CreatedAt:      wfCreated,
		UpdatedAt:      wfCreated,
	}
	if err := db.WithContext(ctx).Create(fanOut).Error; err != nil {
		return fmt.Errorf("failed to create fan-out: %w", err)
	}

	// Add checkpoints to first 2 completed jobs
	for i := 0; i < 2; i++ {
		for j := 0; j < 2; j++ {
			cp := &core.Checkpoint{
				ID:        fmt.Sprintf("e2e-checkpoint-%03d-%03d", i+1, j+1),
				JobID:     fmt.Sprintf("e2e-completed-%03d", i+1),
				CallIndex: j,
				CallType:  "http.Get",
				Result:    mustJSON(map[string]any{"status": 200, "body": "OK"}),
				CreatedAt: completedJobs[i].CreatedAt.Add(time.Duration(j*10) * time.Second),
			}
			if err := store.SaveCheckpoint(ctx, cp); err != nil {
				return fmt.Errorf("failed to save checkpoint: %w", err)
			}
		}
	}

	log.Printf("Seeded %d jobs, 1 fan-out, 4 checkpoints for E2E testing", len(allJobs))
	return nil
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
