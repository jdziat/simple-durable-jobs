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

	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v3/ui"
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
	seedCtx := context.Background()
	if err := store.Migrate(seedCtx); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
	}

	if err := seedE2EData(seedCtx, store, db); err != nil {
		log.Fatalf("Failed to seed data: %v", err)
	}
	statsStore := ui.NewGormStatsStorage(db)
	if err := statsStore.MigrateStats(seedCtx); err != nil {
		log.Fatalf("Failed to migrate stats data: %v", err)
	}
	if err := seedStatsHistory(seedCtx, statsStore, time.Now()); err != nil {
		log.Fatalf("Failed to seed stats history: %v", err)
	}

	q := queue.New(store)
	q.Register("cleanup", func(context.Context, map[string]any) error {
		return nil
	})
	if err := q.Schedule("cleanup", map[string]any{"mode": "e2e"}, stringSchedule{Schedule: schedule.Every(5 * time.Minute), value: "*/5 * * * *"}, queue.QueueOpt("maintenance")); err != nil {
		log.Fatalf("Failed to schedule cleanup: %v", err)
	}
	ctx, cancel := context.WithCancel(seedCtx)
	defer cancel()

	handler := ui.Handler(store, ui.WithQueue(q), ui.WithContext(ctx), ui.WithInsecureAllowUnauthenticated())

	ln, err := net.Listen("tcp", "127.0.0.1:"+*port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	addr := ln.Addr().(*net.TCPAddr)
	_, _ = fmt.Fprintf(os.Stdout, "E2E_BASE_URL=http://127.0.0.1:%d\n", addr.Port)
	_ = os.Stdout.Sync()

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

type stringSchedule struct {
	schedule.Schedule
	value string
}

func (s stringSchedule) String() string {
	return s.value
}

func seedE2EData(ctx context.Context, store *storage.GormStorage, db *gorm.DB) error {
	now := time.Now()

	// Pending jobs (5 total: 3 in default, 2 in emails)
	pendingJobs := []*core.Job{
		{ID: e2eID("e2e-pending-001"), Type: "SendEmail", Queue: "default", Tenant: "acme", Metadata: core.MetadataMap{"region": "us-east", "team": "payments"}, Status: core.StatusPending, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "user@example.com"}), CreatedAt: now.Add(-10 * time.Minute)},
		{ID: e2eID("e2e-pending-002"), Type: "GenerateReport", Queue: "default", Tenant: "globex", Metadata: core.MetadataMap{"region": "eu-west", "team": "analytics"}, Status: core.StatusPending, Priority: 3, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 42}), CreatedAt: now.Add(-9 * time.Minute)},
		{ID: e2eID("e2e-pending-003"), Type: "ProcessOrder", Queue: "default", Status: core.StatusPending, Priority: 7, MaxRetries: 5, Args: mustJSON(map[string]any{"order_id": 1001}), CreatedAt: now.Add(-8 * time.Minute)},
		{ID: e2eID("e2e-pending-004"), Type: "SendEmail", Queue: "emails", Status: core.StatusPending, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "admin@example.com"}), CreatedAt: now.Add(-7 * time.Minute)},
		{ID: e2eID("e2e-pending-005"), Type: "SendNotification", Queue: "emails", Status: core.StatusPending, Priority: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"channel": "slack"}), CreatedAt: now.Add(-6 * time.Minute)},
	}

	// Running jobs (3 total)
	runningStart1 := now.Add(-5 * time.Minute)
	runningStart2 := now.Add(-3 * time.Minute)
	runningStart3 := now.Add(-2 * time.Minute)
	lockedUntil := now.Add(5 * time.Minute)
	runningJobs := []*core.Job{
		{ID: e2eID("e2e-running-001"), Type: "GenerateReport", Queue: "default", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 99}), CreatedAt: now.Add(-6 * time.Minute), StartedAt: &runningStart1, LockedBy: "worker-1", LockedUntil: &lockedUntil},
		{ID: e2eID("e2e-running-002"), Type: "ProcessOrder", Queue: "default", Status: core.StatusRunning, Priority: 8, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"order_id": 2002}), CreatedAt: now.Add(-4 * time.Minute), StartedAt: &runningStart2, LockedBy: "worker-2", LockedUntil: &lockedUntil},
		{ID: e2eID("e2e-running-003"), Type: "SendEmail", Queue: "emails", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "test@example.com"}), CreatedAt: now.Add(-3 * time.Minute), StartedAt: &runningStart3, LockedBy: "worker-1", LockedUntil: &lockedUntil},
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
			ID:          e2eID(fmt.Sprintf("e2e-completed-%03d", i+1)),
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
		if i == 0 {
			completedJobs[i].Tenant = "acme"
			completedJobs[i].Metadata = core.MetadataMap{"region": "us-east", "team": "payments", "tier": "gold"}
		}
		if i == 1 {
			completedJobs[i].Tenant = "globex"
			completedJobs[i].Metadata = core.MetadataMap{"region": "eu-west", "team": "analytics", "tier": "silver"}
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
		var deadLetteredAt *time.Time
		deadLetterReason := ""
		// e2e-failed-003 is the dead-lettered fixture (no e2e test mutates it,
		// so the DLQ badge/filter/panel assertions stay stable across the run).
		if i == 2 {
			deadLetteredAt = &completedAt
			deadLetterReason = "max retries exhausted: " + failedErrors[i]
		}
		failedJobs[i] = &core.Job{
			ID:               e2eID(fmt.Sprintf("e2e-failed-%03d", i+1)),
			Type:             "SendEmail",
			Queue:            "default",
			Status:           core.StatusFailed,
			Priority:         5,
			Attempt:          3,
			MaxRetries:       3,
			Args:             mustJSON(map[string]any{"to": fmt.Sprintf("user%d@example.com", i+1)}),
			LastError:        failedErrors[i],
			DeadLetteredAt:   deadLetteredAt,
			DeadLetterReason: deadLetterReason,
			CreatedAt:        createdAt,
			StartedAt:        &startedAt,
			CompletedAt:      &completedAt,
		}
		if i == 2 {
			failedJobs[i].Tenant = "acme"
			failedJobs[i].Metadata = core.MetadataMap{"region": "us-east", "team": "payments"}
		}
	}
	deadLetteredJobs := []*core.Job{
		{
			ID:               e2eID("e2e-failed-006"),
			Type:             "SendWebhook",
			Queue:            "default",
			Tenant:           "beta",
			Metadata:         core.MetadataMap{"region": "eu-west", "team": "fulfillment"},
			Status:           core.StatusFailed,
			Priority:         5,
			Attempt:          3,
			MaxRetries:       3,
			Args:             mustJSON(map[string]any{"url": "https://example.invalid/webhook"}),
			LastError:        "webhook endpoint returned 502",
			DeadLetteredAt:   ptrTime(now.Add(-90 * time.Minute)),
			DeadLetterReason: "max retries exhausted: webhook endpoint returned 502",
			CreatedAt:        now.Add(-2 * time.Hour),
			StartedAt:        ptrTime(now.Add(-2*time.Hour + 5*time.Second)),
			CompletedAt:      ptrTime(now.Add(-90 * time.Minute)),
		},
		{
			ID:               e2eID("e2e-failed-007"),
			Type:             "ProcessRefund",
			Queue:            "default",
			Tenant:           "acme",
			Metadata:         core.MetadataMap{"region": "us-west", "team": "support"},
			Status:           core.StatusFailed,
			Priority:         4,
			Attempt:          3,
			MaxRetries:       3,
			Args:             mustJSON(map[string]any{"refund_id": 7007}),
			LastError:        "refund approval timed out",
			DeadLetteredAt:   ptrTime(now.Add(-80 * time.Minute)),
			DeadLetterReason: "max retries exhausted: refund approval timed out",
			CreatedAt:        now.Add(-110 * time.Minute),
			StartedAt:        ptrTime(now.Add(-110*time.Minute + 5*time.Second)),
			CompletedAt:      ptrTime(now.Add(-80 * time.Minute)),
		},
	}

	// Paused jobs (2 total)
	pausedJobs := []*core.Job{
		{ID: e2eID("e2e-paused-001"), Type: "SendEmail", Queue: "default", Status: core.StatusPaused, Priority: 5, MaxRetries: 3, Args: mustJSON(map[string]any{"to": "paused@example.com"}), CreatedAt: now.Add(-15 * time.Minute)},
		{ID: e2eID("e2e-paused-002"), Type: "GenerateReport", Queue: "emails", Status: core.StatusPaused, Priority: 3, MaxRetries: 3, Args: mustJSON(map[string]any{"report_id": 77}), CreatedAt: now.Add(-14 * time.Minute)},
	}

	// Cancelled job (1 total)
	cancelledStart := now.Add(-20 * time.Minute)
	cancelledComplete := cancelledStart.Add(10 * time.Second)
	cancelledJobs := []*core.Job{
		{ID: e2eID("e2e-cancelled-001"), Type: "ProcessOrder", Queue: "default", Status: core.StatusCancelled, Priority: 5, Attempt: 1, MaxRetries: 3, Args: mustJSON(map[string]any{"order_id": 9999}), LastError: "cancelled by user", CreatedAt: now.Add(-25 * time.Minute), StartedAt: &cancelledStart, CompletedAt: &cancelledComplete},
	}

	// Workflow: parent + 3 children + FanOut
	wfCreated := now.Add(-2 * time.Hour)
	wfStarted := wfCreated.Add(5 * time.Second)
	fanOutID := e2eID("e2e-fanout-001")
	workflowJobs := []*core.Job{
		{ID: e2eID("e2e-workflow-root"), Type: "BatchProcess", Queue: "default", Status: core.StatusRunning, Priority: 5, Attempt: 1, MaxRetries: 1, Args: mustJSON(map[string]any{"batch_size": 3}), CreatedAt: wfCreated, StartedAt: &wfStarted, LockedBy: "worker-1", LockedUntil: &lockedUntil},
	}
	rootID := e2eID("e2e-workflow-root")
	childStatuses := []core.JobStatus{core.StatusCompleted, core.StatusRunning, core.StatusPending}
	for i := 0; i < 3; i++ {
		childCreated := wfCreated.Add(time.Duration(i+1) * 10 * time.Second)
		child := &core.Job{
			ID:          e2eID(fmt.Sprintf("e2e-workflow-child-%03d", i+1)),
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
	allJobs := make([]*core.Job, 0, 32)
	allJobs = append(allJobs, pendingJobs...)
	allJobs = append(allJobs, runningJobs...)
	allJobs = append(allJobs, completedJobs...)
	allJobs = append(allJobs, failedJobs...)
	allJobs = append(allJobs, deadLetteredJobs...)
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
				ID:        e2eID(fmt.Sprintf("e2e-checkpoint-%03d-%03d", i+1, j+1)),
				JobID:     e2eID(fmt.Sprintf("e2e-completed-%03d", i+1)),
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

func seedStatsHistory(ctx context.Context, statsStore ui.StatsStorage, now time.Time) error {
	queues := []string{"default", "emails"}
	nowMin := now.Truncate(time.Minute)

	// Historical span: 31 full days with a diurnal wave so every throughput
	// window (1h / 24h / 7d / 30d) shows a real day/night shape and the selector
	// visibly changes the chart. Granularity is tiered to keep startup fast while
	// covering each window's bucket size: 15-minute within the last ~25h (feeds
	// the 24h view's 30m buckets) and hourly before that (feeds 7d's 3h and 30d's
	// 12h buckets). The most recent ~75 minutes are overwritten at 1-minute
	// granularity (below) so the 1h window is smooth. Counts are per-bucket; the
	// y-axis auto-scales.
	monthAgo := nowMin.Add(-31 * 24 * time.Hour)
	denseSince := nowMin.Add(-25 * time.Hour)
	recentStart := nowMin.Add(-75 * time.Minute)
	histBuckets := 0
	for ts := monthAgo; ts.Before(recentStart); {
		// Diurnal factor: busy 09:00-18:00, quiet overnight (deterministic).
		hour := ts.Hour()
		dayFactor := int64(1)
		switch {
		case hour >= 9 && hour < 18:
			dayFactor = 5
		case hour >= 6 && hour < 22:
			dayFactor = 3
		}
		minuteOfDay := ts.Hour()*60 + ts.Minute()
		// Each row counts jobs over the interval it represents, so a fixed-width
		// chart bucket sums to a comparable total whether it was sourced from
		// hourly or 15-minute rows (otherwise the denser recent tier would spike).
		intervalMin := int64(15)
		if ts.Before(denseSince) {
			intervalMin = 60
		}
		for queueIndex, queueName := range queues {
			ratePerMin := dayFactor*int64(1+((minuteOfDay/15+queueIndex*5)%3)) + int64(queueIndex)
			completed := ratePerMin * intervalMin
			failed := int64(0)
			if (minuteOfDay/15+queueIndex*3)%23 == 0 {
				failed = (dayFactor + int64(minuteOfDay%4)) * intervalMin / 15
			}
			if err := statsStore.UpsertStatCounters(ctx, queueName, ts, completed, failed, 0); err != nil {
				return fmt.Errorf("failed to seed stat counters for %s at %s: %w", queueName, ts.Format(time.RFC3339), err)
			}
			// Hourly queue-depth snapshots over the historical span.
			if ts.Minute() == 0 {
				pending := int64(3 + ((hour + queueIndex*2) % 9))
				running := int64(1 + ((hour + queueIndex) % 3))
				if err := statsStore.SnapshotQueueDepth(ctx, queueName, ts, pending, running); err != nil {
					return fmt.Errorf("failed to seed queue depth for %s at %s: %w", queueName, ts.Format(time.RFC3339), err)
				}
			}
		}
		histBuckets++
		if ts.Before(denseSince) {
			ts = ts.Add(time.Hour) // coarse for the 7d / 30d windows
		} else {
			ts = ts.Add(15 * time.Minute) // dense for the 24h window
		}
	}

	// Recent window: dense 1-minute granularity for a smooth 1h view.
	start := nowMin.Add(-70 * time.Minute)
	for minute := 0; minute <= 70; minute++ {
		ts := start.Add(time.Duration(minute) * time.Minute)
		for queueIndex, queueName := range queues {
			completed := int64(3 + ((minute*5 + queueIndex*7) % 16))
			failed := int64(0)
			if (minute+queueIndex*3)%19 == 0 {
				failed = int64(1 + (minute % 3))
			}

			// Deliberately sparse buckets exercise the frontend's timestamp-union merge:
			// one minute has completions only, another has failures only.
			completionOnly := queueName == "emails" && minute == 17
			failureOnly := queueName == "default" && minute == 23
			if completionOnly {
				completed = 12
				failed = 0
			}
			if failureOnly {
				completed = 0
				failed = 2
			}

			if err := statsStore.UpsertStatCounters(ctx, queueName, ts, completed, failed, 0); err != nil {
				return fmt.Errorf("failed to seed stat counters for %s at %s: %w", queueName, ts.Format(time.RFC3339), err)
			}

			if completionOnly || failureOnly {
				continue
			}
			if minute%3 == 0 || minute%7 == 0 {
				pending := int64(4 + ((70 - minute + queueIndex*2) % 9))
				running := int64(1 + ((minute + queueIndex) % 3))
				if err := statsStore.SnapshotQueueDepth(ctx, queueName, ts, pending, running); err != nil {
					return fmt.Errorf("failed to seed queue depth for %s at %s: %w", queueName, ts.Format(time.RFC3339), err)
				}
			}
		}
	}

	log.Printf("Seeded stats history for %d queues: 31d tiered (hourly + 15m, %d buckets) + last 71m at 1m granularity", len(queues), histBuckets)
	return nil
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

// e2eID maps a stable human-readable fixture name to a deterministic UUID so the
// seeded IDs are valid core.UUID values (v3 binary PK) yet reproducible across
// runs. The e2e suite's seed-data.ts is re-keyed to these UUIDs separately.
func e2eID(name string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceURL, []byte("sdj-e2e/"+name)).String())
}
