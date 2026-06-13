// Command jobsui runs the Jobs UI dashboard as a standalone server.
//
// Usage:
//
//	jobsui --driver sqlite --dsn "./jobs.db"
//	jobsui --driver postgres --dsn "postgres://user:pass@localhost/myapp"
//	jobsui --driver sqlite --dsn "./jobs.db" --init  # Seed with demo data
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"time"


	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v3/ui"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	dsn := flag.String("dsn", "", "Database connection string (required)")
	driver := flag.String("driver", "sqlite", "Database driver: postgres, sqlite")
	addr := flag.String("addr", "127.0.0.1:8080", "Listen address (defaults to loopback; a non-loopback/all-interfaces bind needs --insecure or auth)")
	init := flag.Bool("init", false, "Seed database with demo data")
	insecure := flag.Bool("insecure", false, "Allow unauthenticated dashboard access on non-loopback addresses")
	flag.Parse()

	if *dsn == "" {
		log.Fatal("--dsn is required")
	}

	var dialector gorm.Dialector
	switch *driver {
	case "postgres":
		dialector = postgres.Open(*dsn)
	case "sqlite":
		dialector = sqlite.Open(*dsn)
	default:
		log.Fatalf("Unknown driver: %s", *driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Configure connection pool
	if err := storage.ConfigurePool(db); err != nil {
		log.Printf("Warning: could not configure connection pool: %v", err)
	}

	store, err := storage.NewGormStorageWithPool(db)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Run migrations to ensure tables exist
	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	if *init {
		log.Println("Seeding database with demo data...")
		if err := seedDatabase(context.Background(), store); err != nil {
			log.Fatalf("Failed to seed database: %v", err)
		}
		log.Println("Database seeded successfully")
	}

	handlerOpts := []ui.Option{}
	if isLoopbackAddr(*addr) {
		log.Printf("Local dashboard, unauthenticated - loopback only (%s)", *addr)
		handlerOpts = append(handlerOpts, ui.WithInsecureAllowUnauthenticated())
	} else if *insecure {
		log.Printf("WARNING: jobs UI running unauthenticated on non-loopback address %s", *addr)
		handlerOpts = append(handlerOpts, ui.WithInsecureAllowUnauthenticated())
	} else {
		log.Fatalf("refusing to run unauthenticated jobs UI on non-loopback address %s; add --insecure for a trusted network or wire auth in an embedded ui.Handler", *addr)
	}

	log.Printf("Jobs UI running at %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, ui.Handler(store, handlerOpts...)))
}

// isLoopbackAddr reports whether addr binds ONLY the loopback interface. An
// empty or unspecified host (":8080", "0.0.0.0:8080", "[::]:8080") binds ALL
// interfaces and is therefore NOT loopback — treating it as loopback would
// silently expose the unauthenticated dashboard on the network.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "" {
		return false // empty host binds 0.0.0.0 / [::] (all interfaces)
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback() // false for unspecified 0.0.0.0 / ::
}

// seedDatabase populates the database with demo jobs for testing.
func seedDatabase(ctx context.Context, store *storage.GormStorage) error {
	queues := []string{"default", "emails", "reports", "notifications", "imports"}
	jobTypes := []string{
		"SendEmail", "GenerateReport", "ProcessOrder",
		"SyncInventory", "SendNotification", "ImportData",
		"ExportData", "CleanupOldRecords", "UpdateMetrics",
	}

	now := time.Now()

	// Create jobs in various states
	var jobs []*core.Job

	// Pending jobs
	for i := 0; i < 15; i++ {
		jobs = append(jobs, &core.Job{
			ID:         core.NewID(),
			Type:       jobTypes[rand.Intn(len(jobTypes))],
			Queue:      queues[rand.Intn(len(queues))],
			Status:     core.StatusPending,
			Priority:   rand.Intn(10),
			Attempt:    0,
			MaxRetries: 3,
			Args:       mustJSON(map[string]any{"item_id": rand.Intn(10000), "user_id": rand.Intn(1000)}),
			CreatedAt:  now.Add(-time.Duration(rand.Intn(60)) * time.Minute),
		})
	}

	// Running jobs
	for i := 0; i < 5; i++ {
		startedAt := now.Add(-time.Duration(rand.Intn(10)) * time.Minute)
		jobs = append(jobs, &core.Job{
			ID:         core.NewID(),
			Type:       jobTypes[rand.Intn(len(jobTypes))],
			Queue:      queues[rand.Intn(len(queues))],
			Status:     core.StatusRunning,
			Priority:   rand.Intn(10),
			Attempt:    1,
			MaxRetries: 3,
			Args:       mustJSON(map[string]any{"batch_id": core.NewID()}),
			CreatedAt:  startedAt.Add(-time.Minute),
			StartedAt:  &startedAt,
			LockedBy:   fmt.Sprintf("worker-%d", rand.Intn(3)+1),
		})
	}

	// Completed jobs
	for i := 0; i < 50; i++ {
		createdAt := now.Add(-time.Duration(rand.Intn(24)) * time.Hour)
		startedAt := createdAt.Add(time.Duration(rand.Intn(60)) * time.Second)
		completedAt := startedAt.Add(time.Duration(rand.Intn(300)) * time.Second)
		jobs = append(jobs, &core.Job{
			ID:          core.NewID(),
			Type:        jobTypes[rand.Intn(len(jobTypes))],
			Queue:       queues[rand.Intn(len(queues))],
			Status:      core.StatusCompleted,
			Priority:    rand.Intn(10),
			Attempt:     rand.Intn(2) + 1,
			MaxRetries:  3,
			Args:        mustJSON(map[string]any{"order_id": rand.Intn(100000)}),
			CreatedAt:   createdAt,
			StartedAt:   &startedAt,
			CompletedAt: &completedAt,
		})
	}

	// Failed jobs
	errors := []string{
		"connection timeout after 30s",
		"record not found: user_id=12345",
		"rate limit exceeded, retry after 60s",
		"invalid input: email format",
		"external API returned 503",
		"database deadlock detected",
	}
	for i := 0; i < 10; i++ {
		createdAt := now.Add(-time.Duration(rand.Intn(12)) * time.Hour)
		startedAt := createdAt.Add(time.Duration(rand.Intn(60)) * time.Second)
		completedAt := startedAt.Add(time.Duration(rand.Intn(60)) * time.Second)
		jobs = append(jobs, &core.Job{
			ID:          core.NewID(),
			Type:        jobTypes[rand.Intn(len(jobTypes))],
			Queue:       queues[rand.Intn(len(queues))],
			Status:      core.StatusFailed,
			Priority:    rand.Intn(10),
			Attempt:     3,
			MaxRetries:  3,
			Args:        mustJSON(map[string]any{"request_id": core.NewID()}),
			LastError:   errors[rand.Intn(len(errors))],
			CreatedAt:   createdAt,
			StartedAt:   &startedAt,
			CompletedAt: &completedAt,
		})
	}

	// Scheduled jobs (future run_at)
	for i := 0; i < 5; i++ {
		runAt := now.Add(time.Duration(rand.Intn(60)+1) * time.Minute)
		jobs = append(jobs, &core.Job{
			ID:         core.NewID(),
			Type:       jobTypes[rand.Intn(len(jobTypes))],
			Queue:      queues[rand.Intn(len(queues))],
			Status:     core.StatusPending,
			Priority:   rand.Intn(10),
			Attempt:    0,
			MaxRetries: 3,
			Args:       mustJSON(map[string]any{"scheduled": true, "delay_minutes": rand.Intn(60)}),
			CreatedAt:  now,
			RunAt:      &runAt,
		})
	}

	// Enqueue all jobs
	for _, job := range jobs {
		if err := store.Enqueue(ctx, job); err != nil {
			return fmt.Errorf("failed to enqueue job: %w", err)
		}
	}

	// Add some checkpoints to a few completed jobs
	for i := 0; i < 5; i++ {
		if i < len(jobs) && jobs[i].Status == core.StatusCompleted {
			for j := 0; j < rand.Intn(3)+1; j++ {
				cp := &core.Checkpoint{
					ID:        core.NewID(),
					JobID:     jobs[i].ID,
					CallIndex: j,
					CallType:  "http.Get",
					Result:    mustJSON(map[string]any{"status": 200, "body": "OK"}),
					CreatedAt: jobs[i].CreatedAt.Add(time.Duration(j*10) * time.Second),
				}
				if err := store.SaveCheckpoint(ctx, cp); err != nil {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
			}
		}
	}

	log.Printf("Created %d jobs across %d queues", len(jobs), len(queues))
	return nil
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
