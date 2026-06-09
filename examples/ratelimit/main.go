// Package main demonstrates queue rate limiting and per-key concurrency caps.
//
// The local queue rate limit is per worker process. The concurrency cap is
// database-backed with GormStorage, so workers in the same fleet share the cap.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sdj-ratelimit-*")
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
	observed := newObservations()

	queue.Register("limited-work", func(ctx context.Context, args LimitedWorkArgs) error {
		start := time.Now()
		elapsed, inFlight := observed.started(args.Phase, args.Tenant, start)
		fmt.Printf("[phase %s elapsed=%5.3fs] start item=%02d tenant=%s in-flight=%d\n",
			args.Phase, elapsed.Seconds(), args.Item, args.Tenant, inFlight)
		time.Sleep(time.Duration(args.WorkMillis) * time.Millisecond)
		observed.finished(args.Tenant)
		return nil
	})

	fmt.Println("Legend: elapsed=seconds since phase start; in-flight=active jobs for the tenant; avg-start-rate=first-to-last start spacing.")

	const rateJobs = 8
	fmt.Println("\n=== Phase A: queue rate limit binds ===")
	observed.startPhase("A")
	for i := 1; i <= rateJobs; i++ {
		id, err := queue.Enqueue(ctx, "limited-work", LimitedWorkArgs{
			Phase:      "A",
			Item:       i,
			Tenant:     "tenant-a",
			WorkMillis: 50,
		}, jobs.QueueOpt("rate-limited"), jobs.Unique(fmt.Sprintf("rate:tenant-a:%02d", i)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Enqueued phase A job %s item=%02d\n", id[:8], i)
	}

	rateCtx, cancelRate := context.WithCancel(ctx)
	rateWorker := queue.NewWorker(
		jobs.WorkerQueue("rate-limited", jobs.Concurrency(6)),
		jobs.WithQueueRateLimit("rate-limited", 3, 1),
		jobs.WithPollInterval(50*time.Millisecond),
	)
	go func() {
		fmt.Println("Starting phase A worker with 3/sec queue rate limit and burst=1...")
		_ = rateWorker.Start(rateCtx)
	}()

	if err := waitForCompletedCount(ctx, storage, rateJobs, 10*time.Second); err != nil {
		log.Fatal(err)
	}
	cancelRate()
	time.Sleep(200 * time.Millisecond)

	first, last, maxInFlight := observed.summary("A")
	elapsed := last.Sub(first).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	observedRate := float64(rateJobs-1) / elapsed
	fmt.Printf("Phase A observed starts: %d jobs over %.2fs after first start (%.2f avg-start-rate)\n", rateJobs, elapsed, observedRate)
	fmt.Printf("Phase A maximum in-flight for tenant-a: %d\n", maxInFlight)
	fmt.Println("Phase A pacing governed by the queue rate limit (3/sec); 50ms work keeps concurrency from binding.")

	const capJobs = 6
	fmt.Println("\n=== Phase B: per-key concurrency cap binds ===")
	observed.startPhase("B")
	for i := 1; i <= capJobs; i++ {
		id, err := queue.Enqueue(ctx, "limited-work", LimitedWorkArgs{
			Phase:      "B",
			Item:       i,
			Tenant:     "tenant-a",
			WorkMillis: 700,
		}, jobs.QueueOpt("cap-limited"), jobs.Unique(fmt.Sprintf("cap:tenant-a:%02d", i)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Enqueued phase B job %s item=%02d\n", id[:8], i)
	}

	capCtx, cancelCap := context.WithCancel(ctx)
	capWorker := queue.NewWorker(
		jobs.WorkerQueue("cap-limited", jobs.Concurrency(6)),
		jobs.ConcurrencyCap("tenant-work", 2, jobs.CapKey(tenantFromJob)),
		jobs.WithPollInterval(50*time.Millisecond),
	)
	go func() {
		fmt.Println("Starting phase B worker with per-tenant cap=2 and no queue rate limit...")
		_ = capWorker.Start(capCtx)
	}()

	if err := waitForCompletedCount(ctx, storage, rateJobs+capJobs, 10*time.Second); err != nil {
		log.Fatal(err)
	}
	cancelCap()
	time.Sleep(300 * time.Millisecond)

	_, _, maxInFlight = observed.summary("B")
	fmt.Printf("Phase B maximum observed in-flight for tenant-a: %d\n", maxInFlight)
	fmt.Println("Phase B pacing governed by the per-key concurrency cap (2); no queue rate limit is configured.")
	fmt.Println("Rate limit example complete: queue rate limit and per-key concurrency cap both observed")
}

func tenantFromJob(job *jobs.Job) string {
	var args LimitedWorkArgs
	if err := json.Unmarshal(job.Args, &args); err != nil || args.Tenant == "" {
		return "unknown"
	}
	return args.Tenant
}

func waitForCompletedCount(ctx context.Context, storage jobs.Storage, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		completed, err := storage.GetJobsByStatus(ctx, jobs.StatusCompleted, 100)
		if err != nil {
			return err
		}
		if len(completed) >= want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	completed, _ := storage.GetJobsByStatus(ctx, jobs.StatusCompleted, 100)
	return fmt.Errorf("timed out waiting for %d completed jobs; saw %d", want, len(completed))
}

type LimitedWorkArgs struct {
	Phase      string `json:"phase"`
	Item       int    `json:"item"`
	Tenant     string `json:"tenant"`
	WorkMillis int    `json:"work_millis"`
}

type observations struct {
	mu          sync.Mutex
	phaseStart  map[string]time.Time
	starts      map[string][]time.Time
	inFlight    map[string]int
	maxInFlight map[string]int
}

func newObservations() *observations {
	return &observations{
		phaseStart:  make(map[string]time.Time),
		starts:      make(map[string][]time.Time),
		inFlight:    make(map[string]int),
		maxInFlight: make(map[string]int),
	}
}

func (o *observations) startPhase(phase string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.phaseStart[phase] = time.Now()
}

func (o *observations) started(phase, tenant string, at time.Time) (time.Duration, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts[phase] = append(o.starts[phase], at)
	o.inFlight[tenant]++
	if o.inFlight[tenant] > o.maxInFlight[phase] {
		o.maxInFlight[phase] = o.inFlight[tenant]
	}
	return at.Sub(o.phaseStart[phase]), o.inFlight[tenant]
}

func (o *observations) finished(tenant string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inFlight[tenant]--
}

func (o *observations) summary(phase string) (time.Time, time.Time, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	starts := o.starts[phase]
	if len(starts) == 0 {
		return time.Time{}, time.Time{}, 0
	}
	return starts[0], starts[len(starts)-1], o.maxInFlight[phase]
}
