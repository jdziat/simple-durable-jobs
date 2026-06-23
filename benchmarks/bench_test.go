package benchmarks

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	benchQueue        = "bench"
	benchPollInterval = 50 * time.Millisecond
	benchConcurrency  = 16
	benchBatchSize    = 100
	benchDequeueBatch = 10
	e2eJobs           = 1000
	workflowSteps     = 5
)

var sqliteCounter atomic.Int64

type benchEnv struct {
	queue *jobs.Queue
}

type noopArgs struct {
	N int `json:"n"`
}

func BenchmarkEnqueue(b *testing.B) {
	env := newBenchEnv(b)
	env.queue.Register("bench.enqueue", func(context.Context, noopArgs) error { return nil })
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := env.queue.Enqueue(ctx, "bench.enqueue", noopArgs{N: i}, jobs.QueueOpt(benchQueue)); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "jobs/sec")
}

func BenchmarkEnqueueBatch(b *testing.B) {
	env := newBenchEnv(b)
	env.queue.Register("bench.batch", func(context.Context, noopArgs) error { return nil })
	ctx := context.Background()

	entries := make([]jobs.BatchEntry, benchBatchSize)
	for i := range entries {
		entries[i] = jobs.Batch("bench.batch", noopArgs{N: i}, jobs.QueueOpt(benchQueue))
	}

	totalJobs := 0
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if _, err := env.queue.EnqueueBatch(ctx, entries); err != nil {
			b.Fatal(err)
		}
		totalJobs += len(entries)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(totalJobs)/elapsed.Seconds(), "jobs/sec")
}

func BenchmarkEndToEnd(b *testing.B) {
	benchmarkEndToEnd(b, 1)
}

func BenchmarkEndToEndBatchDequeue(b *testing.B) {
	benchmarkEndToEnd(b, benchDequeueBatch)
}

func BenchmarkWorkflowSteps(b *testing.B) {
	env := newBenchEnv(b)
	ctx := context.Background()
	var completed atomic.Int64
	done := make(chan struct{})

	for i := 0; i < workflowSteps; i++ {
		name := fmt.Sprintf("bench.step.%d", i)
		env.queue.Register(name, func(context.Context, noopArgs) (int, error) { return 1, nil })
	}
	env.queue.Register("bench.workflow", func(ctx context.Context, args noopArgs) error {
		for i := 0; i < workflowSteps; i++ {
			if _, err := jobs.Call[int](ctx, fmt.Sprintf("bench.step.%d", i), args); err != nil {
				return err
			}
		}
		return nil
	})
	env.queue.OnJobComplete(func(context.Context, *jobs.Job) {
		if completed.Add(1) == int64(b.N) {
			close(done)
		}
	})
	for i := 0; i < b.N; i++ {
		if _, err := env.queue.Enqueue(ctx, "bench.workflow", noopArgs{N: i}, jobs.QueueOpt(benchQueue)); err != nil {
			b.Fatal(err)
		}
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := jobs.NewWorker(env.queue,
		jobs.WorkerQueue(benchQueue, jobs.Concurrency(benchConcurrency)),
		jobs.WithPollInterval(benchPollInterval),
		jobs.WithDequeueBatchSize(benchDequeueBatch),
		jobs.WithDrainTimeout(5*time.Second),
	)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	errCh := startWorker(workerCtx, worker)
	waitDone(b, done, 2*time.Minute)
	elapsed := time.Since(start)
	b.StopTimer()
	cancel()
	requireWorkerStopped(b, errCh)
	b.ReportMetric(float64(b.N*workflowSteps)/elapsed.Seconds(), "steps/sec")
}

func BenchmarkDispatchLatency(b *testing.B) {
	env := newBenchEnv(b)
	ctx := context.Background()
	latencies := make([]int64, 0, b.N)
	started := make(chan *jobs.JobStarted, 1)
	completed := make(chan jobs.UUID, 1)

	env.queue.Register("bench.latency", func(context.Context, noopArgs) error { return nil })
	env.queue.OnJobStart(func(_ context.Context, job *jobs.Job) {
		started <- &jobs.JobStarted{Job: job, Timestamp: time.Now()}
	})
	env.queue.OnJobComplete(func(_ context.Context, job *jobs.Job) {
		completed <- job.ID
	})

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := jobs.NewWorker(env.queue,
		jobs.WorkerQueue(benchQueue, jobs.Concurrency(benchConcurrency)),
		jobs.WithPollInterval(benchPollInterval),
		jobs.WithDequeueBatchSize(benchDequeueBatch),
		jobs.WithDrainTimeout(5*time.Second),
	)
	errCh := startWorker(workerCtx, worker)
	time.Sleep(2 * benchPollInterval)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		time.Sleep(7 * time.Millisecond)
		b.StartTimer()

		enqueuedAt := time.Now()
		id, err := env.queue.Enqueue(ctx, "bench.latency", noopArgs{N: i}, jobs.QueueOpt(benchQueue))
		if err != nil {
			b.Fatal(err)
		}
		start := waitJobStarted(b, started, id, 2*time.Second)
		latencies = append(latencies, start.Sub(enqueuedAt).Microseconds())
		waitJobCompleted(b, completed, id, 2*time.Second)
	}
	b.StopTimer()
	cancel()
	requireWorkerStopped(b, errCh)

	reportLatency(b, latencies)
}

func benchmarkEndToEnd(b *testing.B, dequeueBatch int) {
	totalJobs := 0
	var totalElapsed time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		env := newBenchEnv(b)
		ctx := context.Background()
		var once sync.Once
		var firstStart time.Time
		var completed atomic.Int64
		done := make(chan struct{})

		env.queue.Register("bench.e2e", func(context.Context, noopArgs) error { return nil })
		env.queue.OnJobStart(func(context.Context, *jobs.Job) {
			once.Do(func() { firstStart = time.Now() })
		})
		env.queue.OnJobComplete(func(context.Context, *jobs.Job) {
			if completed.Add(1) == e2eJobs {
				close(done)
			}
		})
		entries := make([]jobs.BatchEntry, e2eJobs)
		for j := range entries {
			entries[j] = jobs.Batch("bench.e2e", noopArgs{N: j}, jobs.QueueOpt(benchQueue))
		}
		if _, err := env.queue.EnqueueBatch(ctx, entries); err != nil {
			b.Fatal(err)
		}

		workerCtx, cancel := context.WithCancel(ctx)
		opts := []jobs.WorkerOption{
			jobs.WorkerQueue(benchQueue, jobs.Concurrency(benchConcurrency)),
			jobs.WithPollInterval(benchPollInterval),
			jobs.WithDrainTimeout(5 * time.Second),
		}
		// Pin the dequeue batch size unconditionally so BenchmarkEndToEnd
		// (dequeueBatch == 1) measures genuine single-row dequeue regardless of
		// the worker's batched default, preserving the documented ~20 jobs/sec
		// poll-floor row and keeping the checked-in benchstat baselines
		// comparable. BenchmarkEndToEndBatchDequeue passes the batch size directly.
		opts = append(opts, jobs.WithDequeueBatchSize(dequeueBatch))
		worker := jobs.NewWorker(env.queue, opts...)

		b.StartTimer()
		errCh := startWorker(workerCtx, worker)
		waitDone(b, done, 2*time.Minute)
		elapsed := time.Since(firstStart)
		b.StopTimer()
		cancel()
		requireWorkerStopped(b, errCh)
		totalJobs += e2eJobs
		totalElapsed += elapsed
	}
	b.ReportMetric(float64(totalJobs)/totalElapsed.Seconds(), "jobs/sec")
}

func newBenchEnv(b *testing.B) *benchEnv {
	b.Helper()
	db := openBenchDB(b)
	sqlDB, err := db.DB()
	require.NoError(b, err)
	sqlDB.SetMaxOpenConns(16)
	sqlDB.SetMaxIdleConns(8)
	b.Cleanup(func() { _ = sqlDB.Close() })

	cleanupBenchDB(b, db)
	store := jobs.NewGormStorage(db)
	require.NoError(b, store.Migrate(context.Background()))
	cleanupBenchDB(b, db)
	return &benchEnv{queue: jobs.New(store)}
}

func openBenchDB(b *testing.B) *gorm.DB {
	b.Helper()
	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(b, err, "open mysql benchmark db")
		return db
	}
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(b, err, "open postgres benchmark db")
		return db
	}

	n := sqliteCounter.Add(1)
	path := fmt.Sprintf("%s/sdj_bench_%d_%d.db", os.TempDir(), os.Getpid(), n)
	b.Cleanup(func() {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(path + suffix)
		}
	})
	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN(path)), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(b, err, "open sqlite benchmark db")
	return db
}

func cleanupBenchDB(b *testing.B, db *gorm.DB) {
	b.Helper()
	tables := []string{"signals", "checkpoints", "fan_outs", "queue_states", "jobs", "scheduled_fires", "leases", "concurrency_slots", "rate_limit_windows"}
	for _, table := range tables {
		_ = db.Exec("DELETE FROM " + table).Error
	}
}

func startWorker(ctx context.Context, worker jobs.Starter) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Start(ctx)
	}()
	return errCh
}

func waitDone(b *testing.B, done <-chan struct{}, timeout time.Duration) {
	b.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		b.Fatalf("timed out waiting for benchmark jobs to complete")
	}
}

func waitJobStarted(b *testing.B, started <-chan *jobs.JobStarted, jobID jobs.UUID, timeout time.Duration) time.Time {
	b.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev := <-started:
			if ev.Job.ID == jobID {
				return ev.Timestamp
			}
		case <-timer.C:
			b.Fatalf("timed out waiting for job %s to start", jobID)
		}
	}
}

func waitJobCompleted(b *testing.B, completed <-chan jobs.UUID, jobID jobs.UUID, timeout time.Duration) {
	b.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case id := <-completed:
			if id == jobID {
				return
			}
		case <-timer.C:
			b.Fatalf("timed out waiting for job %s to complete", jobID)
		}
	}
}

func requireWorkerStopped(b *testing.B, errCh <-chan error) {
	b.Helper()
	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		b.Fatal("worker did not stop")
	}
}

func reportLatency(b *testing.B, values []int64) {
	b.Helper()
	require.NotEmpty(b, values)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	b.ReportMetric(float64(percentile(values, 50)), "p50-us")
	b.ReportMetric(float64(percentile(values, 95)), "p95-us")
	b.ReportMetric(float64(percentile(values, 99)), "p99-us")
}

func percentile(values []int64, p int) int64 {
	if len(values) == 0 {
		return 0
	}
	idx := (len(values)*p + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(values) {
		idx = len(values)
	}
	return values[idx-1]
}
