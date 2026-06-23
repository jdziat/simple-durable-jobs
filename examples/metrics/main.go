// Package main demonstrates Prometheus metrics for the queue.
//
// The metrics package is optional: call jobsmetrics.Instrument(queue) to emit
// OpenTelemetry metrics, then expose them with the provided Prometheus handler.
// You can also supply your own OpenTelemetry MeterProvider/exporter instead.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	jobsmetrics "github.com/jdziat/simple-durable-jobs/v4/pkg/metrics"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sdj-metrics-*")
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
	handler, mp, err := jobsmetrics.NewPrometheusHandler()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = mp.Shutdown(ctx) }()
	jobsmetrics.Instrument(queue, jobsmetrics.WithMeterProvider(mp))

	queue.Register("metric-job", func(ctx context.Context, args MetricJobArgs) error {
		fmt.Printf("[worker] Processing metric job %d\n", args.Index)
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	for i := 1; i <= 3; i++ {
		id, err := queue.Enqueue(ctx, "metric-job", MetricJobArgs{Index: i})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Enqueued metric job %s\n", id[:8])
	}

	server := httptest.NewServer(handler)
	defer server.Close()

	beforeBody, err := scrape(server.URL + "/metrics")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Queue depth before worker starts:")
	printMetricLines(beforeBody, []string{
		"jobs_queue_depth",
	})

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(2)))
	go func() {
		fmt.Println("Starting worker...")
		_ = worker.Start(workerCtx)
	}()

	if err := waitForCompletedCount(ctx, storage, 3, 10*time.Second); err != nil {
		log.Fatal(err)
	}

	afterBody, err := scrape(server.URL + "/metrics")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Processed job metrics after worker completes:")
	printMetricLines(afterBody, []string{
		"jobs_completed_total",
		"jobs_started_total",
	})
	if !printMetricLines(afterBody, []string{"jobs_queue_depth"}) {
		fmt.Println("jobs_queue_depth: no pending series (queue drained)")
	}
	fmt.Println("Histogram metrics also exist for jobs.wait.duration and jobs.run.duration.")

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Metrics example complete: scraped real queue metrics")
}

func scrape(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("failed to close metrics response body: %v", err)
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func printMetricLines(body string, names []string) bool {
	printed := false
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, name := range names {
			if strings.HasPrefix(line, name) {
				fmt.Println(line)
				printed = true
			}
		}
	}
	return printed
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

type MetricJobArgs struct {
	Index int `json:"index"`
}
