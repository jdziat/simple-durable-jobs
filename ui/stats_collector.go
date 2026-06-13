package ui

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
)

// StatsCollector subscribes to queue events and periodically snapshots queue depth.
type StatsCollector struct {
	queue     *queue.Queue
	stats     StatsStorage
	retention time.Duration
	logger    *slog.Logger

	mu       sync.Mutex
	counters map[string]*statCounters

	// ready is closed once the collector has subscribed to events and is processing.
	ready     chan struct{}
	readyOnce sync.Once
}

type statCounters struct {
	completed int64
	failed    int64
	retried   int64
}

// StatsCollectorOption configures the StatsCollector.
type StatsCollectorOption interface {
	apply(*StatsCollector)
}

type statsCollectorOptionFunc func(*StatsCollector)

func (f statsCollectorOptionFunc) apply(sc *StatsCollector) { f(sc) }

// WithStatsCollectorRetention sets the retention duration for stats rows.
// Default: 31 days, so the dashboard's 30d throughput window always has data.
// Lower it to reduce stats-table growth if you do not use the longer windows.
func WithStatsCollectorRetention(d time.Duration) StatsCollectorOption {
	return statsCollectorOptionFunc(func(sc *StatsCollector) {
		sc.retention = d
	})
}

// WithStatsCollectorLogger sets the logger used for stats persistence errors.
func WithStatsCollectorLogger(logger *slog.Logger) StatsCollectorOption {
	return statsCollectorOptionFunc(func(sc *StatsCollector) {
		if logger != nil {
			sc.logger = logger
		}
	})
}

// NewStatsCollector creates a new StatsCollector.
func NewStatsCollector(q *queue.Queue, stats StatsStorage, opts ...StatsCollectorOption) *StatsCollector {
	sc := &StatsCollector{
		queue:     q,
		stats:     stats,
		retention: 31 * 24 * time.Hour, // covers the dashboard's longest (30d) throughput window
		logger:    slog.Default(),
		counters:  make(map[string]*statCounters),
		ready:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt.apply(sc)
	}
	return sc
}

// WaitReady blocks until the collector has subscribed to events.
func (sc *StatsCollector) WaitReady() {
	<-sc.ready
}

// Start begins the event listener and periodic snapshot ticker.
// Blocks until ctx is cancelled.
func (sc *StatsCollector) Start(ctx context.Context) {
	events := sc.queue.Events()
	defer sc.queue.Unsubscribe(events)

	sc.readyOnce.Do(func() { close(sc.ready) })

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			sc.Flush(flushCtx)
			cancel()
			return
		case e := <-events:
			sc.handleEvent(e)
		case <-ticker.C:
			sc.Flush(ctx)
			sc.snapshot(ctx)
			sc.prune(ctx)
		}
	}
}

func (sc *StatsCollector) handleEvent(e core.Event) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	switch ev := e.(type) {
	case *core.JobCompleted:
		sc.getCounters(ev.Job.Queue).completed++
	case *core.JobFailed:
		sc.getCounters(ev.Job.Queue).failed++
	case *core.JobRetrying:
		sc.getCounters(ev.Job.Queue).retried++
	}
}

func (sc *StatsCollector) getCounters(queue string) *statCounters {
	c, ok := sc.counters[queue]
	if !ok {
		c = &statCounters{}
		sc.counters[queue] = c
	}
	return c
}

// Flush writes accumulated counters to the stats storage.
func (sc *StatsCollector) Flush(ctx context.Context) {
	sc.mu.Lock()
	batch := sc.counters
	sc.counters = make(map[string]*statCounters)
	sc.mu.Unlock()

	ts := time.Now().Truncate(time.Minute)
	for queueName, c := range batch {
		if c.completed == 0 && c.failed == 0 && c.retried == 0 {
			continue
		}
		if err := sc.stats.UpsertStatCounters(ctx, queueName, ts, c.completed, c.failed, c.retried); err != nil {
			sc.logger.Error("failed to upsert stats counters",
				"queue", queueName,
				"completed", c.completed,
				"failed", c.failed,
				"retried", c.retried,
				"error", err,
			)
			sc.remergeCounters(queueName, c)
		}
	}
}

func (sc *StatsCollector) remergeCounters(queueName string, failed *statCounters) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	c := sc.getCounters(queueName)
	c.completed += failed.completed
	c.failed += failed.failed
	c.retried += failed.retried
}

func (sc *StatsCollector) snapshot(ctx context.Context) {
	ts := time.Now().Truncate(time.Minute)
	storage := sc.queue.Storage()

	queueDepth := make(map[string]*[2]int64) // [pending, running]

	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning} {
		jobs, err := storage.GetJobsByStatus(ctx, status, 10000)
		if err != nil {
			sc.logger.Error("failed to query jobs for stats snapshot", "status", status, "error", err)
			continue
		}
		for _, job := range jobs {
			d, ok := queueDepth[job.Queue]
			if !ok {
				d = &[2]int64{}
				queueDepth[job.Queue] = d
			}
			switch status {
			case core.StatusPending:
				d[0]++
			case core.StatusRunning:
				d[1]++
			}
		}
	}

	for queueName, d := range queueDepth {
		if err := sc.stats.SnapshotQueueDepth(ctx, queueName, ts, d[0], d[1]); err != nil {
			sc.logger.Error("failed to snapshot queue depth",
				"queue", queueName,
				"pending", d[0],
				"running", d[1],
				"error", err,
			)
		}
	}
}

func (sc *StatsCollector) prune(ctx context.Context) {
	if sc.retention > 0 {
		if _, err := sc.stats.PruneStats(ctx, time.Now().Add(-sc.retention)); err != nil {
			sc.logger.Error("failed to prune stats", "retention", sc.retention, "error", err)
		}
	}
}
