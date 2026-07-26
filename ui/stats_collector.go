package ui

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
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

// snapshot records one per-queue depth sample per minute.
//
// NOTE ON WHAT THIS DOES *NOT* CAPTURE: the counters this collector folds in
// alongside these depths come from Queue.Events, which is IN-PROCESS ONLY. Under
// the multi-worker topology the docs describe, the completed/failed/retried
// series therefore reflects only the one process serving the dashboard and
// under-reports the fleet. The DEPTH series below is read from the database and
// is fleet-wide. See docs/content/docs/embedded-ui.md.
func (sc *StatsCollector) snapshot(ctx context.Context) {
	ts := time.Now().Truncate(time.Minute)
	storage := sc.queue.Storage()

	var (
		queueDepth map[string]*[2]int64
		err        error
	)
	if agg, ok := storage.(queueDepthStatsStorage); ok {
		// Do NOT fall back to the scan when the aggregate fails: it reads the same
		// table the aggregate just failed on, only far more expensively.
		queueDepth, err = sc.aggregateQueueDepth(ctx, agg)
	} else {
		queueDepth, err = sc.scanQueueDepth(ctx, storage)
	}
	if err != nil {
		sc.logger.Error("failed to read queue depth for stats snapshot", "error", err)
		return
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

// aggregateQueueDepth counts depth with one GROUP BY per call.
//
// The queue SET it reports is deliberately identical to the scan's: a row only
// for queues with pending or running work. The aggregate itself also returns
// queues whose jobs are all terminal, and writing those would add a zero-depth
// sample every minute, forever, for every queue that has ever run a job —
// unbounded growth in job_stats and a behaviour change nobody asked for. This
// packet is about the numbers being RIGHT, not about which queues appear.
func (sc *StatsCollector) aggregateQueueDepth(ctx context.Context, agg queueDepthStatsStorage) (map[string]*[2]int64, error) {
	stats, err := agg.GetQueueDepthStats(ctx)
	if err != nil {
		return nil, err
	}
	queueDepth := make(map[string]*[2]int64, len(stats))
	for _, qs := range stats {
		if qs == nil || (qs.GetPending() == 0 && qs.GetRunning() == 0) {
			continue
		}
		queueDepth[qs.GetName()] = &[2]int64{qs.GetPending(), qs.GetRunning()}
	}
	return queueDepth, nil
}

// scanQueueDepth is the fallback for a core.Storage with no aggregate
// capability. It is what this collector used to do unconditionally, and it is
// kept honest about its own limit: it pages FULL job rows — payloads included
// and codec-decoded — purely to count them, and it TRUNCATES at the cap. A
// truncated count is a wrong number on the dashboard during exactly the incident
// an operator is looking at it for, so the truncation is now logged rather than
// silent.
func (sc *StatsCollector) scanQueueDepth(ctx context.Context, storage core.Storage) (map[string]*[2]int64, error) {
	const scanCap = 10000

	queueDepth := make(map[string]*[2]int64) // [pending, running]
	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning} {
		jobs, err := storage.GetJobsByStatus(ctx, status, scanCap)
		if err != nil {
			// Abort the whole sample rather than continue. Continuing persisted a row
			// in which the FAILED half read as a hard zero — pending=0 when pending was
			// merely unreadable — a fabricated number on the dashboard, which is the
			// exact defect this packet exists to remove. A missing minute is honest.
			return nil, fmt.Errorf("query %s jobs: %w", status, err)
		}
		if len(jobs) == scanCap {
			sc.logger.Warn("queue-depth sample truncated; the dashboard depth chart is an UNDERCOUNT",
				"status", status,
				"cap", scanCap,
				"hint", "this storage does not implement GetQueueDepthStats, so depth is counted by paging rows")
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
	return queueDepth, nil
}

func (sc *StatsCollector) prune(ctx context.Context) {
	if sc.retention > 0 {
		if _, err := sc.stats.PruneStats(ctx, time.Now().Add(-sc.retention)); err != nil {
			sc.logger.Error("failed to prune stats", "retention", sc.retention, "error", err)
		}
	}
}
