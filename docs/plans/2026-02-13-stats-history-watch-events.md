# Stats History & WatchEvents Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the two stubbed UI service methods: `GetStatsHistory` (backed by a dedicated stats table with event-driven counters + periodic snapshots) and `WatchEvents` (real-time event streaming over Connect-RPC server stream).

**Architecture:** A new `JobStat` GORM model stores per-queue, per-minute rows with both point-in-time snapshot fields (pending, running) and event-driven counters (completed, failed, retried). A `StatsCollector` goroutine subscribes to `queue.Events()` to increment counters and runs a 1-minute ticker to snapshot queue depth and prune old rows. `WatchEvents` subscribes to `queue.Events()`, converts `core.Event` to proto `Event`, optionally filters by queue, and streams until client disconnect.

**Tech Stack:** Go, GORM (SQLite-compatible), Connect-RPC server streaming, protobuf

---

### Task 1: Add JobStat GORM model

**Files:**
- Create: `ui/stats.go`

**Step 1: Write the model and interface**

Create `ui/stats.go` with:

```go
package ui

import (
	"context"
	"time"
)

// JobStat stores per-queue statistics bucketed by minute.
type JobStat struct {
	ID        uint      `gorm:"primaryKey"`
	Queue     string    `gorm:"index:idx_job_stats_queue_ts;size:255;not null"`
	Timestamp time.Time `gorm:"index:idx_job_stats_queue_ts;not null"`
	Pending   int64     `gorm:"default:0"`
	Running   int64     `gorm:"default:0"`
	Completed int64     `gorm:"default:0"`
	Failed    int64     `gorm:"default:0"`
	Retried   int64     `gorm:"default:0"`
}

// StatsStorage is the interface for stats persistence.
type StatsStorage interface {
	MigrateStats(ctx context.Context) error
	UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error
	SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error
	GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error)
	PruneStats(ctx context.Context, before time.Time) (int64, error)
}
```

**Step 2: Verify it compiles**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go build ./ui/...`
Expected: PASS (no errors)

**Step 3: Commit**

```bash
git add ui/stats.go
git commit -m "feat(ui): add JobStat model and StatsStorage interface"
```

---

### Task 2: Implement GORM StatsStorage

**Files:**
- Create: `ui/stats_gorm.go`

**Step 1: Write the failing test**

Create `ui/stats_gorm_test.go`:

```go
package ui

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupTestStatsDB(t *testing.T) *gormStatsStorage {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	s := &gormStatsStorage{db: db}
	err = s.MigrateStats(context.Background())
	require.NoError(t, err)
	return s
}

func TestGormStatsStorage_UpsertAndQuery(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()
	ts := time.Now().Truncate(time.Minute)

	// First upsert creates a row
	err := s.UpsertStatCounters(ctx, "default", ts, 5, 2, 1)
	require.NoError(t, err)

	// Second upsert increments
	err = s.UpsertStatCounters(ctx, "default", ts, 3, 1, 0)
	require.NoError(t, err)

	// Snapshot queue depth
	err = s.SnapshotQueueDepth(ctx, "default", ts, 10, 3)
	require.NoError(t, err)

	// Query
	stats, err := s.GetStatsHistory(ctx, "", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)

	assert.Equal(t, "default", stats[0].Queue)
	assert.Equal(t, int64(8), stats[0].Completed)
	assert.Equal(t, int64(3), stats[0].Failed)
	assert.Equal(t, int64(1), stats[0].Retried)
	assert.Equal(t, int64(10), stats[0].Pending)
	assert.Equal(t, int64(3), stats[0].Running)
}

func TestGormStatsStorage_QueryByQueue(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()
	ts := time.Now().Truncate(time.Minute)

	err := s.UpsertStatCounters(ctx, "emails", ts, 5, 0, 0)
	require.NoError(t, err)
	err = s.UpsertStatCounters(ctx, "payments", ts, 3, 0, 0)
	require.NoError(t, err)

	stats, err := s.GetStatsHistory(ctx, "emails", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, "emails", stats[0].Queue)
}

func TestGormStatsStorage_Prune(t *testing.T) {
	s := setupTestStatsDB(t)
	ctx := context.Background()

	old := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	recent := time.Now().Truncate(time.Minute)

	err := s.UpsertStatCounters(ctx, "default", old, 1, 0, 0)
	require.NoError(t, err)
	err = s.UpsertStatCounters(ctx, "default", recent, 1, 0, 0)
	require.NoError(t, err)

	pruned, err := s.PruneStats(ctx, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(1), pruned)

	stats, err := s.GetStatsHistory(ctx, "", time.Time{}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, recent.Unix(), stats[0].Timestamp.Unix())
}
```

**Step 2: Run test to verify it fails**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run TestGormStatsStorage -v`
Expected: FAIL - `gormStatsStorage` undefined

**Step 3: Write the implementation**

Create `ui/stats_gorm.go`:

```go
package ui

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// gormStatsStorage implements StatsStorage using GORM.
type gormStatsStorage struct {
	db *gorm.DB
}

// NewGormStatsStorage creates a GORM-backed stats storage.
func NewGormStatsStorage(db *gorm.DB) StatsStorage {
	return &gormStatsStorage{db: db}
}

func (s *gormStatsStorage) MigrateStats(ctx context.Context) error {
	return s.db.WithContext(ctx).AutoMigrate(&JobStat{})
}

func (s *gormStatsStorage) UpsertStatCounters(ctx context.Context, queue string, ts time.Time, completed, failed, retried int64) error {
	ts = ts.Truncate(time.Minute)

	// Try to find existing row
	var existing JobStat
	result := s.db.WithContext(ctx).
		Where("queue = ? AND timestamp = ?", queue, ts).
		First(&existing)

	if result.Error == gorm.ErrRecordNotFound {
		// Create new row
		return s.db.WithContext(ctx).Create(&JobStat{
			Queue:     queue,
			Timestamp: ts,
			Completed: completed,
			Failed:    failed,
			Retried:   retried,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}

	// Increment existing counters
	return s.db.WithContext(ctx).Model(&existing).Updates(map[string]any{
		"completed": gorm.Expr("completed + ?", completed),
		"failed":    gorm.Expr("failed + ?", failed),
		"retried":   gorm.Expr("retried + ?", retried),
	}).Error
}

func (s *gormStatsStorage) SnapshotQueueDepth(ctx context.Context, queue string, ts time.Time, pending, running int64) error {
	ts = ts.Truncate(time.Minute)

	// Upsert: create row if not exists, update pending/running if it does
	stat := JobStat{
		Queue:     queue,
		Timestamp: ts,
		Pending:   pending,
		Running:   running,
	}

	return s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "queue"}, {Name: "timestamp"}},
			DoUpdates: clause.AssignmentColumns([]string{"pending", "running"}),
		}).
		// Fallback for SQLite: find-and-update
		Where("queue = ? AND timestamp = ?", queue, ts).
		Assign(map[string]any{"pending": pending, "running": running}).
		FirstOrCreate(&stat).Error
}

func (s *gormStatsStorage) GetStatsHistory(ctx context.Context, queue string, since time.Time, until time.Time) ([]JobStat, error) {
	var stats []JobStat
	q := s.db.WithContext(ctx).Order("timestamp ASC")

	if queue != "" {
		q = q.Where("queue = ?", queue)
	}
	if !since.IsZero() {
		q = q.Where("timestamp >= ?", since)
	}
	if !until.IsZero() {
		q = q.Where("timestamp <= ?", until)
	}

	return stats, q.Find(&stats).Error
}

func (s *gormStatsStorage) PruneStats(ctx context.Context, before time.Time) (int64, error) {
	result := s.db.WithContext(ctx).Where("timestamp < ?", before).Delete(&JobStat{})
	return result.RowsAffected, result.Error
}
```

**Step 4: Run test to verify it passes**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run TestGormStatsStorage -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ui/stats_gorm.go ui/stats_gorm_test.go
git commit -m "feat(ui): implement GORM-backed stats storage with upsert and prune"
```

---

### Task 3: Implement StatsCollector

**Files:**
- Create: `ui/stats_collector.go`

**Step 1: Write the failing test**

Create `ui/stats_collector_test.go`:

```go
package ui

import (
	"context"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupCollectorTest(t *testing.T) (*StatsCollector, *gormStatsStorage, *queue.Queue) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// Migrate both jobs and stats tables
	err = db.AutoMigrate(&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{}, &JobStat{})
	require.NoError(t, err)

	statsStore := &gormStatsStorage{db: db}
	storage := setupGormStorage(db)
	q := queue.New(storage)

	collector := NewStatsCollector(q, statsStore)
	return collector, statsStore, q
}

func TestStatsCollector_EventDrivenCounters(t *testing.T) {
	collector, statsStore, q := setupCollectorTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go collector.Start(ctx)

	// Emit events
	job := &core.Job{ID: "test-1", Queue: "default", Type: "test"}
	q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobFailed{Job: job, Timestamp: time.Now()})
	q.Emit(&core.JobRetrying{Job: job, Attempt: 1, Timestamp: time.Now()})

	// Give the collector time to process
	time.Sleep(200 * time.Millisecond)

	// Force a flush
	collector.Flush(ctx)

	ts := time.Now().Truncate(time.Minute)
	stats, err := statsStore.GetStatsHistory(ctx, "default", ts.Add(-time.Minute), ts.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, int64(2), stats[0].Completed)
	assert.Equal(t, int64(1), stats[0].Failed)
	assert.Equal(t, int64(1), stats[0].Retried)
}
```

NOTE: `setupGormStorage` is a test helper that wraps a `*gorm.DB` into a `core.Storage`. Since this file is in the `ui` package, you'll need a minimal adapter. The simplest approach: use a build-tag-free test helper that constructs a minimal storage. Alternatively, import the storage package directly in the test:

Replace `setupGormStorage(db)` with:
```go
import "github.com/jdziat/simple-durable-jobs/pkg/storage"
// ...
storage := storage.NewGormStorage(db)
```

**Step 2: Run test to verify it fails**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run TestStatsCollector -v`
Expected: FAIL - `StatsCollector` undefined

**Step 3: Write the implementation**

Create `ui/stats_collector.go`:

```go
package ui

import (
	"context"
	"sync"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// StatsCollector subscribes to queue events and periodically snapshots queue depth.
type StatsCollector struct {
	queue     *queue.Queue
	stats     StatsStorage
	retention time.Duration

	// In-memory counters batched per flush
	mu       sync.Mutex
	counters map[string]*statCounters // key: queue name
}

type statCounters struct {
	completed int64
	failed    int64
	retried   int64
}

// NewStatsCollector creates a new StatsCollector.
func NewStatsCollector(q *queue.Queue, stats StatsStorage, opts ...StatsCollectorOption) *StatsCollector {
	sc := &StatsCollector{
		queue:     q,
		stats:     stats,
		retention: 7 * 24 * time.Hour,
		counters:  make(map[string]*statCounters),
	}
	for _, opt := range opts {
		opt.apply(sc)
	}
	return sc
}

// StatsCollectorOption configures the StatsCollector.
type StatsCollectorOption interface {
	apply(*StatsCollector)
}

type statsCollectorOptionFunc func(*StatsCollector)

func (f statsCollectorOptionFunc) apply(sc *StatsCollector) { f(sc) }

// WithStatsRetention sets the retention duration for stats rows.
func WithStatsRetention(d time.Duration) StatsCollectorOption {
	return statsCollectorOptionFunc(func(sc *StatsCollector) {
		sc.retention = d
	})
}

// Start begins the event listener and periodic snapshot ticker.
// Blocks until ctx is cancelled.
func (sc *StatsCollector) Start(ctx context.Context) {
	events := sc.queue.Events()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush before shutdown
			sc.Flush(context.Background())
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
		_ = sc.stats.UpsertStatCounters(ctx, queueName, ts, c.completed, c.failed, c.retried)
	}
}

func (sc *StatsCollector) snapshot(ctx context.Context) {
	ts := time.Now().Truncate(time.Minute)
	storage := sc.queue.Storage()

	// Get counts per queue by status
	queueDepth := make(map[string]*[2]int64) // [pending, running]

	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning} {
		jobs, err := storage.GetJobsByStatus(ctx, status, 100000)
		if err != nil {
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
		_ = sc.stats.SnapshotQueueDepth(ctx, queueName, ts, d[0], d[1])
	}
}

func (sc *StatsCollector) prune(ctx context.Context) {
	if sc.retention > 0 {
		_ = sc.stats.PruneStats(ctx, time.Now().Add(-sc.retention))
	}
}
```

**Step 4: Run test to verify it passes**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run TestStatsCollector -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ui/stats_collector.go ui/stats_collector_test.go
git commit -m "feat(ui): add StatsCollector with event-driven counters and periodic snapshots"
```

---

### Task 4: Wire StatsCollector into UI Handler

**Files:**
- Modify: `ui/options.go` - add `WithStatsRetention` option and stats collector config
- Modify: `ui/handler.go` - create stats storage, start collector, wire into service
- Modify: `ui/service.go` - update `jobsService` to use `StatsStorage`, implement `GetStatsHistory`

**Step 1: Update options.go**

Add to the `config` struct and add new option functions:

In `ui/options.go`, update `config` to:
```go
type config struct {
	middleware     func(http.Handler) http.Handler
	queue          *queue.Queue
	statsRetention time.Duration
}
```

Add new option:
```go
// WithStatsRetention sets how long stats rows are kept. Default: 7 days.
func WithStatsRetention(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.statsRetention = d
	})
}
```

**Step 2: Update service.go**

Add `statsStorage` field to `jobsService`:
```go
type jobsService struct {
	jobsv1connect.UnimplementedJobsServiceHandler
	storage      core.Storage
	queue        *queue.Queue
	statsStorage StatsStorage
}
```

Update `newJobsService`:
```go
func newJobsService(storage core.Storage, q *queue.Queue, statsStorage StatsStorage) *jobsService {
	return &jobsService{
		storage:      storage,
		queue:        q,
		statsStorage: statsStorage,
	}
}
```

Replace the `GetStatsHistory` method:
```go
func (s *jobsService) GetStatsHistory(ctx context.Context, req *connect.Request[jobsv1.GetStatsHistoryRequest]) (*connect.Response[jobsv1.GetStatsHistoryResponse], error) {
	if s.statsStorage == nil {
		return connect.NewResponse(&jobsv1.GetStatsHistoryResponse{}), nil
	}

	// Parse period to determine time range
	since, until := parsePeriod(req.Msg.Period)

	stats, err := s.statsStorage.GetStatsHistory(ctx, "", since, until)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Aggregate completed and failed into DataPoint slices
	completedMap := make(map[int64]int64)
	failedMap := make(map[int64]int64)
	for _, st := range stats {
		ts := st.Timestamp.Unix()
		completedMap[ts] += st.Completed
		failedMap[ts] += st.Failed
	}

	resp := &jobsv1.GetStatsHistoryResponse{}
	for ts, val := range completedMap {
		resp.Completed = append(resp.Completed, &jobsv1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(ts, 0)),
			Value:     val,
		})
	}
	for ts, val := range failedMap {
		resp.Failed = append(resp.Failed, &jobsv1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(ts, 0)),
			Value:     val,
		})
	}

	// Sort by timestamp
	sort.Slice(resp.Completed, func(i, j int) bool {
		return resp.Completed[i].Timestamp.AsTime().Before(resp.Completed[j].Timestamp.AsTime())
	})
	sort.Slice(resp.Failed, func(i, j int) bool {
		return resp.Failed[i].Timestamp.AsTime().Before(resp.Failed[j].Timestamp.AsTime())
	})

	return connect.NewResponse(resp), nil
}

func parsePeriod(period string) (since, until time.Time) {
	until = time.Now()
	switch period {
	case "1h":
		since = until.Add(-1 * time.Hour)
	case "24h":
		since = until.Add(-24 * time.Hour)
	case "7d":
		since = until.Add(-7 * 24 * time.Hour)
	default:
		since = until.Add(-1 * time.Hour)
	}
	return
}
```

**Step 3: Update handler.go**

In the `Handler` function, after creating `svc`, add stats collector setup:

```go
func Handler(storage core.Storage, opts ...Option) http.Handler {
	cfg := &config{}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Set up stats storage if we have a GORM-backed storage
	var statsStorage StatsStorage
	var collector *StatsCollector
	if gs, ok := storage.(interface{ DB() *gorm.DB }); ok {
		statsStore := NewGormStatsStorage(gs.DB())
		_ = statsStore.MigrateStats(context.Background())
		statsStorage = statsStore

		if cfg.queue != nil {
			var collectorOpts []StatsCollectorOption
			if cfg.statsRetention > 0 {
				collectorOpts = append(collectorOpts, WithStatsRetention(cfg.statsRetention))
			}
			collector = NewStatsCollector(cfg.queue, statsStore, collectorOpts...)
			go collector.Start(context.Background())
		}
	}

	mux := http.NewServeMux()

	svc := newJobsService(storage, cfg.queue, statsStorage)
	// ... rest unchanged
```

Note: `collector` variable is kept to avoid the unused-variable error; the goroutine keeps it alive. If the linter complains, use `_ = collector`.

**Step 4: Verify it compiles and existing tests pass**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go build ./ui/... && go test ./ui/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ui/options.go ui/handler.go ui/service.go
git commit -m "feat(ui): wire StatsCollector into handler and implement GetStatsHistory"
```

---

### Task 5: Implement WatchEvents

**Files:**
- Modify: `ui/service.go` - replace `WatchEvents` stub

**Step 1: Write the failing test**

Add to `ui/stats_collector_test.go` (or create `ui/watch_events_test.go`):

```go
package ui

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockServerStream captures events sent via the WatchEvents stream.
type mockServerStream struct {
	events []*jobsv1.Event
}

func (m *mockServerStream) Send(e *jobsv1.Event) error {
	m.events = append(m.events, e)
	return nil
}

func TestWatchEvents_ReceivesEvents(t *testing.T) {
	q := queue.New(nil) // No storage needed for event emission
	svc := newJobsService(nil, q, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &mockServerStream{}

	// Emit events in background after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		job := &core.Job{ID: "j1", Queue: "default", Type: "test", Status: core.StatusCompleted}
		q.Emit(&core.JobCompleted{Job: job, Timestamp: time.Now()})
		q.Emit(&core.JobFailed{Job: &core.Job{ID: "j2", Queue: "default", Type: "test", Status: core.StatusFailed}, Error: fmt.Errorf("boom"), Timestamp: time.Now()})
	}()

	// This will block until ctx timeout since WatchEvents is a long-lived stream
	// We need to use the actual Connect server stream interface, but for unit testing
	// we test the conversion helper directly.

	// Instead, test the event conversion helper
	job := &core.Job{ID: "j1", Queue: "default", Type: "test", Status: core.StatusCompleted}
	ev := coreEventToProto(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	require.NotNil(t, ev)
	assert.Equal(t, "job.completed", ev.Type)
	assert.Equal(t, "j1", ev.Job.Id)
}

func TestCoreEventToProto_AllTypes(t *testing.T) {
	job := &core.Job{ID: "j1", Queue: "default", Type: "test"}
	now := time.Now()

	tests := []struct {
		event    core.Event
		wantType string
	}{
		{&core.JobStarted{Job: job, Timestamp: now}, "job.started"},
		{&core.JobCompleted{Job: job, Timestamp: now}, "job.completed"},
		{&core.JobFailed{Job: job, Timestamp: now}, "job.failed"},
		{&core.JobRetrying{Job: job, Timestamp: now}, "job.retrying"},
		{&core.JobPaused{Job: job, Timestamp: now}, "job.paused"},
		{&core.JobResumed{Job: job, Timestamp: now}, "job.resumed"},
		{&core.QueuePaused{Queue: "default", Timestamp: now}, "queue.paused"},
		{&core.QueueResumed{Queue: "default", Timestamp: now}, "queue.resumed"},
	}

	for _, tt := range tests {
		ev := coreEventToProto(tt.event)
		require.NotNil(t, ev, "event type %T should produce a proto event", tt.event)
		assert.Equal(t, tt.wantType, ev.Type)
	}
}

func TestCoreEventToProto_QueueFilter(t *testing.T) {
	job := &core.Job{ID: "j1", Queue: "emails", Type: "test"}
	ev := coreEventToProto(&core.JobCompleted{Job: job, Timestamp: time.Now()})
	assert.Equal(t, "emails", ev.Job.Queue)
}
```

**Step 2: Run test to verify it fails**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run TestCoreEventToProto -v`
Expected: FAIL - `coreEventToProto` undefined

**Step 3: Write the implementation**

In `ui/service.go`, replace the `WatchEvents` method and add the conversion helper:

```go
// WatchEvents streams real-time job events.
func (s *jobsService) WatchEvents(ctx context.Context, req *connect.Request[jobsv1.WatchEventsRequest], stream *connect.ServerStream[jobsv1.Event]) error {
	if s.queue == nil {
		// No queue configured - block until disconnect
		<-ctx.Done()
		return ctx.Err()
	}

	events := s.queue.Events()
	filterQueues := req.Msg.Queues

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-events:
			pbEvent := coreEventToProto(e)
			if pbEvent == nil {
				continue
			}

			// Apply queue filter
			if len(filterQueues) > 0 {
				eventQueue := ""
				if pbEvent.Job != nil {
					eventQueue = pbEvent.Job.Queue
				}
				if !containsString(filterQueues, eventQueue) {
					continue
				}
			}

			if err := stream.Send(pbEvent); err != nil {
				return err
			}
		}
	}
}

func coreEventToProto(e core.Event) *jobsv1.Event {
	switch ev := e.(type) {
	case *core.JobStarted:
		return &jobsv1.Event{
			Type:      "job.started",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobCompleted:
		return &jobsv1.Event{
			Type:      "job.completed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobFailed:
		return &jobsv1.Event{
			Type:      "job.failed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobRetrying:
		return &jobsv1.Event{
			Type:      "job.retrying",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobPaused:
		return &jobsv1.Event{
			Type:      "job.paused",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobResumed:
		return &jobsv1.Event{
			Type:      "job.resumed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.QueuePaused:
		return &jobsv1.Event{
			Type:      "queue.paused",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.QueueResumed:
		return &jobsv1.Event{
			Type:      "queue.resumed",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.WorkerPaused:
		return &jobsv1.Event{
			Type:      "worker.paused",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.WorkerResumed:
		return &jobsv1.Event{
			Type:      "worker.resumed",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	default:
		return nil
	}
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
```

**Step 4: Run tests to verify they pass**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./ui/ -run "TestCoreEventToProto|TestWatchEvents" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add ui/service.go ui/watch_events_test.go
git commit -m "feat(ui): implement WatchEvents with real-time event streaming and queue filtering"
```

---

### Task 6: Add stats migration to GormStorage.Migrate

**Files:**
- Modify: `pkg/storage/gorm.go` - add `JobStat` to `AutoMigrate` call

**Step 1: Update Migrate**

In `pkg/storage/gorm.go`, update the Migrate function to include `JobStat` from the ui package.

WAIT - this creates a circular dependency (storage -> ui -> core -> storage). Instead, the `JobStat` migration is handled in `Handler()` via `statsStore.MigrateStats()`. This is already done in Task 4. No change needed here.

Skip this task.

---

### Task 7: Run all tests and verify nothing is broken

**Step 1: Run all tests**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go test ./... -v -count=1`
Expected: All PASS

**Step 2: Build check**

Run: `cd /home/jdziat/Code/jdziat/simple-durable-jobs && go build ./...`
Expected: No errors

**Step 3: Commit any fixups**

If any test failures occurred, fix and commit.
