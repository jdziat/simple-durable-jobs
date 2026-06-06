// Package jobstest provides test fixtures and enqueue assertions for jobs.
package jobstest

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const assertionQueryLimit = 10000

type testHelper interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Cleanup(func())
	TempDir() string
}

// Option configures a Queue fixture.
type Option interface {
	apply(*fixtureConfig)
}

type optionFunc func(*fixtureConfig)

func (f optionFunc) apply(c *fixtureConfig) { f(c) }

type fixtureConfig struct {
	gormConfig    *gorm.Config
	storageOpts   []jobs.GormStorageOption
	sqliteDSNFunc func(dbPath string) string
}

func defaultFixtureConfig() fixtureConfig {
	return fixtureConfig{
		gormConfig: &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		},
		sqliteDSNFunc: func(dbPath string) string {
			return dbPath + "?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"
		},
	}
}

// WithGormConfig sets the GORM config used to open the fixture database.
func WithGormConfig(config *gorm.Config) Option {
	return optionFunc(func(c *fixtureConfig) {
		c.gormConfig = config
	})
}

// WithStorageOptions passes options to jobs.NewGormStorage.
func WithStorageOptions(opts ...jobs.GormStorageOption) Option {
	return optionFunc(func(c *fixtureConfig) {
		c.storageOpts = append(c.storageOpts, opts...)
	})
}

// NewQueue returns an isolated, migrated SQLite-backed Queue for tests.
func NewQueue(t testing.TB, opts ...Option) *jobs.Queue {
	t.Helper()
	q, _ := newQueueWithStorage(t, opts...)
	return q
}

// NewQueueWithStorage returns an isolated Queue and its Storage for tests.
func NewQueueWithStorage(t testing.TB, opts ...Option) (*jobs.Queue, jobs.Storage) {
	t.Helper()
	return newQueueWithStorage(t, opts...)
}

func newQueueWithStorage(t testHelper, opts ...Option) (*jobs.Queue, jobs.Storage) {
	t.Helper()

	cfg := defaultFixtureConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	db, err := gorm.Open(sqlite.Open(cfg.sqliteDSNFunc(dbPath)), cfg.gormConfig)
	if err != nil {
		t.Fatalf("jobstest: open sqlite fixture: %v", err)
		return nil, nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("jobstest: get sqlite fixture handle: %v", err)
		return nil, nil
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	store := jobs.NewGormStorage(db, cfg.storageOpts...)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("jobstest: migrate sqlite fixture: %v", err)
		return nil, nil
	}
	return jobs.New(store), store
}

// MatchOption filters enqueue assertions.
type MatchOption interface {
	applyMatch(*matchConfig)
}

type matchOptionFunc func(*matchConfig)

func (f matchOptionFunc) applyMatch(c *matchConfig) { f(c) }

type matchConfig struct {
	queue       string
	hasQueue    bool
	expected    any
	hasExpected bool
}

// InQueue matches jobs enqueued to the named queue.
func InQueue(name string) MatchOption {
	return matchOptionFunc(func(c *matchConfig) {
		c.queue = name
		c.hasQueue = true
	})
}

// WithArgs matches jobs whose JSON arguments equal expected.
func WithArgs(expected any) MatchOption {
	return matchOptionFunc(func(c *matchConfig) {
		c.expected = expected
		c.hasExpected = true
	})
}

// AssertEnqueued asserts that at least one job of jobType was inserted.
func AssertEnqueued(t testing.TB, q *jobs.Queue, jobType string, opts ...MatchOption) *jobs.Job {
	t.Helper()
	return assertEnqueued(t, q, jobType, false, opts...)
}

// RequireEnqueued is like AssertEnqueued but stops the test on mismatch.
func RequireEnqueued(t testing.TB, q *jobs.Queue, jobType string, opts ...MatchOption) *jobs.Job {
	t.Helper()
	return assertEnqueued(t, q, jobType, true, opts...)
}

// AssertEnqueuedCount asserts that exactly n matching jobs were inserted.
func AssertEnqueuedCount(t testing.TB, q *jobs.Queue, jobType string, n int, opts ...MatchOption) []*jobs.Job {
	t.Helper()
	return assertEnqueuedCount(t, q, jobType, n, opts...)
}

// AssertNotEnqueued asserts that no matching job was inserted.
func AssertNotEnqueued(t testing.TB, q *jobs.Queue, jobType string, opts ...MatchOption) {
	t.Helper()
	assertNotEnqueued(t, q, jobType, opts...)
}

func assertEnqueued(t testHelper, q *jobs.Queue, jobType string, require bool, opts ...MatchOption) *jobs.Job {
	t.Helper()
	matches, err := findMatchingJobs(context.Background(), q, jobType, opts...)
	if err != nil {
		failf(t, require, "jobstest: AssertEnqueued(%q) query failed: %v", jobType, err)
		return nil
	}
	if len(matches) == 0 {
		failf(t, require, "jobstest: expected at least 1 enqueued %q job%s, found 0", jobType, describeMatchOptions(opts))
		return nil
	}
	return matches[0]
}

func assertEnqueuedCount(t testHelper, q *jobs.Queue, jobType string, n int, opts ...MatchOption) []*jobs.Job {
	t.Helper()
	matches, err := findMatchingJobs(context.Background(), q, jobType, opts...)
	if err != nil {
		t.Errorf("jobstest: AssertEnqueuedCount(%q, %d) query failed: %v", jobType, n, err)
		return nil
	}
	if len(matches) != n {
		t.Errorf("jobstest: expected exactly %d enqueued %q job%s, found %d", n, jobType, describeMatchOptions(opts), len(matches))
	}
	return matches
}

func assertNotEnqueued(t testHelper, q *jobs.Queue, jobType string, opts ...MatchOption) {
	t.Helper()
	matches, err := findMatchingJobs(context.Background(), q, jobType, opts...)
	if err != nil {
		t.Errorf("jobstest: AssertNotEnqueued(%q) query failed: %v", jobType, err)
		return
	}
	if len(matches) != 0 {
		t.Errorf("jobstest: expected no enqueued %q job%s, found %d", jobType, describeMatchOptions(opts), len(matches))
	}
}

func failf(t testHelper, require bool, format string, args ...any) {
	if require {
		t.Fatalf(format, args...)
		return
	}
	t.Errorf(format, args...)
}

func findMatchingJobs(ctx context.Context, q *jobs.Queue, jobType string, opts ...MatchOption) ([]*jobs.Job, error) {
	cfg := matchConfig{}
	for _, opt := range opts {
		opt.applyMatch(&cfg)
	}

	candidates, err := insertedJobs(ctx, q.Storage())
	if err != nil {
		return nil, err
	}

	matches := make([]*jobs.Job, 0)
	for _, job := range candidates {
		if job.Type != jobType {
			continue
		}
		if cfg.hasQueue && job.Queue != cfg.queue {
			continue
		}
		if cfg.hasExpected {
			ok, err := argsEqual(job.Args, cfg.expected)
			if err != nil {
				return nil, fmt.Errorf("match args for job %s: %w", job.ID, err)
			}
			if !ok {
				continue
			}
		}
		matches = append(matches, job)
	}
	return matches, nil
}

func insertedJobs(ctx context.Context, store jobs.Storage) ([]*jobs.Job, error) {
	statuses := []jobs.JobStatus{
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusCompleted,
		jobs.StatusFailed,
		jobs.StatusRetrying,
		jobs.StatusWaiting,
		jobs.StatusCancelled,
		jobs.StatusPaused,
	}

	all := make([]*jobs.Job, 0)
	for _, status := range statuses {
		jobList, err := store.GetJobsByStatus(ctx, status, assertionQueryLimit)
		if err != nil {
			return nil, err
		}
		all = append(all, jobList...)
	}
	return all, nil
}

func argsEqual(stored []byte, expected any) (bool, error) {
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return false, fmt.Errorf("marshal expected args: %w", err)
	}

	var storedValue any
	if err := json.Unmarshal(stored, &storedValue); err != nil {
		return false, fmt.Errorf("decode stored args: %w", err)
	}

	var expectedValue any
	if err := json.Unmarshal(expectedJSON, &expectedValue); err != nil {
		return false, fmt.Errorf("decode expected args: %w", err)
	}

	return reflect.DeepEqual(storedValue, expectedValue), nil
}

func describeMatchOptions(opts []MatchOption) string {
	cfg := matchConfig{}
	for _, opt := range opts {
		opt.applyMatch(&cfg)
	}

	var parts []string
	if cfg.hasQueue {
		parts = append(parts, fmt.Sprintf("queue %q", cfg.queue))
	}
	if cfg.hasExpected {
		parts = append(parts, "matching args")
	}
	if len(parts) == 0 {
		return ""
	}
	return " with " + strings.Join(parts, " and ")
}
