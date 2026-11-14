package storage

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// An EnqueueBatch of N jobs binds N x columns variables in one INSERT. SQLite
// refuses more than 32766 ("too many SQL variables"); PostgreSQL and MySQL stop at
// 65535. These tests pin that a batch of any size is split under the ceiling, in
// one transaction, without changing dedup, ordering or correction semantics.

// sqlRecorder is a gorm logger that keeps every statement it is asked to trace.
type sqlRecorder struct {
	mu   sync.Mutex
	sqls []string
}

func (r *sqlRecorder) LogMode(logger.LogLevel) logger.Interface      { return r }
func (r *sqlRecorder) Info(context.Context, string, ...interface{})  {}
func (r *sqlRecorder) Warn(context.Context, string, ...interface{})  {}
func (r *sqlRecorder) Error(context.Context, string, ...interface{}) {}
func (r *sqlRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	r.mu.Lock()
	r.sqls = append(r.sqls, sql)
	r.mu.Unlock()
}

func (r *sqlRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sqls...)
}

var insertJobsRE = regexp.MustCompile("(?i)INSERT INTO [\"`]?jobs[\"`]?[ (]")

func (r *sqlRecorder) jobInserts() int {
	n := 0
	for _, q := range r.all() {
		if insertJobsRE.MatchString(q) {
			n++
		}
	}
	return n
}

func (r *sqlRecorder) count(match func(string) bool) int {
	n := 0
	for _, q := range r.all() {
		if match(q) {
			n++
		}
	}
	return n
}

func (r *sqlRecorder) saw(substr string) bool {
	for _, q := range r.all() {
		if strings.Contains(strings.ToUpper(q), substr) {
			return true
		}
	}
	return false
}

func newChunkedStorage(t *testing.T, rows int) *GormStorage {
	t.Helper()
	s := newTestStorage(t)
	s.insertChunkRows = rows
	return s
}

func batchOf(n int, mk func(i int) *core.Job) []*core.Job {
	out := make([]*core.Job, n)
	for i := range out {
		out[i] = mk(i)
	}
	return out
}

func plainJob(i int) *core.Job {
	return &core.Job{Type: "chunk.plain", Queue: "default", Status: core.StatusPending, Priority: i % 7}
}

func countJobs(t *testing.T, s *GormStorage) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(&core.Job{}).Count(&n).Error)
	return n
}

func TestJobInsertChunkRows_StaysUnderEveryBindCeiling(t *testing.T) {
	s := newTestStorage(t)

	stmt := &gorm.Statement{DB: s.db}
	require.NoError(t, stmt.Parse(&core.Job{}))
	cols := len(stmt.Schema.DBNames)
	require.Greater(t, cols, 0)

	rows := s.jobInsertChunkRows(s.db)
	assert.GreaterOrEqual(t, rows, 1)
	assert.LessOrEqual(t, rows, insertMaxChunkRow)
	assert.Less(t, rows*cols, 32766, "a full chunk must fit SQLite's bind-variable ceiling (%d rows x %d columns)", rows, cols)
	assert.Less(t, rows*cols, 65535, "a full chunk must fit PostgreSQL's and MySQL's ceiling")
}

func TestEnqueueBatchChunking_SingleStatementBelowTheCap(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	rec := &sqlRecorder{}
	s = NewGormStorage(s.db.Session(&gorm.Session{Logger: rec}), WithHotStatsCacheTTL(0))

	jobs := batchOf(100, plainJob)
	require.NoError(t, s.EnqueueBatch(ctx, jobs))

	assert.Equal(t, 1, rec.jobInserts(), "a batch that fits in one chunk must stay a single INSERT")
	assert.False(t, rec.saw("SAVEPOINT"), "a single-chunk batch needs no savepoint")
}

// The headline: sizes either side of SQLite's 963-row limit, and one past
// PostgreSQL's and MySQL's, must all succeed.
func TestEnqueueBatchChunking_LargeBatchSizes(t *testing.T) {
	for _, n := range []int{963, 964, 2500} {
		n := n
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			ctx := context.Background()
			s := newTestStorage(t)

			jobs := batchOf(n, plainJob)
			require.NoError(t, s.EnqueueBatch(ctx, jobs))
			assert.EqualValues(t, n, countJobs(t, s))

			seen := make(map[core.UUID]bool, n)
			for _, j := range jobs {
				require.NotEmpty(t, j.ID)
				seen[j.ID] = true
			}
			assert.Len(t, seen, n, "every job must keep its own id")
		})
	}
}

func TestEnqueueBatchChunking_CrossChunkSemantics(t *testing.T) {
	t.Run("a duplicate id straddling a chunk boundary keeps the first job", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)

		shared := core.NewID()
		jobs := batchOf(8, plainJob)
		jobs[0].ID, jobs[0].Type, jobs[0].MaxRetries = shared, "first", 2
		jobs[7].ID, jobs[7].Type, jobs[7].MaxRetries, jobs[7].MaxRetriesSet = shared, "last", 0, true

		require.NoError(t, s.EnqueueBatch(ctx, jobs))

		var got []core.Job
		require.NoError(t, s.db.Where("id = ?", shared).Find(&got).Error)
		require.Len(t, got, 1)
		assert.Equal(t, "first", got[0].Type)
		assert.Equal(t, 2, got[0].MaxRetries, "the suppressed job's Retries(0) must not land on the survivor")
		assert.EqualValues(t, 7, countJobs(t, s))
	})

	t.Run("a duplicate unique key straddling a chunk boundary repoints to the survivor", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)

		jobs := batchOf(8, plainJob)
		jobs[1].UniqueKey = "chunk-key"
		jobs[6].UniqueKey = "chunk-key"

		require.NoError(t, s.EnqueueBatch(ctx, jobs))

		assert.Equal(t, jobs[1].ID, jobs[6].ID, "the duplicate must resolve to the surviving row's id")
		var n int64
		require.NoError(t, s.db.Model(&core.Job{}).Where("unique_key = ?", "chunk-key").Count(&n).Error)
		assert.EqualValues(t, 1, n)
		assert.EqualValues(t, 7, countJobs(t, s))
	})

	t.Run("a stranger holding an id in a later chunk is not reconfigured", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)

		stranger := &core.Job{ID: core.NewID(), Type: "stranger", Queue: "default", Status: core.StatusPending, MaxRetries: 7}
		require.NoError(t, s.Enqueue(ctx, stranger))

		jobs := batchOf(8, plainJob)
		jobs[6].ID, jobs[6].Type, jobs[6].MaxRetries, jobs[6].MaxRetriesSet = stranger.ID, "intruder", 0, true

		require.NoError(t, s.EnqueueBatch(ctx, jobs))

		var got core.Job
		require.NoError(t, s.db.Where("id = ?", stranger.ID).First(&got).Error)
		assert.Equal(t, "stranger", got.Type)
		assert.Equal(t, 7, got.MaxRetries, "the corrective UPDATE rewrote a row the batch did not create")
	})

	t.Run("Retries(0) and a delayed job in a non-first chunk are applied", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)

		future := time.Now().Add(time.Hour)
		jobs := batchOf(8, plainJob)
		jobs[5].MaxRetries, jobs[5].MaxRetriesSet = 0, true
		jobs[6].RunAt = &future

		require.NoError(t, s.EnqueueBatch(ctx, jobs))

		var zero, delayed core.Job
		require.NoError(t, s.db.Where("id = ?", jobs[5].ID).First(&zero).Error)
		require.NoError(t, s.db.Where("id = ?", jobs[6].ID).First(&delayed).Error)
		assert.Equal(t, 0, zero.MaxRetries, "an explicit Retries(0) in chunk 2 must reach the row")
		assert.False(t, delayed.DQReady, "a delayed job in chunk 3 must not be dequeue-ready")

		var ready int64
		require.NoError(t, s.db.Model(&core.Job{}).Where("dq_ready = ?", true).Count(&ready).Error)
		assert.EqualValues(t, 7, ready)
	})

	t.Run("replaying a large batch creates no duplicates", func(t *testing.T) {
		ctx := context.Background()
		s := newTestStorage(t)

		jobs := batchOf(2500, func(i int) *core.Job {
			j := plainJob(i)
			if i%100 == 0 {
				j.MaxRetries, j.MaxRetriesSet = 0, true
			}
			return j
		})
		require.NoError(t, s.EnqueueBatch(ctx, jobs))
		require.NoError(t, s.EnqueueBatch(ctx, jobs), "an at-least-once replay of the same batch must succeed")

		assert.EqualValues(t, 2500, countJobs(t, s))
		var zeros int64
		require.NoError(t, s.db.Model(&core.Job{}).Where("max_retries = ?", 0).Count(&zeros).Error)
		assert.EqualValues(t, 25, zeros)
	})
}

// The unique-key lookup and the two corrective UPDATEs bind one variable per id and
// are chunked at retentionDeleteChunkSize ids. A batch larger than that exercises
// every chunk of each.
func TestEnqueueBatchChunking_CorrectionAndLookupListsSpanChunks(t *testing.T) {
	ctx := context.Background()
	base := newTestStorage(t)

	existing := &core.Job{ID: core.NewID(), Type: "existing", Queue: "default", Status: core.StatusPending, UniqueKey: "uk-2300"}
	require.NoError(t, base.Enqueue(ctx, existing))

	rec := &sqlRecorder{}
	s := NewGormStorage(base.db.Session(&gorm.Session{Logger: rec}), WithHotStatsCacheTTL(0))

	future := time.Now().Add(time.Hour)
	const n = 2500
	jobs := batchOf(n, func(i int) *core.Job {
		j := plainJob(i)
		j.UniqueKey = "uk-" + strconv.Itoa(i)
		j.MaxRetries, j.MaxRetriesSet = 0, true
		j.RunAt = &future
		return j
	})
	require.NoError(t, s.EnqueueBatch(ctx, jobs))

	// 2500 ids at 1000 per statement is three statements for each list.
	assert.Equal(t, 3, rec.count(func(q string) bool {
		return strings.Contains(q, "unique_key") && strings.HasPrefix(strings.ToUpper(q), "SELECT")
	}),
		"the unique-key lookup must be split into chunks")
	assert.Equal(t, 3, rec.count(func(q string) bool {
		return strings.Contains(q, "max_retries") && strings.HasPrefix(strings.ToUpper(q), "UPDATE")
	}),
		"the Retries(0) correction must be split into chunks")
	assert.Equal(t, 3, rec.count(func(q string) bool {
		return strings.Contains(q, "dq_ready") && strings.HasPrefix(strings.ToUpper(q), "UPDATE")
	}),
		"the dq_ready correction must be split into chunks")

	assert.Equal(t, existing.ID, jobs[2300].ID, "the lookup must find the active job in a later key chunk")
	assert.EqualValues(t, n, countJobs(t, base), "2499 new rows plus the one that already existed")

	var withRetries, ready int64
	require.NoError(t, s.db.Model(&core.Job{}).Where("max_retries <> ? AND type = ?", 0, "chunk.plain").Count(&withRetries).Error)
	require.NoError(t, s.db.Model(&core.Job{}).Where("dq_ready = ? AND type = ?", true, "chunk.plain").Count(&ready).Error)
	assert.EqualValues(t, 0, withRetries, "every explicit Retries(0) must be applied across update chunks")
	assert.EqualValues(t, 0, ready, "every delayed job must be not-ready across update chunks")
}

// failingCreate makes every INSERT whose rows include a job of type "boom" fail.
func failingCreate(t *testing.T, s *GormStorage) {
	t.Helper()
	const name = "test:fail-boom"
	require.NoError(t, s.db.Callback().Create().Before("gorm:create").Register(name, func(db *gorm.DB) {
		if rows, ok := db.Statement.Dest.([]*core.Job); ok {
			for _, r := range rows {
				if r != nil && r.Type == "boom" {
					_ = db.AddError(errors.New("boom: injected insert failure"))
					return
				}
			}
		}
	}))
	t.Cleanup(func() { _ = s.db.Callback().Create().Remove(name) })
}

func TestEnqueueBatchChunking_FailureInALaterChunkPersistsNothing(t *testing.T) {
	t.Run("EnqueueBatch", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)
		failingCreate(t, s)

		jobs := batchOf(8, plainJob)
		jobs[7].Type = "boom"
		require.Error(t, s.EnqueueBatch(ctx, jobs))
		assert.EqualValues(t, 0, countJobs(t, s))
	})

	t.Run("a caller-owned transaction that ignores the error and commits", func(t *testing.T) {
		ctx := context.Background()
		s := newChunkedStorage(t, 3)
		failingCreate(t, s)

		rec := &sqlRecorder{}
		tx := s.db.Session(&gorm.Session{Logger: rec}).Begin()
		require.NoError(t, tx.Error)

		jobs := batchOf(8, plainJob)
		jobs[7].Type = "boom"
		require.Error(t, s.EnqueueBatchTx(ctx, tx, jobs))
		assert.True(t, rec.saw("SAVEPOINT"), "a multi-chunk insert must run inside a savepoint")

		// The outer transaction must still be usable after the inner failure, and
		// must keep none of the earlier chunks. PostgreSQL would reject this
		// follow-up statement without the savepoint rollback.
		require.NoError(t, s.EnqueueBatchTx(ctx, tx, batchOf(1, plainJob)))
		require.NoError(t, tx.Commit().Error)

		assert.EqualValues(t, 1, countJobs(t, s),
			"chunks 1 and 2 survived a failure in chunk 3 inside a caller-owned transaction")
	})
}

// failNthDQReadyUpdate makes the nth UPDATE of dq_ready fail. The corrective UPDATE
// is chunked at retentionDeleteChunkSize ids, so n=2 fails the second chunk.
func failNthDQReadyUpdate(t *testing.T, s *GormStorage, n int) {
	t.Helper()
	const name = "test:fail-dq-update"
	seen := 0
	require.NoError(t, s.db.Callback().Update().Before("gorm:update").Register(name, func(db *gorm.DB) {
		if m, ok := db.Statement.Dest.(map[string]interface{}); ok {
			if _, has := m["dq_ready"]; has {
				seen++
				if seen == n {
					_ = db.AddError(errors.New("boom: injected dq_ready update failure"))
				}
			}
		}
	}))
	t.Cleanup(func() { _ = s.db.Callback().Update().Remove(name) })
}

// The corrective UPDATEs run after the inserts. They share the inserts' savepoint,
// so a failure in a later UPDATE chunk cannot leave delayed jobs persisted with
// dq_ready=true (which a worker would dequeue early). 1500 ids is two UPDATE chunks
// and three insert chunks at the derived size.
func TestEnqueueBatchChunking_FailureInACorrectiveUpdateChunkPersistsNothing(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	failNthDQReadyUpdate(t, s, 2)

	future := time.Now().Add(time.Hour)
	jobs := batchOf(retentionDeleteChunkSize+500, func(i int) *core.Job {
		j := plainJob(i)
		j.RunAt = &future
		return j
	})

	tx := s.db.Begin()
	require.NoError(t, tx.Error)
	require.Error(t, s.EnqueueBatchTx(ctx, tx, jobs))
	_ = tx.Commit()

	assert.EqualValues(t, 0, countJobs(t, s),
		"a failure in the second corrective UPDATE chunk left the inserted rows persisted")
}

func TestEnqueueBatchChunking_FanOutWithManySubJobs(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	const worker = "w1"
	const subs = 2500

	parentID := seedRunningLockedParent(t, ctx, s, worker)
	fanOut, cp, children := buildFanOutFixtures(parentID, subs)
	require.NoError(t, s.SuspendForFanOut(ctx, parentID, worker, fanOut, cp, children))

	parent, err := s.GetJob(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, core.StatusWaiting, parent.Status)

	got, err := s.GetSubJobs(ctx, fanOut.ID)
	require.NoError(t, err)
	require.Len(t, got, subs)

	// Fan-out counts are derived from the sub-jobs' statuses; completing every
	// sub-job must read back as a fully completed fan-out at this size.
	require.NoError(t, s.db.Model(&core.Job{}).
		Where("fan_out_id = ?", fanOut.ID).
		UpdateColumn("status", core.StatusCompleted).Error)

	fo, err := s.GetFanOut(ctx, fanOut.ID)
	require.NoError(t, err)
	require.NotNil(t, fo)
	assert.Equal(t, subs, fo.TotalCount)
	assert.Equal(t, subs, fo.CompletedCount)
}
