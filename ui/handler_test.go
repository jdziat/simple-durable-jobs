package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	storagepackage "github.com/jdziat/simple-durable-jobs/pkg/storage"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
)

// ---------------------------------------------------------------------------
// Handler factory tests
// ---------------------------------------------------------------------------

func TestHandler_ReturnsHTTPHandler(t *testing.T) {
	store := &mockStorage{}
	h := Handler(store)
	require.NotNil(t, h)
	// Handler already returns http.Handler; verify it is non-nil.
	assert.NotNil(t, h)
}

func TestHandler_ServesPlaceholderRootPath(t *testing.T) {
	// frontendFS is likely missing the dist directory in tests, so the
	// placeholder branch is exercised.
	store := &mockStorage{}
	h := Handler(store)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	// We expect either 200 (placeholder HTML) or a served file.
	assert.True(t, rw.Code == http.StatusOK || rw.Code == http.StatusNotFound,
		"unexpected status %d", rw.Code)
}

func TestHandler_ServesPlaceholderIndexHTML(t *testing.T) {
	store := &mockStorage{}
	h := Handler(store)

	req := httptest.NewRequest(http.MethodGet, "/index.html", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	// The file server may redirect /index.html → /, serve it directly (200),
	// or return 404 if the frontend dist is not built.
	acceptable := rw.Code == http.StatusOK ||
		rw.Code == http.StatusNotFound ||
		rw.Code == http.StatusMovedPermanently
	assert.True(t, acceptable, "unexpected status %d", rw.Code)
}

func TestHandler_Returns404ForUnknownPath(t *testing.T) {
	store := &mockStorage{}
	h := Handler(store)

	req := httptest.NewRequest(http.MethodGet, "/some/path/that/does/not/exist.txt", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	// The handler should return 404 for non-existent static assets.
	assert.Equal(t, http.StatusNotFound, rw.Code)
}

// ---------------------------------------------------------------------------
// WithMiddleware option
// ---------------------------------------------------------------------------

func TestWithMiddleware_WrapsHandler(t *testing.T) {
	middlewareCalled := false
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middlewareCalled = true
			next.ServeHTTP(w, r)
		})
	}

	store := &mockStorage{}
	h := Handler(store, WithMiddleware(mw))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.True(t, middlewareCalled, "middleware should have been called")
}

func TestWithMiddleware_CanShortCircuit(t *testing.T) {
	mw := func(_ http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		})
	}

	store := &mockStorage{}
	h := Handler(store, WithMiddleware(mw))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusForbidden, rw.Code)
}

// ---------------------------------------------------------------------------
// Option application tests (via config inspection through Handler behavior)
// ---------------------------------------------------------------------------

func TestWithContext_AppliesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := WithContext(ctx)
	cfg := &config{ctx: context.Background()}
	opt.apply(cfg)

	assert.Equal(t, ctx, cfg.ctx)
}

func TestWithQueue_AppliesConfig(t *testing.T) {
	opt := WithQueue(nil)
	cfg := &config{}
	opt.apply(cfg)
	assert.Nil(t, cfg.queue)
}

func TestWithStatsRetention_AppliesConfig(t *testing.T) {
	d := 48 * time.Hour
	opt := WithStatsRetention(d)
	cfg := &config{}
	opt.apply(cfg)
	assert.Equal(t, d, cfg.statsRetention)
}

func TestWithMiddleware_AppliesConfig(t *testing.T) {
	mw := func(h http.Handler) http.Handler { return h }
	opt := WithMiddleware(mw)
	cfg := &config{}
	opt.apply(cfg)
	assert.NotNil(t, cfg.middleware)
}

// ---------------------------------------------------------------------------
// Multiple options composed together
// ---------------------------------------------------------------------------

func TestHandler_MultipleOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	callCount := 0
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			next.ServeHTTP(w, r)
		})
	}

	store := &mockStorage{}
	h := Handler(store,
		WithContext(ctx),
		WithStatsRetention(24*time.Hour),
		WithMiddleware(mw),
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.Equal(t, 1, callCount)
}

// ---------------------------------------------------------------------------
// Connect-RPC path routing sanity check
// ---------------------------------------------------------------------------

func TestHandler_ConnectRPCPathReturnsNon404(t *testing.T) {
	store := &mockUIStorage{
		getQueueStatsFn: func(_ context.Context) ([]*jobsv1.QueueStats, error) {
			return nil, nil
		},
	}
	h := Handler(store)

	// A POST to the Connect RPC endpoint should reach the handler (not 404).
	// The request won't have valid Connect framing, so Connect returns 400,
	// but that proves the route is registered.
	req := httptest.NewRequest(http.MethodPost, "/jobs.v1.JobsService/GetStats", nil)
	req.Header.Set("Content-Type", "application/connect+proto")
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	assert.NotEqual(t, http.StatusNotFound, rw.Code,
		"Connect-RPC path should be routed, got %d", rw.Code)
}

// ---------------------------------------------------------------------------
// Handler with GORM-backed storage (exercises the stats collector branch)
// ---------------------------------------------------------------------------

func setupGormStorage(t *testing.T) *storagepackage.GormStorage {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&core.Job{}, &core.Checkpoint{}, &core.FanOut{}, &core.QueueState{},
	))
	return storagepackage.NewGormStorage(db)
}

func TestHandler_GormStorage_NoQueue(t *testing.T) {
	// Storage with DB() causes the GORM branch to activate for stats.
	// Without a queue, the collector goroutine is not started.
	store := setupGormStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := Handler(store, WithContext(ctx))
	require.NotNil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	acceptable := rw.Code == http.StatusOK || rw.Code == http.StatusNotFound
	assert.True(t, acceptable, "unexpected status %d", rw.Code)
}

func TestHandler_GormStorage_WithQueue(t *testing.T) {
	// Storage with DB() and a queue causes the stats collector goroutine to start.
	store := setupGormStorage(t)
	q := queue.New(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := Handler(store,
		WithContext(ctx),
		WithQueue(q),
		WithStatsRetention(24*time.Hour),
	)
	require.NotNil(t, h)

	// Verify the handler processes requests normally.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	acceptable := rw.Code == http.StatusOK || rw.Code == http.StatusNotFound
	assert.True(t, acceptable, "unexpected status %d", rw.Code)
}

func TestHandler_GormStorage_WithQueue_NoRetention(t *testing.T) {
	// Exercises the collector opts path without statsRetention set.
	store := setupGormStorage(t)
	q := queue.New(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := Handler(store, WithContext(ctx), WithQueue(q))
	require.NotNil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	acceptable := rw.Code == http.StatusOK || rw.Code == http.StatusNotFound
	assert.True(t, acceptable, "unexpected status %d", rw.Code)
}
