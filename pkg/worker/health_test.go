package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

type healthStorage struct {
	*mockStorage
	pingCalls atomic.Int64
	pingErr   error
}

func (s *healthStorage) Ping(context.Context) error {
	s.pingCalls.Add(1)
	return s.pingErr
}

func TestWorkerHealthHandlerHealthzReturnsOKWithoutDBCall(t *testing.T) {
	store := &healthStorage{mockStorage: &mockStorage{}}
	w := NewWorker(queue.New(store))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	w.HealthHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int64(0), store.pingCalls.Load())
}

func TestWorkerHealthHandlerReadyzReturnsOKWhenStoragePingSucceedsWhilePaused(t *testing.T) {
	store := &healthStorage{mockStorage: &mockStorage{}}
	w := NewWorker(queue.New(store))
	w.Pause(core.PauseModeGraceful)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	w.HealthHandler().ServeHTTP(rec, req)

	require.True(t, w.IsPaused())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, int64(1), store.pingCalls.Load())
}

func TestWorkerHealthHandlerReadyzReturnsUnavailableWhenStoragePingFails(t *testing.T) {
	store := &healthStorage{
		mockStorage: &mockStorage{},
		pingErr:     errors.New("database down"),
	}
	w := NewWorker(queue.New(store))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	w.HealthHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, int64(1), store.pingCalls.Load())
}

func TestWorkerHealthStartedResetsAfterShutdown(t *testing.T) {
	store := &healthStorage{mockStorage: &mockStorage{}}
	w := NewWorker(queue.New(store))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Start(ctx)
	}()

	require.Eventually(t, func() bool {
		return w.Health().Started
	}, time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, w.Health().Started)
}

func ExampleWorker_HealthHandler() {
	store := &healthStorage{mockStorage: &mockStorage{}}
	w := NewWorker(queue.New(store))

	mux := http.NewServeMux()
	mux.Handle("/", w.HealthHandler())

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	fmt.Println(resp.StatusCode)

	// Output: 200
}
