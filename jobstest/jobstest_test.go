package jobstest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type emailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type fakeTB struct {
	mu       sync.Mutex
	errors   []string
	fatals   []string
	helpers  int
	cleanups []func()
	tempDirs []string
}

func (f *fakeTB) Helper() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.helpers++
}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fatals = append(f.fatals, sprintf(format, args...))
}

func (f *fakeTB) Cleanup(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, fn)
}

func (f *fakeTB) TempDir() string {
	dir, err := os.MkdirTemp("", "jobstest-*")
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tempDirs = append(f.tempDirs, dir)
	return dir
}

func (f *fakeTB) runCleanups() {
	f.mu.Lock()
	cleanups := append([]func(){}, f.cleanups...)
	tempDirs := append([]string{}, f.tempDirs...)
	f.mu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
	for _, dir := range tempDirs {
		_ = os.RemoveAll(dir)
	}
}

func (f *fakeTB) failed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.errors) > 0 || len(f.fatals) > 0
}

func sprintf(format string, args ...any) string {
	return strings.TrimSuffix(fmt.Sprintf(format, args...), "\n")
}

func TestNewQueueReturnsUsableMigratedQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })

	id, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com"})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	job := AssertEnqueued(t, q, "send-email", WithArgs(emailArgs{To: "a@example.com"}))
	require.NotNil(t, job)
	assert.Equal(t, id, job.ID)
}

func TestNewQueueCleanupRunsWithoutError(t *testing.T) {
	t.Parallel()

	tb := &fakeTB{}
	q, store := newQueueWithStorage(tb)
	require.False(t, tb.failed())
	require.NotNil(t, q)
	require.NotNil(t, store)

	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "cleanup@example.com"})
	require.NoError(t, err)

	tb.runCleanups()
	require.False(t, tb.failed())
}

func TestAssertEnqueuedPassesWhenMatchingJobExists(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com", Subject: "Welcome"}, jobs.QueueOpt("mail"))
	require.NoError(t, err)

	tb := &fakeTB{}
	job := assertEnqueued(tb, q, "send-email", false, InQueue("mail"), WithArgs(emailArgs{To: "a@example.com", Subject: "Welcome"}))
	require.NotNil(t, job)
	assert.False(t, tb.failed())
	assert.Greater(t, tb.helpers, 0)
}

func TestAssertEnqueuedFailsWhenAbsent(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	tb := &fakeTB{}

	job := assertEnqueued(tb, q, "send-email", false)
	assert.Nil(t, job)
	assert.Len(t, tb.errors, 1)
	assert.Empty(t, tb.fatals)
}

func TestRequireEnqueuedFailsFatallyWhenAbsent(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	tb := &fakeTB{}

	job := assertEnqueued(tb, q, "send-email", true)
	assert.Nil(t, job)
	assert.Empty(t, tb.errors)
	assert.Len(t, tb.fatals, 1)
}

func TestAssertEnqueuedFailsOnArgsMismatch(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com"})
	require.NoError(t, err)

	tb := &fakeTB{}
	job := assertEnqueued(tb, q, "send-email", false, WithArgs(emailArgs{To: "b@example.com"}))
	assert.Nil(t, job)
	assert.Len(t, tb.errors, 1)
}

func TestAssertEnqueuedFailsOnWrongQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com"}, jobs.QueueOpt("mail"))
	require.NoError(t, err)

	tb := &fakeTB{}
	job := assertEnqueued(tb, q, "send-email", false, InQueue("critical"))
	assert.Nil(t, job)
	assert.Len(t, tb.errors, 1)
}

func TestAssertEnqueuedCount(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com"})
	require.NoError(t, err)
	_, err = q.Enqueue(context.Background(), "send-email", emailArgs{To: "b@example.com"})
	require.NoError(t, err)

	tb := &fakeTB{}
	matches := assertEnqueuedCount(tb, q, "send-email", 2)
	assert.Len(t, matches, 2)
	assert.False(t, tb.failed())

	tb = &fakeTB{}
	_ = assertEnqueuedCount(tb, q, "send-email", 1)
	assert.Len(t, tb.errors, 1)
}

func TestAssertNotEnqueued(t *testing.T) {
	t.Parallel()

	q := NewQueue(t)
	q.Register("send-email", func(context.Context, emailArgs) error { return nil })
	_, err := q.Enqueue(context.Background(), "send-email", emailArgs{To: "a@example.com"})
	require.NoError(t, err)

	tb := &fakeTB{}
	assertNotEnqueued(tb, q, "welcome-email")
	assert.False(t, tb.failed())

	tb = &fakeTB{}
	assertNotEnqueued(tb, q, "send-email")
	assert.Len(t, tb.errors, 1)
}
