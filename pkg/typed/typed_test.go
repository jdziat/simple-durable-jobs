package typed_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/pkg/codec"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/pkg/typed"
	"github.com/jdziat/simple-durable-jobs/pkg/worker"
)

type args struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type result struct {
	Message string `json:"message"`
	Total   int    `json:"total"`
}

func TestTypedPackageDoesNotImportRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "list", "-deps", ".")
	cmd.Dir = "."
	out, err := cmd.Output()
	require.NoError(t, err)

	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		assert.NotEqual(t, "github.com/jdziat/simple-durable-jobs", dep)
	}
}

func TestTypedEnqueueMatchesStringlyArgsBytes(t *testing.T) {
	ctx := context.Background()
	q, store := newTestQueue(t, nil)

	def := typed.Define(q, "goldenTyped", func(_ context.Context, a args) (result, error) {
		return result{Message: a.Name, Total: a.Count}, nil
	})

	value := args{Name: "alpha", Count: 7}
	typedID, err := def.Enqueue(ctx, value)
	require.NoError(t, err)
	stringlyID, err := q.Enqueue(ctx, def.Name(), value)
	require.NoError(t, err)

	typedJob, err := store.GetJob(ctx, typedID)
	require.NoError(t, err)
	stringlyJob, err := store.GetJob(ctx, stringlyID)
	require.NoError(t, err)

	assert.True(t, bytes.Equal(stringlyJob.Args, typedJob.Args), "typed enqueue must preserve stringly JSON bytes")
}

func TestDeclareEnqueueRemoteDoesNotRequireRegistration(t *testing.T) {
	ctx := context.Background()
	q, store := newTestQueue(t, nil)

	def := typed.Declare[args, result](q, "remoteTyped")
	jobID, err := def.EnqueueRemote(ctx, args{Name: "producer", Count: 2})
	require.NoError(t, err)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, def.Name(), job.Type)
}

func TestSecretboxCodecRoundTripsTypedResult(t *testing.T) {
	ctx := context.Background()
	var key [32]byte
	key[0] = 42
	secretbox, err := codec.NewSecretbox(key)
	require.NoError(t, err)

	q, store := newTestQueue(t, []storage.GormStorageOption{storage.WithCodec(secretbox)})
	def := typed.Define(q, "secretboxRoundTrip", func(_ context.Context, a args) (result, error) {
		return result{Message: "sealed:" + a.Name, Total: a.Count + 1}, nil
	})

	jobID, err := def.Enqueue(ctx, args{Name: "payload", Count: 4})
	require.NoError(t, err)
	runWorkerUntilStatus(t, q, store, jobID, core.StatusCompleted)

	got, err := def.Load(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, result{Message: "sealed:payload", Total: 5}, got)
}

func TestDefineEnqueueLoadAndNestedCall(t *testing.T) {
	ctx := context.Background()
	q, store := newTestQueue(t, nil)

	child := typed.Define(q, "childTyped", func(_ context.Context, a args) (result, error) {
		return result{Message: "child:" + a.Name, Total: a.Count + 10}, nil
	})
	parent := typed.Define(q, "parentTyped", func(ctx context.Context, a args) (result, error) {
		got, err := child.Call(ctx, a)
		if err != nil {
			return result{}, err
		}
		got.Message = "parent:" + got.Message
		got.Total++
		return got, nil
	})

	jobID, err := parent.Enqueue(ctx, args{Name: "nested", Count: 3})
	require.NoError(t, err)
	runWorkerUntilStatus(t, q, store, jobID, core.StatusCompleted)

	got, err := parent.Load(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, result{Message: "parent:child:nested", Total: 14}, got)
}

func TestDefineVoid(t *testing.T) {
	ctx := context.Background()
	q, store := newTestQueue(t, nil)
	var seen args

	def := typed.DefineVoid(q, "voidTyped", func(_ context.Context, a args) error {
		seen = a
		return nil
	})

	jobID, err := def.Enqueue(ctx, args{Name: "side-effect", Count: 1})
	require.NoError(t, err)
	runWorkerUntilStatus(t, q, store, jobID, core.StatusCompleted)

	got, err := def.Load(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, struct{}{}, got)
	assert.Equal(t, args{Name: "side-effect", Count: 1}, seen)
}

func TestCompileTimeTypeMismatchFixture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "build", "./testdata/compilefail")
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	require.Error(t, err, "fixture must fail to compile")
	assert.Contains(t, string(out), "cannot use")
}

func newTestQueue(t *testing.T, opts []storage.GormStorageOption) (*queue.Queue, core.Storage) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "jobs.db")
	db, err := gorm.Open(sqlite.Open(sqliteDSN(path)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("remove sqlite file: %v", err)
			}
		}
	})

	store := storage.NewGormStorage(db, opts...)
	require.NoError(t, store.Migrate(context.Background()))
	return queue.New(store), store
}

func sqliteDSN(path string) string {
	return fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate", path)
}

func runWorkerUntilStatus(t *testing.T, q *queue.Queue, store core.Storage, jobID string, want core.JobStatus) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w := worker.NewWorker(q, worker.WithPollInterval(50*time.Millisecond))
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Start(ctx)
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for job %s to reach %s", jobID, want)
		case err := <-errCh:
			require.NoError(t, err)
			t.Fatalf("worker stopped before job %s reached %s", jobID, want)
		case <-ticker.C:
			job, err := store.GetJob(context.Background(), jobID)
			require.NoError(t, err)
			if job != nil && job.Status == want {
				cancel()
				err = <-errCh
				if err != nil && !errors.Is(err, context.Canceled) {
					require.NoError(t, err)
				}
				return
			}
		}
	}
}
