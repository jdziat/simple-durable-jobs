package typed_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/worker"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type exampleArgs struct {
	UserID string
}

type exampleResult struct {
	Greeting string
}

func ExampleDef() {
	q := queue.New(nil)
	def := typed.Define(q, "typedExampleDef", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: "hello " + a.UserID}, nil
	})

	fmt.Println(def.Name())
	// Output: typedExampleDef
}

func ExampleDefine() {
	q := queue.New(nil)
	def := typed.Define(q, "typedExampleDefine", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: "hello " + a.UserID}, nil
	})

	fmt.Println(def.Name())
	// Output: typedExampleDefine
}

func ExampleDefineE() {
	q := queue.New(nil)
	def, err := typed.DefineE(q, "typedExampleDefineE", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: "hello " + a.UserID}, nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(def.Name())
	// Output: typedExampleDefineE
}

func ExampleDeclare() {
	q := queue.New(newExampleStorage())
	def := typed.Declare[exampleArgs, exampleResult](q, "typedExampleDeclaredRemote")

	jobID, err := def.EnqueueRemote(context.Background(), exampleArgs{UserID: "u-123"})
	if err != nil {
		panic(err)
	}
	fmt.Println(jobID != "")
	// Output: true
}

func ExampleDefineVoid() {
	q := queue.New(nil)
	def := typed.DefineVoid(q, "typedExampleDefineVoid", func(_ context.Context, _ exampleArgs) error {
		return nil
	})

	fmt.Println(def.Name())
	// Output: typedExampleDefineVoid
}

func ExampleDef_Name() {
	q := queue.New(nil)
	def := typed.Define(q, "typedExampleName", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: a.UserID}, nil
	})

	fmt.Println(def.Name())
	// Output: typedExampleName
}

func ExampleDef_Enqueue() {
	q := queue.New(newExampleStorage())
	def := typed.Define(q, "typedExampleEnqueue", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: a.UserID}, nil
	})

	jobID, err := def.Enqueue(context.Background(), exampleArgs{UserID: "u-123"})
	if err != nil {
		panic(err)
	}
	fmt.Println(jobID != "")
	// Output: true
}

func ExampleDef_EnqueueRemote() {
	q := queue.New(newExampleStorage())
	def := typed.Declare[exampleArgs, exampleResult](q, "typedExampleEnqueueRemote")

	jobID, err := def.EnqueueRemote(context.Background(), exampleArgs{UserID: "u-123"})
	if err != nil {
		panic(err)
	}
	fmt.Println(jobID != "")
	// Output: true
}

func ExampleDef_EnqueueTx() {
	q, store, cleanup := newExampleDurableQueue()
	defer cleanup()

	def := typed.Define(q, "typedExampleEnqueueTx", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: a.UserID}, nil
	})

	txStore, ok := store.(interface{ DB() *gorm.DB })
	if !ok {
		panic("example storage does not expose DB")
	}
	tx := txStore.DB().Begin()
	if tx.Error != nil {
		panic(tx.Error)
	}
	jobID, err := def.EnqueueTx(context.Background(), tx, exampleArgs{UserID: "u-123"})
	if err != nil {
		_ = tx.Rollback().Error
		panic(err)
	}
	if err := tx.Commit().Error; err != nil {
		panic(err)
	}
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		panic(err)
	}
	fmt.Println(job != nil)
	// Output: true
}

func ExampleDef_Call() {
	ctx := context.Background()
	q, store, cleanup := newExampleDurableQueue()
	defer cleanup()

	child := typed.Define(q, "typedExampleCallChild", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: "child:" + a.UserID}, nil
	})
	parent := typed.Define(q, "typedExampleCallParent", func(ctx context.Context, a exampleArgs) (exampleResult, error) {
		got, err := child.Call(ctx, a)
		if err != nil {
			return exampleResult{}, err
		}
		got.Greeting = "parent:" + got.Greeting
		return got, nil
	})

	jobID, err := parent.Enqueue(ctx, exampleArgs{UserID: "u-123"})
	if err != nil {
		panic(err)
	}
	runExampleWorkerUntilStatus(q, store, jobID, core.StatusCompleted)

	got, err := parent.Load(ctx, jobID)
	if err != nil {
		panic(err)
	}
	fmt.Println(got.Greeting)
	// Output: parent:child:u-123
}

func ExampleDef_Load() {
	q := queue.New(newExampleStorage())
	def := typed.Define(q, "typedExampleLoad", func(_ context.Context, a exampleArgs) (exampleResult, error) {
		return exampleResult{Greeting: a.UserID}, nil
	})

	_, err := def.Load(context.Background(), "missing")
	fmt.Println(err != nil)
	// Output: true
}

func newExampleDurableQueue() (*queue.Queue, core.Storage, func()) {
	path, err := os.MkdirTemp("", "typed-example-*")
	if err != nil {
		panic(err)
	}
	dbPath := filepath.Join(path, "jobs.db")
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate", dbPath)), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		_ = os.RemoveAll(path)
		panic(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		_ = os.RemoveAll(path)
		panic(err)
	}
	store := storage.NewGormStorage(db)
	if err := store.Migrate(context.Background()); err != nil {
		_ = sqlDB.Close()
		_ = os.RemoveAll(path)
		panic(err)
	}
	cleanup := func() {
		_ = sqlDB.Close()
		_ = os.RemoveAll(path)
	}
	return queue.New(store), store, cleanup
}

func runExampleWorkerUntilStatus(q *queue.Queue, store core.Storage, jobID string, want core.JobStatus) {
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
			panic(fmt.Sprintf("timed out waiting for job %s to reach %s", jobID, want))
		case err := <-errCh:
			if err != nil {
				panic(err)
			}
			panic(fmt.Sprintf("worker stopped before job %s reached %s", jobID, want))
		case <-ticker.C:
			job, err := store.GetJob(context.Background(), jobID)
			if err != nil {
				panic(err)
			}
			if job != nil && job.Status == want {
				cancel()
				err = <-errCh
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
				return
			}
		}
	}
}
