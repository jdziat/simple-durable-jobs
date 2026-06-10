package jobs_test

import (
	"context"
	"fmt"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v2"
	typed "github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type exampleOrder struct {
	ID         uint `gorm:"primaryKey"`
	ExternalID string
}

type exampleProcessedOrder struct {
	ID      uint `gorm:"primaryKey"`
	OrderID string
}

type exampleTypedEmail struct {
	To string
}

type exampleTypedEmailResult struct {
	MessageID string
}

func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("send-email", func(ctx context.Context, to string) error {
		fmt.Printf("send email to %s\n", to)
		return nil
	})

	if _, err := q.Enqueue(ctx, "send-email", "user@example.com"); err != nil {
		panic(err)
	}

	// Illustrative only: start the worker, then shut down immediately so the
	// example terminates. A real program would block on Start until shutdown.
	worker := jobs.NewWorker(q)
	go func() {
		_ = worker.Start(ctx)
	}()
	cancel()
}

func ExampleDefine() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	sendEmail := typed.Define(q, "typed-send-email", func(ctx context.Context, args exampleTypedEmail) (exampleTypedEmailResult, error) {
		return exampleTypedEmailResult{MessageID: "msg_" + args.To}, nil
	})

	jobID, err := sendEmail.Enqueue(ctx, exampleTypedEmail{To: "user@example.com"})
	if err != nil {
		panic(err)
	}
	_, _ = sendEmail.Load(ctx, jobID)
}

// ExampleNew_enqueueOptions enqueues onto a named queue with per-job options.
func ExampleNew_enqueueOptions() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("send-email", func(ctx context.Context, to string) error {
		return nil
	})

	_, err = q.Enqueue(ctx, "send-email", "user@example.com",
		jobs.QueueOpt("mailers"),
		jobs.Retries(5),
		jobs.Delay(30*time.Second),
	)
	if err != nil {
		panic(err)
	}
}

func ExampleIdempotencyKey() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("charge-card", func(ctx context.Context, paymentID string) error {
		return nil
	})

	jobID, err := q.Enqueue(ctx, "charge-card", "pay_123",
		jobs.IdempotencyKey("request-abc", 24*time.Hour),
	)
	if err != nil {
		panic(err)
	}
	_ = jobID
}

func ExampleUniqueFor() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("sync-account", func(ctx context.Context, accountID string) error {
		return nil
	})

	jobID, err := q.Enqueue(ctx, "sync-account", "acct_123",
		jobs.UniqueFor(time.Hour),
	)
	if err != nil {
		panic(err)
	}
	_ = jobID
}

// ExampleNew_transactional enqueues a job atomically with a business-row
// write using a caller-supplied transaction: the job is committed iff the
// order row commits (transactional/outbox enqueue).
func ExampleNew_transactional() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	if err := db.AutoMigrate(&exampleOrder{}); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	// A worker (in this process or another) consumes the job once committed.
	q.Register("send-receipt", func(ctx context.Context, externalID string) error {
		return nil
	})

	tx := db.WithContext(ctx).Begin()
	if tx.Error != nil {
		panic(tx.Error)
	}
	defer tx.Rollback()

	order := exampleOrder{ExternalID: "order-123"}
	if err := tx.Create(&order).Error; err != nil {
		panic(err)
	}
	if _, err := q.EnqueueTx(ctx, tx, "send-receipt", order.ExternalID); err != nil {
		panic(err)
	}
	if err := tx.Commit().Error; err != nil {
		panic(err)
	}
}

func ExampleSavePhaseCheckpointTx() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	if err := db.AutoMigrate(&exampleProcessedOrder{}); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("process-order", func(ctx context.Context, orderID string) error {
		if _, ok := jobs.LoadPhaseCheckpoint[string](ctx, "mark-processed"); ok {
			return nil
		}

		tx := db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return tx.Error
		}
		defer tx.Rollback()

		if err := tx.Create(&exampleProcessedOrder{OrderID: orderID}).Error; err != nil {
			return err
		}
		if err := jobs.SavePhaseCheckpointTx(ctx, tx, "mark-processed", "done"); err != nil {
			return err
		}
		return tx.Commit().Error
	})

	if _, err := q.Enqueue(ctx, "process-order", "order-123"); err != nil {
		panic(err)
	}
}

func ExampleSignal() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("approval-workflow", func(ctx context.Context, _ struct{}) error {
		approved, err := jobs.WaitForSignal[bool](ctx, "approved")
		if err != nil {
			return err
		}
		if !approved {
			return fmt.Errorf("approval rejected")
		}
		return nil
	})

	jobID, err := q.Enqueue(ctx, "approval-workflow", struct{}{})
	if err != nil {
		panic(err)
	}
	if err := jobs.Signal(ctx, q, jobID, "approved", true); err != nil {
		panic(err)
	}
}

func ExampleCall() {
	ctx := context.Background()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}

	q := jobs.New(store)
	q.Register("calculate-total", func(ctx context.Context, orderID string) (int, error) {
		return 4200, nil
	})
	q.Register("invoice-order", func(ctx context.Context, orderID string) error {
		totalCents, err := jobs.Call[int](ctx, "calculate-total", orderID)
		if err != nil {
			return err
		}
		_ = totalCents
		return nil
	})

	if _, err := q.Enqueue(ctx, "invoice-order", "order-123"); err != nil {
		panic(err)
	}
}
