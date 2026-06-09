package main

import (
	"context"
	"fmt"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type paymentArgs struct {
	OrderID string `json:"order_id"`
	Cents   int    `json:"cents"`
}

type paymentResult struct {
	ReceiptID string `json:"receipt_id"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("typed-workflow.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	q := jobs.New(store)

	charge := jobs.Define(q, "typedCharge", func(_ context.Context, a paymentArgs) (paymentResult, error) {
		return paymentResult{ReceiptID: "receipt-" + a.OrderID}, nil
	})
	results := make(chan paymentResult, 1)
	processOrder := jobs.Define(q, "typedProcessOrder", func(ctx context.Context, a paymentArgs) (paymentResult, error) {
		receipt, err := charge.Call(ctx, a)
		if err != nil {
			return paymentResult{}, err
		}
		results <- receipt
		return receipt, nil
	})

	worker := jobs.NewWorker(q)
	go func() {
		_ = worker.Start(ctx)
	}()

	if _, err := processOrder.Enqueue(ctx, paymentArgs{OrderID: "ord-123", Cents: 5000}); err != nil {
		panic(err)
	}

	select {
	case got := <-results:
		fmt.Println(got.ReceiptID)
	case <-ctx.Done():
		panic(ctx.Err())
	}
}
