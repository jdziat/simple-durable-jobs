// Package main demonstrates durable workflows with checkpointing.
//
// This example shows how to build multi-step workflows where each step
// is checkpointed. If the workflow fails mid-execution, it will resume
// from the last successful checkpoint on retry.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	// Setup database
	db, err := gorm.Open(sqlite.Open("workflow.db"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	storage := jobs.NewGormStorage(db)
	if err := storage.Migrate(context.Background()); err != nil {
		log.Fatal(err)
	}

	queue := jobs.New(storage)

	// Register step handlers
	queue.Register("validate-order", func(ctx context.Context, order Order) (Order, error) {
		fmt.Printf("  [Step 1] Validating order %s...\n", order.ID)
		time.Sleep(100 * time.Millisecond)

		if order.Total <= 0 {
			return order, errors.New("invalid order total")
		}

		order.Status = "validated"
		fmt.Printf("  [Step 1] Order %s validated\n", order.ID)
		return order, nil
	})

	queue.Register("charge-payment", func(ctx context.Context, order Order) (PaymentResult, error) {
		fmt.Printf("  [Step 2] Charging payment for order %s ($%.2f)...\n", order.ID, order.Total)
		time.Sleep(200 * time.Millisecond)

		// Simulate occasional payment failures
		if rand.Float32() < 0.3 {
			fmt.Printf("  [Step 2] Payment failed (will retry)\n")
			return PaymentResult{}, errors.New("payment gateway timeout")
		}

		result := PaymentResult{
			TransactionID: fmt.Sprintf("txn_%d", time.Now().UnixNano()),
			Amount:        order.Total,
			Status:        "charged",
		}
		fmt.Printf("  [Step 2] Payment successful: %s\n", result.TransactionID)
		return result, nil
	})

	queue.Register("ship-order", func(ctx context.Context, args ShipArgs) (string, error) {
		fmt.Printf("  [Step 3] Shipping order %s...\n", args.OrderID)
		time.Sleep(150 * time.Millisecond)

		trackingNumber := fmt.Sprintf("TRACK%d", time.Now().UnixNano()%1000000)
		fmt.Printf("  [Step 3] Order shipped: %s\n", trackingNumber)
		return trackingNumber, nil
	})

	queue.Register("send-confirmation", func(ctx context.Context, args ConfirmationArgs) error {
		fmt.Printf("  [Step 4] Sending confirmation to %s...\n", args.Email)
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("  [Step 4] Confirmation sent\n")
		return nil
	})

	// Register the main workflow that orchestrates the steps
	queue.Register("process-order", func(ctx context.Context, order Order) error {
		fmt.Printf("\n=== Processing Order %s ===\n", order.ID)

		// Step 1: Validate order (checkpointed)
		validatedOrder, err := jobs.Call[Order](ctx, "validate-order", order)
		if err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}

		// Step 2: Charge payment (checkpointed)
		// If this fails and retries, step 1 won't re-execute
		payment, err := jobs.Call[PaymentResult](ctx, "charge-payment", validatedOrder)
		if err != nil {
			return fmt.Errorf("payment failed: %w", err)
		}

		// Step 3: Ship order (checkpointed)
		trackingNumber, err := jobs.Call[string](ctx, "ship-order", ShipArgs{
			OrderID:       order.ID,
			TransactionID: payment.TransactionID,
		})
		if err != nil {
			return fmt.Errorf("shipping failed: %w", err)
		}

		// Step 4: Send confirmation (checkpointed)
		_, err = jobs.Call[any](ctx, "send-confirmation", ConfirmationArgs{
			Email:          order.CustomerEmail,
			OrderID:        order.ID,
			TrackingNumber: trackingNumber,
		})
		if err != nil {
			return fmt.Errorf("confirmation failed: %w", err)
		}

		fmt.Printf("=== Order %s Complete ===\n\n", order.ID)
		return nil
	})

	// Enqueue some orders
	ctx := context.Background()

	orders := []Order{
		{ID: "ORD-001", CustomerEmail: "alice@example.com", Total: 99.99},
		{ID: "ORD-002", CustomerEmail: "bob@example.com", Total: 149.50},
		{ID: "ORD-003", CustomerEmail: "charlie@example.com", Total: 299.00},
	}

	for _, order := range orders {
		id, err := queue.Enqueue(ctx, "process-order", order, jobs.Retries(5))
		if err != nil {
			log.Printf("Failed to enqueue: %v", err)
			continue
		}
		fmt.Printf("Enqueued workflow %s for order %s\n", id[:8], order.ID)
	}

	// Start worker
	workerCtx, cancel := context.WithCancel(ctx)
	worker := queue.NewWorker(jobs.WorkerQueue("default", jobs.Concurrency(2)))

	go func() {
		fmt.Println("\nStarting worker...")
		worker.Start(workerCtx)
	}()

	// Wait for interrupt or completion
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		time.Sleep(30 * time.Second)
		sigCh <- syscall.SIGTERM
	}()

	<-sigCh
	fmt.Println("\nShutting down...")
	cancel()
	time.Sleep(500 * time.Millisecond)

	// Show checkpoint stats
	var checkpointCount int64
	db.Raw("SELECT COUNT(*) FROM checkpoints").Scan(&checkpointCount)
	fmt.Printf("Total checkpoints created: %d\n", checkpointCount)
}

type Order struct {
	ID            string  `json:"id"`
	CustomerEmail string  `json:"customer_email"`
	Total         float64 `json:"total"`
	Status        string  `json:"status"`
}

type PaymentResult struct {
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
}

type ShipArgs struct {
	OrderID       string `json:"order_id"`
	TransactionID string `json:"transaction_id"`
}

type ConfirmationArgs struct {
	Email          string `json:"email"`
	OrderID        string `json:"order_id"`
	TrackingNumber string `json:"tracking_number"`
}
