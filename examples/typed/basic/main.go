package main

import (
	"context"
	"fmt"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type emailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type emailResult struct {
	MessageID string `json:"message_id"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("typed-basic.db")), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	store := jobs.NewGormStorage(db)
	if err := store.Migrate(ctx); err != nil {
		panic(err)
	}
	q := jobs.New(store)

	sendEmail := jobs.Define[emailArgs, emailResult](q, "typedSendEmail", func(_ context.Context, a emailArgs) (emailResult, error) {
		return emailResult{MessageID: "msg-" + a.To}, nil
	})

	results := make(chan emailResult, 1)
	runExample := jobs.Define[emailArgs, emailResult](q, "typedSendEmailExample", func(ctx context.Context, a emailArgs) (emailResult, error) {
		got, err := sendEmail.Call(ctx, a)
		if err != nil {
			return emailResult{}, err
		}
		results <- got
		return got, nil
	})

	worker := jobs.NewWorker(q)
	go func() {
		_ = worker.Start(ctx)
	}()

	if _, err := runExample.Enqueue(ctx, emailArgs{To: "user@example.com", Subject: "Welcome"}); err != nil {
		panic(err)
	}

	select {
	case got := <-results:
		fmt.Println(got.MessageID)
	case <-ctx.Done():
		panic(ctx.Err())
	}
}
