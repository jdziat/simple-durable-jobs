package main

import (
	"context"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
)

type args struct {
	Name string
}

func main() {
	q := queue.New(nil)
	def := typed.Define(q, "compileTyped", func(context.Context, args) (string, error) {
		return "", nil
	})
	_, _ = def.Enqueue(context.Background(), "not args")
}
