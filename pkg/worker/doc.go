// Package worker provides the Worker type for job processing.
//
// This package includes:
//   - Worker: Processes jobs from the queue
//   - WorkerOption: Configuration options for workers
//   - Concurrency and queue configuration
//   - Scheduler for recurring jobs
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which provides access to worker configuration through queue.NewWorker().
package worker
