// Package queue provides the Queue type for job orchestration.
//
// This package includes:
//   - Queue: The main orchestrator for registering handlers and enqueueing jobs
//   - Option: Configuration options for job enqueueing
//   - Hook registration for job lifecycle events
//   - Event subscription for monitoring
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which re-exports Queue and all option functions.
package queue
