// Package context provides internal context helpers for job execution.
//
// This package is internal and should not be imported directly.
// It provides context value types for:
//   - Job context: Current job and queue references during handler execution
//   - Call state: Checkpoint tracking for durable calls
package context
