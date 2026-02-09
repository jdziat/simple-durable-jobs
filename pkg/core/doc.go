// Package core provides the fundamental types and interfaces for the jobs package.
//
// This package contains:
//   - Job and Checkpoint data models with GORM annotations
//   - Storage interface defining the persistence contract
//   - Event types for queue monitoring
//   - Error types for job processing
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// instead of this package directly.
package core
