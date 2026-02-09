// Package call provides the durable Call function for nested job operations.
//
// The Call[T] generic function allows jobs to make nested calls to other
// registered handlers. Results are automatically checkpointed, enabling
// crash recovery and replay without re-executing completed calls.
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which re-exports the Call function.
package call
