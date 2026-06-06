// Package jobstest provides helpers for testing code that enqueues jobs.
//
// NewQueue creates an isolated SQLite-backed queue using the same concurrency
// safe SQLite pragmas documented for production tests. Assertion helpers query
// the queue storage directly and check whether matching jobs were inserted.
package jobstest
