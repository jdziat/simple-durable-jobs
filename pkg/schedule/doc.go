// Package schedule provides scheduling implementations for recurring jobs.
//
// This package includes:
//   - Schedule interface for defining job schedules
//   - Every() for fixed-interval schedules
//   - Daily() for daily schedules at a specific time
//   - Weekly() for weekly schedules on a specific day and time
//   - Cron() for cron expression-based schedules
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which re-exports these functions.
package schedule
