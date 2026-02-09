// Package security provides validation, sanitization, and limits for the jobs package.
//
// This package includes:
//   - Input validation for job type names and queue names
//   - Error message sanitization to prevent sensitive data leakage
//   - Clamping functions to enforce safe limits on retries and concurrency
//   - Security-related constants defining maximum sizes and counts
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which re-exports these functions.
package security
