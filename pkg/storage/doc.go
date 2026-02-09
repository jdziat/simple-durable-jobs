// Package storage provides storage implementations for job persistence.
//
// This package includes:
//   - GormStorage: A GORM-based implementation supporting various databases
//
// The Storage interface is defined in pkg/core and must be implemented
// by any custom storage backend.
//
// Most users should import the root package github.com/jdziat/simple-durable-jobs
// which provides NewGormStorage() to create storage instances.
package storage
