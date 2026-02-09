package storage

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

// PoolConfig holds connection pool configuration.
type PoolConfig struct {
	// MaxOpenConns is the maximum number of open connections to the database.
	// Default: 25
	MaxOpenConns int

	// MaxIdleConns is the maximum number of connections in the idle pool.
	// Default: 10
	MaxIdleConns int

	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	// Default: 5 minutes
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	// Default: 1 minute
	ConnMaxIdleTime time.Duration
}

// DefaultPoolConfig returns sensible defaults for connection pooling.
// These defaults are suitable for most workloads:
//   - MaxOpenConns: 25 - enough for concurrent workers without overwhelming DB
//   - MaxIdleConns: 10 - keeps warm connections ready
//   - ConnMaxLifetime: 5min - prevents stale connections
//   - ConnMaxIdleTime: 1min - reclaims unused connections
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 1 * time.Minute,
	}
}

// PoolOption configures connection pool settings.
type PoolOption interface {
	applyPool(*PoolConfig)
}

type poolOptionFunc func(*PoolConfig)

func (f poolOptionFunc) applyPool(c *PoolConfig) { f(c) }

// MaxOpenConns sets the maximum number of open connections.
// Set to 0 for unlimited (not recommended for production).
func MaxOpenConns(n int) PoolOption {
	return poolOptionFunc(func(c *PoolConfig) {
		c.MaxOpenConns = n
	})
}

// MaxIdleConns sets the maximum number of idle connections.
// Should be less than or equal to MaxOpenConns.
func MaxIdleConns(n int) PoolOption {
	return poolOptionFunc(func(c *PoolConfig) {
		c.MaxIdleConns = n
	})
}

// ConnMaxLifetime sets the maximum connection lifetime.
// Connections older than this are closed and replaced.
// Set to 0 for no limit (not recommended).
func ConnMaxLifetime(d time.Duration) PoolOption {
	return poolOptionFunc(func(c *PoolConfig) {
		c.ConnMaxLifetime = d
	})
}

// ConnMaxIdleTime sets the maximum idle time for connections.
// Idle connections older than this are closed.
// Set to 0 for no limit.
func ConnMaxIdleTime(d time.Duration) PoolOption {
	return poolOptionFunc(func(c *PoolConfig) {
		c.ConnMaxIdleTime = d
	})
}

// ConfigurePool applies pool configuration to a GORM database connection.
// Returns an error if the underlying *sql.DB cannot be retrieved.
func ConfigurePool(db *gorm.DB, opts ...PoolOption) error {
	config := DefaultPoolConfig()
	for _, opt := range opts {
		opt.applyPool(&config)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying *sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	return nil
}

// NewGormStorageWithPool creates a new GORM-backed storage with connection pooling configured.
// Uses DefaultPoolConfig() values, which can be overridden with PoolOption arguments.
//
// Example:
//
//	storage, err := NewGormStorageWithPool(db,
//	    MaxOpenConns(50),
//	    MaxIdleConns(20),
//	)
func NewGormStorageWithPool(db *gorm.DB, opts ...PoolOption) (*GormStorage, error) {
	if err := ConfigurePool(db, opts...); err != nil {
		return nil, err
	}
	return NewGormStorage(db), nil
}

// HighConcurrencyPoolConfig returns pool settings optimized for high-concurrency workloads.
// Use this when running many workers (50+) or processing high job volumes.
func HighConcurrencyPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    100,
		MaxIdleConns:    25,
		ConnMaxLifetime: 10 * time.Minute,
		ConnMaxIdleTime: 2 * time.Minute,
	}
}

// LowLatencyPoolConfig returns pool settings optimized for low-latency job processing.
// Keeps more idle connections warm to reduce connection acquisition time.
func LowLatencyPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    50,
		MaxIdleConns:    40, // Keep most connections warm
		ConnMaxLifetime: 15 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}
}

// ResourceConstrainedPoolConfig returns pool settings for limited database resources.
// Use this when the database has connection limits or limited resources.
func ResourceConstrainedPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 3 * time.Minute,
		ConnMaxIdleTime: 30 * time.Second,
	}
}
