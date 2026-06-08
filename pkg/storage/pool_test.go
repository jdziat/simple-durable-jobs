package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	assert.Equal(t, 25, cfg.MaxOpenConns)
	assert.Equal(t, 10, cfg.MaxIdleConns)
	assert.Equal(t, 5*time.Minute, cfg.ConnMaxLifetime)
	assert.Equal(t, 1*time.Minute, cfg.ConnMaxIdleTime)
}

func TestHighConcurrencyPoolConfig(t *testing.T) {
	cfg := HighConcurrencyPoolConfig()

	assert.Equal(t, 100, cfg.MaxOpenConns)
	assert.Equal(t, 25, cfg.MaxIdleConns)
	assert.Equal(t, 10*time.Minute, cfg.ConnMaxLifetime)
	assert.Equal(t, 2*time.Minute, cfg.ConnMaxIdleTime)
}

func TestLowLatencyPoolConfig(t *testing.T) {
	cfg := LowLatencyPoolConfig()

	assert.Equal(t, 50, cfg.MaxOpenConns)
	assert.Equal(t, 40, cfg.MaxIdleConns)
	assert.Equal(t, 15*time.Minute, cfg.ConnMaxLifetime)
	assert.Equal(t, 5*time.Minute, cfg.ConnMaxIdleTime)
}

func TestResourceConstrainedPoolConfig(t *testing.T) {
	cfg := ResourceConstrainedPoolConfig()

	assert.Equal(t, 10, cfg.MaxOpenConns)
	assert.Equal(t, 5, cfg.MaxIdleConns)
	assert.Equal(t, 3*time.Minute, cfg.ConnMaxLifetime)
	assert.Equal(t, 30*time.Second, cfg.ConnMaxIdleTime)
}

func TestPoolOptions(t *testing.T) {
	cfg := PoolConfig{}

	MaxOpenConns(50).applyPool(&cfg)
	assert.Equal(t, 50, cfg.MaxOpenConns)

	MaxIdleConns(20).applyPool(&cfg)
	assert.Equal(t, 20, cfg.MaxIdleConns)

	ConnMaxLifetime(10 * time.Minute).applyPool(&cfg)
	assert.Equal(t, 10*time.Minute, cfg.ConnMaxLifetime)

	ConnMaxIdleTime(2 * time.Minute).applyPool(&cfg)
	assert.Equal(t, 2*time.Minute, cfg.ConnMaxIdleTime)
}

func TestConfigurePool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	err = ConfigurePool(db,
		MaxOpenConns(30),
		MaxIdleConns(15),
		ConnMaxLifetime(7*time.Minute),
		ConnMaxIdleTime(90*time.Second),
	)
	require.NoError(t, err)

	// Verify settings were applied
	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 30, stats.MaxOpenConnections)
	// Note: MaxIdleConns and timeouts aren't exposed via Stats()
}

func TestConfigurePool_DefaultValues(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// Configure with no options - should use defaults
	err = ConfigurePool(db)
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 25, stats.MaxOpenConnections) // Default
}

func TestNewGormStorageWithPool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	storage, err := NewGormStorageWithPool(db,
		MaxOpenConns(40),
		MaxIdleConns(20),
	)
	require.NoError(t, err)
	require.NotNil(t, storage)

	// Verify pool was configured
	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 40, stats.MaxOpenConnections)
}

func TestNewGormStorageWithPool_DefaultPool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// No options - uses defaults
	storage, err := NewGormStorageWithPool(db)
	require.NoError(t, err)
	require.NotNil(t, storage)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 25, stats.MaxOpenConnections)
}

func TestNewGormStorage_AppliesBoundedSQLiteDefault(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	storage := NewGormStorage(db)
	require.NotNil(t, storage)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 4, stats.MaxOpenConnections)
	assert.NotEqual(t, 0, stats.MaxOpenConnections)
}

func TestApplyDefaultPool_NonSQLiteBranch(t *testing.T) {
	// Use a sqlite DB but exercise the non-SQLite branch directly so the
	// PG/MySQL default value (25) is covered without a live server.
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	applyDefaultPoolIfUnset(db, false)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 25, stats.MaxOpenConnections)
}

func TestApplyDefaultPool_SQLiteBranch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	applyDefaultPoolIfUnset(db, true)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 4, stats.MaxOpenConnections)
}

func TestNewGormStorage_RespectsPreconfiguredPool(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	// Caller bounds the pool BEFORE constructing storage (mirrors
	// benchmarks/testdb pre-setting the pool); the auto-default must not clobber it.
	sqlDB.SetMaxOpenConns(7)

	storage := NewGormStorage(db)
	require.NotNil(t, storage)

	stats := sqlDB.Stats()
	assert.Equal(t, 7, stats.MaxOpenConnections)
}

func TestNewGormStorage_LaterOverrideWins(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	storage := NewGormStorage(db)
	require.NotNil(t, storage)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	// Caller overrides AFTER construction; later wins.
	sqlDB.SetMaxOpenConns(3)

	stats := sqlDB.Stats()
	assert.Equal(t, 3, stats.MaxOpenConnections)
}

func TestNewGormStorageWithPool_UnlimitedPreserved(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	// Explicit unlimited (MaxOpenConns(0)) must survive: the withoutDefaultPool
	// skip path keeps it from being re-bounded by the auto-default.
	storage, err := NewGormStorageWithPool(db, MaxOpenConns(0))
	require.NoError(t, err)
	require.NotNil(t, storage)

	sqlDB, err := db.DB()
	require.NoError(t, err)

	stats := sqlDB.Stats()
	assert.Equal(t, 0, stats.MaxOpenConnections)
}

func TestNewGormStorage_NilDBNoPanic(t *testing.T) {
	require.NotPanics(t, func() {
		storage := NewGormStorage(nil)
		require.NotNil(t, storage)
	})
}

func TestPoolOptionFunc_ImplementsInterface(t *testing.T) {
	var opt PoolOption = poolOptionFunc(func(c *PoolConfig) {
		c.MaxOpenConns = 99
	})

	cfg := PoolConfig{}
	opt.applyPool(&cfg)

	assert.Equal(t, 99, cfg.MaxOpenConns)
}
