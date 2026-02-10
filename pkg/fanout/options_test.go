package fanout

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/stretchr/testify/assert"
)

func TestFailFast(t *testing.T) {
	cfg := defaultConfig()
	FailFast().apply(cfg)
	assert.Equal(t, core.StrategyFailFast, cfg.strategy)
}

func TestCollectAll(t *testing.T) {
	cfg := defaultConfig()
	CollectAll().apply(cfg)
	assert.Equal(t, core.StrategyCollectAll, cfg.strategy)
}

func TestThreshold(t *testing.T) {
	cfg := defaultConfig()
	Threshold(0.8).apply(cfg)
	assert.Equal(t, core.StrategyThreshold, cfg.strategy)
	assert.Equal(t, 0.8, cfg.threshold)
}

func TestWithQueue(t *testing.T) {
	cfg := defaultConfig()
	WithQueue("high-priority").apply(cfg)
	assert.Equal(t, "high-priority", cfg.queue)
}

func TestWithPriority(t *testing.T) {
	cfg := defaultConfig()
	WithPriority(10).apply(cfg)
	assert.Equal(t, 10, cfg.priority)
}

func TestWithRetries(t *testing.T) {
	cfg := defaultConfig()
	WithRetries(5).apply(cfg)
	assert.Equal(t, 5, cfg.retries)
}

func TestWithSubJobTimeout(t *testing.T) {
	cfg := defaultConfig()
	WithSubJobTimeout(5 * time.Minute).apply(cfg)
	assert.Equal(t, 5*time.Minute, cfg.subJobTimeout)
}

func TestWithTimeout(t *testing.T) {
	cfg := defaultConfig()
	WithTimeout(1 * time.Hour).apply(cfg)
	assert.Equal(t, 1*time.Hour, cfg.totalTimeout)
}

func TestCancelOnParentFailure(t *testing.T) {
	cfg := defaultConfig()
	CancelOnParentFailure().apply(cfg)
	assert.True(t, cfg.cancelOnFailure)
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	assert.Equal(t, core.StrategyFailFast, cfg.strategy)
	assert.Equal(t, 1.0, cfg.threshold)
	assert.Equal(t, 3, cfg.retries)
	assert.False(t, cfg.cancelOnFailure)
}

func TestMultipleOptions(t *testing.T) {
	cfg := defaultConfig()
	opts := []Option{
		CollectAll(),
		WithQueue("batch"),
		WithRetries(10),
		CancelOnParentFailure(),
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	assert.Equal(t, core.StrategyCollectAll, cfg.strategy)
	assert.Equal(t, "batch", cfg.queue)
	assert.Equal(t, 10, cfg.retries)
	assert.True(t, cfg.cancelOnFailure)
}
