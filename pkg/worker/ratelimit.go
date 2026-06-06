package worker

import (
	"sync"
	"time"
)

const defaultRateLimitWindow = time.Second

type queueRateLimitConfig struct {
	PerSecond float64
	Burst     int
}

type tokenBucket struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	tokens    float64
	updatedAt time.Time
}

func newTokenBucket(perSecond float64, burst int, now time.Time) *tokenBucket {
	if perSecond <= 0 || burst <= 0 {
		return nil
	}
	return &tokenBucket{
		rate:      perSecond,
		burst:     float64(burst),
		tokens:    float64(burst),
		updatedAt: now,
	}
}

func (b *tokenBucket) hasToken(now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	return b.tokens >= 1
}

func (b *tokenBucket) tryConsume(now time.Time) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (b *tokenBucket) refund(now time.Time) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	b.tokens++
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

func (b *tokenBucket) refillLocked(now time.Time) {
	if b.updatedAt.IsZero() {
		b.updatedAt = now
		return
	}
	if now.Before(b.updatedAt) {
		b.updatedAt = now
		return
	}
	elapsed := now.Sub(b.updatedAt).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.updatedAt = now
}
