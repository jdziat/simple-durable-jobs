package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sync"
	"time"
)

const defaultRateLimitWindow = time.Second

// maxRateLimitNameLen bounds the effective fleet-rate-limit name. The
// rate_limit_windows.limit_name column is varchar(255); an unbounded RateLimitKey
// that overflowed it would error TryConsumeRate and hot-loop the job
// (claim/deny/release forever) (teardown g4).
const maxRateLimitNameLen = 255

// rateReleaser is the optional storage capability for refunding one consumed fleet
// rate-limit unit. The worker uses it to return units it already consumed when a
// LATER fleet limit in the same admission denies the job (teardown g4). Backends
// that do not implement it simply skip the refund (prior behavior).
type rateReleaser interface {
	ReleaseRate(ctx context.Context, limitName string, window time.Duration) error
}

// windowedRateLimiter is the precise consume+refund capability: consume returns
// the window_start the increment committed to, and ReleaseRateAt refunds that
// EXACT window. Backends that implement it (GormStorage, all dialects) get a
// refund that targets the consume's own window even when a window rollover
// happens between consume and refund — closing the off-by-one where the plain
// rateReleaser path refunds "now"'s window instead. Backends without it fall back
// to the rateReleaser path (documented as approximate across a rollover boundary).
type windowedRateLimiter interface {
	TryConsumeRateWindow(ctx context.Context, limitName string, perSecond float64, window time.Duration, now time.Time) (bool, time.Time, error)
	ReleaseRateAt(ctx context.Context, limitName string, windowStart time.Time) error
}

// resolveRateLimitWindow chooses the fixed-window length for a fleet rate limit. An
// explicit author-set Window is always honored. Otherwise, for a FRACTIONAL
// PerSecond (< 1) the default 1s window rounds the per-window ceiling UP to 1
// (ceil(0.1*1)=1), admitting ~1/sec instead of 0.1/sec — up to ~10x over the
// configured rate (teardown g4). Derive a window large enough that
// PerSecond*window >= 1 so the ceiling is exact (0.1/sec -> a 10s window, ceiling 1
// per 10s). Whole-number rates keep the 1s default.
func (w *Worker) resolveRateLimitWindow(limit RateLimitConfig) time.Duration {
	if limit.Window > 0 {
		return limit.Window
	}
	if limit.PerSecond > 0 && limit.PerSecond < 1 {
		return time.Duration(math.Ceil(1/limit.PerSecond)) * time.Second
	}
	return defaultRateLimitWindow
}

// boundRateLimitName returns a name guaranteed to fit the limit_name column. Short
// names pass through unchanged for readability; an over-long effective name (from
// an unbounded RateLimitKey) is replaced by a stable hash so a long key buckets
// consistently instead of overflowing the column and hot-looping the job.
func boundRateLimitName(name string) string {
	if len(name) <= maxRateLimitNameLen {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return "h:" + hex.EncodeToString(sum[:]) // 2 + 64 = 66 chars, always within 255
}

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
