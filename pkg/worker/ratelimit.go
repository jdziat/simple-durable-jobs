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

// maxRateLimitWindow bounds a DERIVED window. Two int64 limits meet here:
// time.Duration counts nanoseconds (math.MaxInt64 is ~292 years), and the storage
// GC computes windowStart.Add(-2*window) — so any window past MaxInt64/2 wraps
// that expression into the FUTURE and deletes live counters. A century keeps
// 2*window safely inside int64 and only binds below ~3.2e-10/sec — under one job
// per century, which no deployment configures. Such a rate is clamped and the
// limiter then runs FASTER than configured; how much faster is UNBOUNDED, not
// "slightly" (at PerSecond=1e-11 the float64→Duration conversion overflows
// negative, the non-positive guard catches it, and the limiter falls back to the
// default window — orders of magnitude above the requested rate). Erring fast
// rather than deadlocking is the safe direction for an absurd input, but the
// magnitude is not something to reason from.
const maxRateLimitWindow = 100 * 365 * 24 * time.Hour

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

// resolveRateLimitWindow chooses the fixed-window length for a fleet rate limit.
// An explicit author-set Window is honoured; otherwise it is derived.
//
// Either way the result is floored to a whole MILLISECOND and clamped, because
// the alignment that requirement exists for is a property of the STORED
// window_start and does not care where the window came from. window_start is
// now.Truncate(window) and rate_limit_windows.window_start is datetime(3) on
// MySQL, so a window like 1500µs yields a microsecond-precision start that MySQL
// ROUNDS on write; the consume's own "WHERE window_start = ?" then matches
// nothing and every rate-limited job bounces forever. Deriving the window already
// guaranteed this; an explicit one used to bypass it and reintroduce exactly that
// failure, so the invariant is enforced here rather than in one of the two paths.
//
// A sub-millisecond explicit window would floor to zero, which is not a window at
// all, so it takes the default instead.
func (w *Worker) resolveRateLimitWindow(limit RateLimitConfig) time.Duration {
	if limit.Window <= 0 {
		return deriveRateLimitWindow(limit.PerSecond)
	}
	window := limit.Window.Truncate(time.Millisecond)
	if window <= 0 {
		return defaultRateLimitWindow
	}
	if window > maxRateLimitWindow {
		return maxRateLimitWindow
	}
	return window
}

// deriveRateLimitWindow picks the fixed window whose per-window ceiling expresses
// PerSecond as closely as a whole number of admissions can.
//
// The storage gate admits ceil(PerSecond*window) units per window, so the enforced
// rate is ceil(PerSecond*window)/window — exact only when PerSecond*window is a
// WHOLE NUMBER, not merely >= 1 as this function previously assumed. That wrong
// sufficient condition (stated in its own doc comment) is the whole bug: it left
// the ceiling rounded up by nearly a full admission for every rate that was
// neither an integer nor 1/n. Measured against the real storage gate on SQLite,
// Postgres and MySQL: 1.2/sec ran at 2/sec (+67%), 7.3/sec at 8/sec (+9.6%),
// 0.011/sec at 0.022/sec (+100%, the analytic worst case).
//
// Solve for the window instead: with units = ceil(PerSecond) admissions per
// window, window = units/PerSecond makes PerSecond*window exactly units for ANY
// rate. An integer rate still derives exactly the 1s default and a 1/n rate still
// derives exactly n seconds, so configurations that were already exact do not
// move; every window for a rate >= 1/sec stays inside [1s, 2s), so
// rate_limit_windows sees no extra row churn.
//
// The window is floored to a whole MILLISECOND for two distinct reasons:
//
//   - Alignment. window_start is now.Truncate(window), and Truncate is relative to
//     the zero time, so a millisecond-multiple window always yields a
//     millisecond-aligned window_start. rate_limit_windows.window_start is
//     datetime(3) on MySQL: a nanosecond-precision start is ROUNDED on write, the
//     consume's own "WHERE window_start = ?" then matches nothing, and every
//     rate-limited job bounces on ErrRecordNotFound forever.
//   - Direction. Flooring never rounds up, so PerSecond*window stays at or below
//     units — the ceiling cannot exceed what was configured.
func deriveRateLimitWindow(perSecond float64) time.Duration {
	// NaN/Inf cannot produce a sane window; fall back to the default rather than
	// computing a negative or overflowing duration.
	if math.IsNaN(perSecond) || math.IsInf(perSecond, 0) || perSecond <= 0 {
		return defaultRateLimitWindow
	}
	units := math.Ceil(perSecond)
	seconds := units / perSecond
	window := time.Duration(seconds * float64(time.Second))
	window = window.Truncate(time.Millisecond)

	// Re-check the ENFORCED unit count against the window we are actually going to
	// use, and shrink until it matches. The arithmetic above is exact in the reals
	// — units/PerSecond*PerSecond is units — but not in float64, and the storage
	// gate does not evaluate the ideal, it evaluates ceil(PerSecond*window) on the
	// truncated value. PerSecond=6.25 derives a 1.12s window, and 6.25*1.12 is
	// 7.000000000000001 in float64, so the gate admits EIGHT units where seven were
	// intended and enforces 7.14/sec — 14.3% fast, well past the 0.5% this
	// derivation promises and UPGRADE.md advertises.
	//
	// One millisecond off is enough to drop back below the integer, and shrinking
	// can only make the enforced rate slower, never faster. Bounded so a
	// pathological input cannot spin.
	for i := 0; i < 8 && window > time.Millisecond; i++ {
		if math.Ceil(perSecond*window.Seconds()) <= units {
			break
		}
		window -= time.Millisecond
	}

	if window < defaultRateLimitWindow {
		window = defaultRateLimitWindow
	}
	if window > maxRateLimitWindow {
		window = maxRateLimitWindow
	}
	return window
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
