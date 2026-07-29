package worker

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The storage gate admits ceil(PerSecond*window) units per window, so the
// ENFORCED rate is ceil(PerSecond*window)/window. That is exact only when
// PerSecond*window is a whole number — but the derivation only required it to be
// >= 1, a weaker condition its own doc comment asserted was sufficient. Every
// rate that was neither an integer nor 1/n therefore ran fast.
//
// FALSE-GREEN TRAP: asserting the derived WINDOW value passes for integer rates
// under both old and new formulas (both give 1s), and asserting "window >= 1s"
// passes under the old one for everything. The discriminating assertion is the
// ENFORCED RATE that the window implies — computed the same way storage does.
func enforcedRate(perSecond float64) float64 {
	w := deriveRateLimitWindow(perSecond)
	units := math.Ceil(perSecond * w.Seconds())
	return units / w.Seconds()
}

func TestDeriveRateLimitWindow_EnforcedRateIsExact(t *testing.T) {
	// Each of these ran fast under the old derivation. The measured overshoot is
	// in the comment; the assertion is that it is now within 0.5%.
	for _, perSecond := range []float64{
		0.011, // was +100% — the analytic worst case
		0.1,   // 1/n: was already exact, must not move
		0.3,   // was +66.67%
		0.4,   // was +66.67%
		0.6,   // was +66.67%
		1.0,   // integer: was exact, must not move
		1.01,  // was +98%
		1.2,   // was +66.67%
		2.5,   // was +20%
		7.3,   // was +9.6%
		10.0,  // integer
	} {
		got := enforcedRate(perSecond)
		relErr := math.Abs(got-perSecond) / perSecond
		assert.LessOrEqual(t, relErr, 0.005,
			"PerSecond=%v enforces %v (%.2f%% off); the configured rate must be honoured within 0.5%%",
			perSecond, got, relErr*100)
	}
}

// TestDeriveRateLimitWindow_EnforcedRateIsExactAcrossTheRange SWEEPS instead of
// sampling.
//
// FALSE-GREEN BY CONSTRUCTION, which is what the table-driven test above is: its
// eleven values are the same ones already tabulated in UPGRADE.md, so it can only
// ever confirm rates whose behaviour was measured when the list was written. A
// sweep of 500,000 rates found exactly one offender it could never have caught —
// PerSecond=6.25 derived a 1.12s window, and 6.25*1.12 is 7.000000000000001 in
// float64, so the storage gate admitted EIGHT units where seven were intended and
// enforced 7.14/sec: 14.3% fast, against a documented bound of 0.5%.
//
// The bound is a PROPERTY of the derivation, so it is tested as one.
func TestDeriveRateLimitWindow_EnforcedRateIsExactAcrossTheRange(t *testing.T) {
	worstRel, worstRate := 0.0, 0.0
	consider := func(perSecond float64) {
		if perSecond <= 0 {
			return
		}
		if rel := math.Abs(enforcedRate(perSecond)-perSecond) / perSecond; rel > worstRel {
			worstRel, worstRate = rel, perSecond
		}
	}

	// Several structurally different families, because a single arithmetic
	// progression can miss a whole class: the one offender this test was written
	// for (6.25) is a dyadic fraction, and those are exactly the values where a
	// float64 product lands a hair above an integer.
	for i := 1; i <= 500000; i++ {
		consider(float64(i) * 0.0001) // fine progression, 0.0001..50
	}
	for i := 1; i <= 200000; i++ {
		consider(float64(i) * 7e-4) // coarser progression, different alignment
	}
	for n := 1; n <= 20000; n++ {
		consider(1 / float64(n)) // reciprocals: the "already exact" family
	}
	for n := 1; n <= 8000; n++ {
		consider(float64(n) / 8)  // dyadic fractions — where 6.25 lives
		consider(float64(n) / 16) //
	}
	for n := 1; n <= 2000; n++ {
		consider(float64(n) - 1e-9) // just under an integer
		consider(float64(n) + 1e-9) // just over
	}
	assert.LessOrEqual(t, worstRel, 0.005,
		"the worst rate found is PerSecond=%v, enforced %v (%.4f%% off) — the derivation "+
			"promises 0.5%% and UPGRADE.md advertises it, so it has to hold across the range and "+
			"not just at the values someone happened to tabulate",
		worstRate, enforcedRate(worstRate), worstRel*100)
}

// TestDeriveRateLimitWindow_ExactRatesDoNotMove pins the compatibility promise:
// the configurations that were ALREADY exact must derive the same window as
// before, so no existing deployment sees its window change.
func TestDeriveRateLimitWindow_ExactRatesDoNotMove(t *testing.T) {
	for _, tc := range []struct {
		perSecond float64
		want      time.Duration
	}{
		{1, time.Second},
		{5, time.Second},
		{100, time.Second},
		{0.5, 2 * time.Second},
		{0.25, 4 * time.Second},
		{0.1, 10 * time.Second},
	} {
		assert.Equal(t, tc.want, deriveRateLimitWindow(tc.perSecond),
			"PerSecond=%v was already exact and its window must not move", tc.perSecond)
	}
}

// TestDeriveRateLimitWindow_WindowIsMillisecondAligned guards a MySQL-specific
// trap. window_start is now.Truncate(window) and rate_limit_windows.window_start
// is datetime(3): a nanosecond-precision start is ROUNDED on write, the consume's
// own "WHERE window_start = ?" then matches nothing, and every rate-limited job
// bounces forever. A millisecond-multiple window makes the start millisecond
// aligned, which round-trips exactly on all three dialects.
func TestDeriveRateLimitWindow_WindowIsMillisecondAligned(t *testing.T) {
	for _, perSecond := range []float64{0.011, 0.3, 1.01, 1.2, 2.5, 7.3, 1.0 / 3.0} {
		w := deriveRateLimitWindow(perSecond)
		assert.Zero(t, w%time.Millisecond,
			"PerSecond=%v derives %v, which is not a whole number of milliseconds — "+
				"MySQL datetime(3) would round window_start and strand every consume", perSecond, w)
	}
}

// TestDeriveRateLimitWindow_RejectsPathologicalRates covers the inputs that would
// otherwise produce a negative, zero or overflowing duration.
func TestDeriveRateLimitWindow_RejectsPathologicalRates(t *testing.T) {
	for name, perSecond := range map[string]float64{
		"NaN":          math.NaN(),
		"+Inf":         math.Inf(1),
		"-Inf":         math.Inf(-1),
		"zero":         0,
		"negative":     -5,
		"denormal-ish": 1e-300,
		// Reaches the UPPER clamp. Without it the clamp assertion below could not
		// fail: NaN/Inf/0/-5 all return early, and 1e-300 overflows the
		// float64->Duration conversion to MinInt64 and is caught by the LOWER
		// clamp, returning 1s. Deleting the upper clamp entirely still passed.
		"one-per-160-years": 2e-10,
	} {
		w := deriveRateLimitWindow(perSecond)
		assert.Positive(t, w, "%s must not yield a non-positive window", name)
		assert.LessOrEqual(t, w, maxRateLimitWindow,
			"%s must be clamped: the storage GC computes windowStart.Add(-2*window), "+
				"so a window past MaxInt64/2 wraps into the FUTURE and deletes live counters", name)
	}
}

// TestDeriveRateLimitWindow_UpperClampIsReachable is the control for the clamp
// assertion above: it names the exact input that lands ON the cap, so deleting
// the clamp is a test failure rather than a silent pass.
//
// The band is narrow, which is why it was missed. Below ~1.1e-10 the
// float64->Duration conversion has already overflowed and the LOWER clamp
// returns the default window instead; above ~3.3e-10 the derived window is
// naturally under the cap. 2e-10 — about one job per 160 years — sits inside it.
func TestDeriveRateLimitWindow_UpperClampIsReachable(t *testing.T) {
	assert.Equal(t, maxRateLimitWindow, deriveRateLimitWindow(2e-10),
		"this input must exercise the upper clamp, or the bound above is untested")
}

// TestResolveRateLimitWindow_ExplicitWindowIsAlsoAligned closes the hole between
// the two ways a window is chosen.
//
// The millisecond alignment exists because window_start is now.Truncate(window)
// and rate_limit_windows.window_start is datetime(3) on MySQL: a
// sub-millisecond-precision start is ROUNDED on write, the consume's own
// "WHERE window_start = ?" matches nothing, and every rate-limited job bounces
// forever. deriveRateLimitWindow guaranteed that; an author-set Window bypassed
// it entirely, so RateLimitConfig{Window: 1500 * time.Microsecond} reintroduced
// the exact failure the derivation was hardened against.
//
// FALSE-GREEN TRAP: asserting that an explicit window is returned unchanged is
// what the old behaviour did — it passes precisely when the bug is present. The
// assertion has to be the INVARIANT (whole milliseconds, positive, clamped),
// applied to explicit and derived windows alike.
func TestResolveRateLimitWindow_ExplicitWindowIsAlsoAligned(t *testing.T) {
	w := &Worker{}

	for name, limit := range map[string]RateLimitConfig{
		"sub-millisecond":      {PerSecond: 10, Window: 1500 * time.Microsecond},
		"nanosecond-precision": {PerSecond: 10, Window: time.Second + 7*time.Nanosecond},
		"absurdly-large":       {PerSecond: 10, Window: 1 << 62},
		"derived":              {PerSecond: 2.5},
	} {
		got := w.resolveRateLimitWindow(limit)
		assert.Positive(t, got, "%s must yield a usable window", name)
		assert.Zero(t, got%time.Millisecond,
			"%s derives %v, which is not a whole number of milliseconds — MySQL datetime(3) "+
				"rounds window_start and strands every consume", name, got)
		assert.LessOrEqual(t, got, maxRateLimitWindow, "%s must be clamped", name)
	}
}

// TestResolveRateLimitWindow_HonoursAnExplicitWindow is the compatibility control:
// aligning must not silently retune a window an author set deliberately.
func TestResolveRateLimitWindow_HonoursAnExplicitWindow(t *testing.T) {
	w := &Worker{}
	for _, want := range []time.Duration{time.Second, 250 * time.Millisecond, 5 * time.Second} {
		assert.Equal(t, want, w.resolveRateLimitWindow(RateLimitConfig{PerSecond: 3, Window: want}),
			"an already-aligned explicit window must be used exactly as given")
	}
}
