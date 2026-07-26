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
