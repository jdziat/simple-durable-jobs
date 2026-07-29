package fanout

import (
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/stretchr/testify/assert"
)

// SubJob must stay COMPARABLE. api-compat (gorelease/apidiff) treats losing
// comparability as an incompatible change, which cannot ship inside /v4 — so a
// slice or map field here would block the release, not just be untidy. The map
// key is the compile-time proof.
var _ = map[SubJob]struct{}{}

func TestSubJob_StaysComparable(t *testing.T) {
	at := time.Unix(0, 0).UTC()
	a := SubJob{Type: "t", Args: "x", Retries: 1, RetriesSet: true, RunAt: &at}
	b := a
	assert.True(t, a == b, "SubJob must remain comparable and equal to its copy")
}

// Sub() used to stamp Retries: queue.DefaultJobRetries unconditionally. That is
// non-zero, so buildSubJobs' `retries == 0` fallback could never reach
// cfg.retries — making WithFanOutRetries 100% DEAD for every Sub()-built child,
// not merely ignored for Retries(0).
//
// FALSE-GREEN TRAP: asserting Sub(...).Retries alone tests the struct field, not
// the effective retries the child is created with. The distinction that matters
// is RetriesSet: it is what lets an explicit 0 survive the fallback.
func TestSub_RetriesSetDistinguishesExplicitZeroFromUnset(t *testing.T) {
	unset := Sub("job", "args")
	assert.False(t, unset.RetriesSet, "no queue.Retries option means unset")

	zero := Sub("job", "args", queue.Retries(0))
	assert.True(t, zero.RetriesSet, "queue.Retries(0) is an explicit choice, not an absence")
	assert.Equal(t, 0, zero.Retries)

	seven := Sub("job", "args", queue.Retries(7))
	assert.True(t, seven.RetriesSet)
	assert.Equal(t, 7, seven.Retries)

	// Options supplied, but none of them Retries: still unset, so the fan-out
	// default governs.
	other := Sub("job", "args", queue.Priority(5))
	assert.False(t, other.RetriesSet, "a non-Retries option must not mark retries as set")
}

// Sub accepted the full queue.Option set and silently dropped these four. A
// sub-job asked to replay deterministically did not; one given a Delay ran
// immediately.
func TestSub_CarriesSchedulingAndDeterminismOptions(t *testing.T) {
	at := time.Now().Add(time.Hour)

	assert.Equal(t, 30*time.Minute, Sub("j", "a", queue.Delay(30*time.Minute)).Delay,
		"Delay was silently dropped")
	assert.Equal(t, &at, Sub("j", "a", queue.At(at)).RunAt,
		"RunAt was silently dropped")
	assert.Equal(t, queue.Strict, Sub("j", "a", queue.Determinism(queue.Strict)).Determinism,
		"Determinism was silently dropped — a sub-job asked to replay strictly did not")
}

// Dedup options cannot be honoured on a fan-out child: children carry a
// fan-out-owned UniqueKey so parent replay stays idempotent. They are FLAGGED so
// FanOut can warn, rather than silently accepted.
//
// Warn, not error, is deliberate for v4: turning a silently-wrong call into a
// hard failure on upgrade would convert a latent bug into an outage.
func TestSub_FlagsDedupOptionsRatherThanAcceptingThem(t *testing.T) {
	assert.True(t, Sub("j", "a", queue.Unique("k")).DedupOptionsIgnored)
	assert.True(t, Sub("j", "a", queue.IdempotencyKey("k", time.Hour)).DedupOptionsIgnored)
	assert.True(t, Sub("j", "a", queue.UniqueFor(0)).DedupOptionsIgnored,
		"a degenerate UniqueFor(0) leaves the TTL at zero, so detection must read the "+
			"dedup MODE rather than the TTL field")
	assert.False(t, Sub("j", "a").DedupOptionsIgnored, "no dedup option means nothing to warn about")
}

// TestSub_RetriesAreOnlyStampedWhenExplicitlySet covers the `if sj.RetriesSet`
// guard in Sub(), which this branch ADDED and which nothing covered: removing the
// conditional (stamping queueOpts.MaxRetries unconditionally) left the entire
// repository green, including ./tests.
//
// The guard is the whole point of the change. Without it every child carries a
// Retries value whether or not the caller asked for one, and an unset literal's
// zero overwrites the fan-out default — which is exactly the "the fan-out default
// is unreachable" defect this wave set out to fix.
//
// FALSE-GREEN TRAP: asserting Retries on a child built WITH queue.Retries(n)
// passes either way, because the value is stamped correctly in both versions. The
// discriminating case is a Sub with NO retries option at all, where the flag must
// stay false and the value must stay zero so buildSubJobs can apply the default.
func TestSub_RetriesAreOnlyStampedWhenExplicitlySet(t *testing.T) {
	// No retries option: the flag must be false so the fan-out default applies.
	unset := Sub("child", "arg")
	assert.False(t, unset.RetriesSet,
		"a Sub with no Retries option must not claim one was set, or the fan-out default "+
			"becomes unreachable")
	assert.Zero(t, unset.Retries,
		"and it must not carry a stamped value for buildSubJobs to prefer over the default")

	// Some OTHER option present, so queueOpts is non-nil and the block runs — this
	// is the case that distinguishes "the block did not run" from "the guard held".
	other := Sub("child", "arg", queue.Priority(7))
	assert.False(t, other.RetriesSet,
		"an unrelated option must not cause a retry count to be stamped")
	assert.Zero(t, other.Retries)
	assert.Equal(t, 7, other.Priority, "sanity: the option block really did run")

	// Explicit values still arrive, including an explicit zero.
	seven := Sub("child", "arg", queue.Retries(7))
	assert.True(t, seven.RetriesSet)
	assert.Equal(t, 7, seven.Retries)

	zero := Sub("child", "arg", queue.Retries(0))
	assert.True(t, zero.RetriesSet, "an explicit Retries(0) means DO NOT RETRY and must be honoured")
	assert.Zero(t, zero.Retries)
}
