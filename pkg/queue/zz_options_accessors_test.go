package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// RetriesSet and DedupRequested are part of the /v4 exported surface and are
// therefore pinned by the api-compat gate — but neither had a test in this
// package. Removing `o.retriesSet = true` from Retries() left ./pkg/queue
// entirely green; only ./pkg/fanout noticed. A package whose exported behaviour
// is checked only by a downstream consumer is one refactor away from silently
// changing.

func apply(opts ...Option) *Options {
	o := &Options{}
	for _, opt := range opts {
		opt.Apply(o)
	}
	return o
}

// TestOptions_RetriesSetDistinguishesZeroFromUnset covers the reason the flag
// exists at all. Retries is an int, so an explicit Retries(0) — "do not retry
// this" — is indistinguishable from "never mentioned" by value alone. A fan-out
// child built from an unset literal must inherit the parent's retry count, and one
// built from an explicit zero must not.
//
// FALSE-GREEN TRAP: asserting o.Retries would pass with the flag removed, because
// the VALUE is set correctly either way. The flag is the whole point.
func TestOptions_RetriesSetDistinguishesZeroFromUnset(t *testing.T) {
	assert.False(t, apply().RetriesSet(),
		"no Retries option was applied, so a consumer must be free to supply its own default")
	assert.True(t, apply(Retries(0)).RetriesSet(),
		"an explicit Retries(0) means DO NOT RETRY; reading it as unset silently re-enables retries")
	assert.True(t, apply(Retries(7)).RetriesSet())
	assert.Equal(t, 7, apply(Retries(7)).MaxRetries)

	// Last write wins, and the flag stays set.
	both := apply(Retries(3), Retries(0))
	assert.True(t, both.RetriesSet())
	assert.Equal(t, 0, both.MaxRetries)
}

// TestOptions_DedupRequestedCoversEveryDedupOption pins the SET of options that
// count as deduplication. It reads windowedDedup rather than the TTL fields
// specifically so a degenerate UniqueFor(0) is still detected — a caller who
// writes that has still asked for dedup, and reporting "no dedup requested" would
// suppress the fan-out warning that tells them it is being ignored.
func TestOptions_DedupRequestedCoversEveryDedupOption(t *testing.T) {
	assert.False(t, apply().DedupRequested(), "no dedup option applied")
	assert.False(t, apply(Retries(2), Priority(5)).DedupRequested(),
		"unrelated options must not read as a dedup request")

	assert.True(t, apply(Unique("k")).DedupRequested())
	assert.True(t, apply(IdempotencyKey("k", time.Minute)).DedupRequested())
	assert.True(t, apply(Unique("k"), UniqueFor(time.Minute)).DedupRequested())
	assert.True(t, apply(UniqueFor(0)).DedupRequested(),
		"a zero window is still a dedup REQUEST; reading it as none would silently drop the "+
			"warning that tells the caller their option is being ignored")
}
