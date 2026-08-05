package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// GormStorage must stay COMPARABLE. It is an exported concrete type, and
// gorelease/apidiff treat a struct losing comparability as an INCOMPATIBLE
// change — which cannot ship inside /v4, so it fails the release-gating
// api-compat job rather than merely being untidy.
//
// This guard exists because the rule was stated in three separate comments
// (hotStatCaches, indexedMetadataKeys, poisonPayloadLog) and enforced by NONE of
// them: a `map[core.UUID]struct{}` field added for poison-payload logging turned
// GormStorage non-comparable and got as far as CI before anything noticed. The
// sibling guard for fanout.SubJob (pkg/fanout/zz_sub_options_test.go) is the
// pattern this follows.
//
// The map key is the compile-time proof: this file does not build if any field
// of GormStorage is a slice, map, or func. Keep such fields behind a POINTER.
var _ = map[GormStorage]struct{}{}

func TestGormStorage_StaysComparable(t *testing.T) {
	// Declared separately rather than as `b := a`: GormStorage holds atomic.Int64,
	// so ASSIGNING one to the other is a lock copy that `go vet` (copylocks)
	// rightly rejects. Comparing two independent zero values proves comparability
	// without copying either.
	var a, b GormStorage
	assert.True(t, a == b, "GormStorage must remain comparable; two zero values must compare equal")
}
