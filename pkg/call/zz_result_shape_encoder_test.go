package call

import (
	"encoding/json"
	"reflect"
	"testing"
)

// These pin the cases that a HAND-WRITTEN mirror of encoding/json's rules got
// wrong across four review rounds. The shape is now read off the real encoder, so
// each of these is correct by construction rather than by a rule someone wrote
// down — but they stay as tests because "correct by construction" is a claim, and
// a claim about serialization is exactly the kind this campaign kept disproving.
//
// The invariant every case checks is the same one: two types encoding/json
// serializes IDENTICALLY must fingerprint identically (or a byte-identical
// refactor wedges a live replay), and two it serializes DIFFERENTLY must not.

func fingerprintOf(v any) string { return ResultFingerprintForTest(reflect.TypeOf(v)) }

func requireWireAgreement(t *testing.T, name string, a, b any, wantSame bool) {
	t.Helper()
	ba, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("%s: marshal a: %v", name, err)
	}
	bb, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("%s: marshal b: %v", name, err)
	}
	if sameWire := string(ba) == string(bb); sameWire != wantSame {
		t.Fatalf("%s: FIXTURE BROKEN — wire same=%v, expected %v\n  a=%s\n  b=%s",
			name, sameWire, wantSame, ba, bb)
	}
	sameFP := fingerprintOf(a) == fingerprintOf(b)
	if sameFP == wantSame {
		return
	}
	if wantSame {
		t.Errorf("%s: identical wire output (%s) but different fingerprints — "+
			"a refactor that changes nothing would wedge in-flight replays", name, ba)
		return
	}
	t.Errorf("%s: different wire output (%s vs %s) but identical fingerprints — "+
		"a real result-type change would replay silently wrong", name, ba, bb)
}

type esAudit struct {
	Ref string `json:"ref"`
}

// ",string" is INERT on any kind json cannot quote (struct, slice, array, map,
// interface). Removing such a dead option changes nothing on the wire.
type esInertStruct struct {
	Audit  esAudit `json:"audit,string"` //nolint:staticcheck // SA5008: the inert option IS the fixture
	Amount int     `json:"amount"`
}
type esInertStructAfter struct {
	Audit  esAudit `json:"audit"`
	Amount int     `json:"amount"`
}
type esInertSlice struct {
	Tags []string `json:"tags,string"` //nolint:staticcheck // SA5008: the inert option IS the fixture
}
type esInertSliceAfter struct {
	Tags []string `json:"tags"`
}

// ...but on a quotable kind it is real: the number becomes a JSON string.
type esQuotedInt struct {
	N int `json:",string"`
}
type esPlainInt struct {
	N int
}

// Only *T implements MarshalJSON. Call marshals an INTERFACE-BOXED result, which
// is not addressable, so encoding/json never invokes the pointer method and the
// struct's fields are emitted instead.
type esPtrMarshaler struct {
	X int `json:"x"`
}

func (p *esPtrMarshaler) MarshalJSON() ([]byte, error) { return []byte(`"PTR"`), nil }

type esHoldsPtrMarshaler struct {
	V esPtrMarshaler `json:"v"`
}
type esPlainInner struct {
	X int `json:"x"`
}
type esHoldsPlain struct {
	V esPlainInner `json:"v"`
}

// A MarshalJSON that reproduces the default encoding is redundant; deleting it is
// byte-identical.
type esRedundant struct {
	A string `json:"a"`
}

func (v esRedundant) MarshalJSON() ([]byte, error) { return []byte(`{"a":"x"}`), nil }

type esRedundantAfter struct {
	A string `json:"a"`
}

// uintptr is emitted as a number.
type esUintptr struct {
	U uintptr `json:"u"`
}
type esUint64 struct {
	U uint64 `json:"u"`
}

// A tagged field beats an untagged one at equal depth, and json EMITS it. The two
// sides deliberately differ in KIND: with both as string, the shape is identical
// whichever candidate wins and the rule under test goes unobserved.
type esTieDifferentKinds struct {
	Name  int    // loses the tie: never serialized
	Alias string `json:"Name"`
}
type esTieWinnerOnly struct {
	Alias string `json:"Name"`
}
type esTieLoserKind struct {
	Name int
}

func TestResultShape_EncoderDerivedInvariants(t *testing.T) {
	requireWireAgreement(t, "inert ,string on a struct field",
		esInertStruct{}, esInertStructAfter{}, true)
	requireWireAgreement(t, "inert ,string on a slice field",
		esInertSlice{}, esInertSliceAfter{}, true)
	requireWireAgreement(t, "live ,string on an int field",
		esQuotedInt{}, esPlainInt{}, false)
	requireWireAgreement(t, "pointer-receiver MarshalJSON is unused when boxed",
		esHoldsPtrMarshaler{}, esHoldsPlain{}, true)
	// Populated: the redundant marshaler hard-codes "x", so only a matching value
	// makes the two byte-identical — which is what "redundant" means.
	requireWireAgreement(t, "deleting a redundant MarshalJSON",
		esRedundant{A: "x"}, esRedundantAfter{A: "x"}, true)
	requireWireAgreement(t, "uintptr is a number",
		esUintptr{}, esUint64{}, true)
	requireWireAgreement(t, "tagged field wins the tie and is emitted",
		esTieDifferentKinds{}, esTieWinnerOnly{}, true)
	requireWireAgreement(t, "the tie LOSER's kind must not be what shows up",
		esTieDifferentKinds{}, esTieLoserKind{}, false)
}

// json.RawMessage holds arbitrary JSON. Synthesizing it from arbitrary bytes makes
// invalid JSON, the marshal fails, and the guard silently switches itself off for
// every result type containing one — a fail-open that no other test would notice.
type esRaw struct {
	B json.RawMessage `json:"b"`
}
type esBytes struct {
	B []byte `json:"b"`
}

func TestResultShape_RawMessageStillProducesAShape(t *testing.T) {
	if fp := fingerprintOf(esRaw{}); fp == "" {
		t.Fatal("a result type containing json.RawMessage produced NO shape, " +
			"so replay skips the check entirely for it")
	}
	// []byte is base64'd into a JSON string; RawMessage is spliced in as raw JSON.
	// Different wire forms, so they must not be interchangeable.
	if fingerprintOf(esRaw{}) == fingerprintOf(esBytes{}) {
		t.Error("[]byte and json.RawMessage have different wire forms and must differ")
	}
}

// A type encoding/json cannot marshal at all must yield NO shape rather than a
// misleading one: an unshapeable type must never be able to wedge a replay.
type esUnmarshalable struct {
	C chan int `json:"c"`
}

func TestResultShape_UnmarshalableTypeYieldsNoShape(t *testing.T) {
	if fp := fingerprintOf(esUnmarshalable{}); fp != "" {
		t.Fatalf("expected no shape for a type json cannot marshal, got %q", fp)
	}
}
