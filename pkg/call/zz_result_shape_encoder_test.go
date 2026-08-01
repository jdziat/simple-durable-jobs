package call

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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

// json.RawMessage holds ARBITRARY JSON, so its wire form is a property of the
// VALUE and not of the type — the same situation as an interface member.
type esRaw struct {
	B json.RawMessage `json:"b"`
}
type esBytes struct {
	B []byte `json:"b"`
}

// esRawTyped is the tightening a RawMessage member invites: "we carried this as
// raw JSON while the schema settled, now it is typed." It is wire-identical.
type esRawInner struct {
	N int `json:"n"`
}
type esRawTyped struct {
	B esRawInner `json:"b"`
}

// TestResultShape_RawMessageRecordsNoShape pins a DELIBERATE, ACCEPTED MISS, and
// it replaces an earlier test that asserted the exact opposite.
//
// That earlier test required a RawMessage member to still produce a shape, on the
// reasoning that yielding none would silently switch the guard off. The reasoning
// was right about the cost and wrong about the alternative: the only way to
// produce a shape was to substitute the stand-in json.RawMessage("null"), which
// PINNED the member as `null`. So RawMessage -> a struct, a string or a map — all
// byte-identical on the wire — were REFUSED on replay. Measured before changing
// it: both sides emit {"r":{"n":1}}, shapes were {r:null} vs {r:{n:number}}.
//
// That is the false-fire direction, and this file's whole bias is that a miss
// leaves prior behaviour in place while a false fire wedges a live workflow. It
// is also the same root cause as every other false fire shipped here: a value
// substituted at a boundary, whose fate encoding/json then decides.
//
// So a result type containing a RawMessage now records NO shape and is skipped,
// exactly as one containing an interface member is. DO NOT "fix" this by
// substituting a stand-in again.
func TestResultShape_RawMessageRecordsNoShape(t *testing.T) {
	a := esRaw{B: json.RawMessage(`{"n":1}`)}
	b := esRawTyped{B: esRawInner{N: 1}}
	wa, err := json.Marshal(a)
	require.NoError(t, err)
	wb, err := json.Marshal(b)
	require.NoError(t, err)
	require.Equal(t, string(wa), string(wb),
		"FIXTURE BROKEN: the pair must be byte-identical for this to say anything")

	require.Empty(t, fingerprintOf(esRaw{}),
		"a RawMessage member cannot be described from its type, so the type must fail OPEN "+
			"— any recorded shape pins it and refuses a wire-identical tightening")

	// []byte is NOT RawMessage: it base64s into a JSON string, which IS knowable
	// from the type. It must keep its shape — the fail-open is scoped to
	// RawMessage alone and must not leak to every []byte result.
	require.NotEmpty(t, fingerprintOf(esBytes{}),
		"[]byte has a knowable wire form (a base64 string) and must still be guarded")
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
