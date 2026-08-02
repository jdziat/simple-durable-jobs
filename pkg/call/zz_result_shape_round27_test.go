package call

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// Round 27. Every case here is the SAME root cause wearing a different hat: the
// depth budget was being spent on things that add no JSON nesting, and the
// "probe did not marshal" fallback was being applied to the whole type instead
// of the member that rejected the probe.
//
// The bias these tests encode is the one UPGRADE.md states: a MISS leaves prior
// behaviour alone, a FALSE FIRE rejects a replay that would have succeeded and
// wedges a live workflow. So the wire is the referee — two types encoding/json
// serializes identically must fingerprint identically, and the tests that assert
// a difference always show the wire differing first.

// ---- (a) an untagged embedded struct adds no JSON nesting -------------------
//
// encoding/json PROMOTES an untagged embed's members into the parent object, so
// grouping members into an embed (or inlining one) is byte-identical on the
// wire. Charging it a depth level made that refactor shift every member below it
// one level deeper, and at the boundary a slice/map/pointer member collapses to
// null — a fingerprint change on a change that cannot alter a byte. Embeds also
// COMPOUND: three embeds on a path used to cost three of the six levels.

type r27D5 struct {
	// A SLICE deliberately: a truncated string is still a JSON string and the
	// shape would not move, so the truncation would be invisible. A truncated
	// slice is nil, which marshals as null — a different kind.
	Leaf []string `json:"leaf"`
}
type r27D4 struct {
	D5 r27D5 `json:"d5"`
}
type r27D3 struct {
	D4 r27D4 `json:"d4"`
}
type r27D2 struct {
	D3 r27D3 `json:"d3"`
}
type r27D1 struct {
	D2 r27D2 `json:"d2"`
}
type r27FlatRoot struct {
	D1 r27D1 `json:"d1"`
}

// The same type after the refactor: d4's single member now arrives through an
// untagged embed. Zero bytes change on the wire.
type r27D4Fields struct {
	D5 r27D5 `json:"d5"`
}
type r27D4Emb struct {
	r27D4Fields
}
type r27D3Emb struct {
	D4 r27D4Emb `json:"d4"`
}
type r27D2Emb struct {
	D3 r27D3Emb `json:"d3"`
}
type r27D1Emb struct {
	D2 r27D2Emb `json:"d2"`
}
type r27EmbRoot struct {
	D1 r27D1Emb `json:"d1"`
}

func r27FlatValue() r27FlatRoot {
	return r27FlatRoot{r27D1{r27D2{r27D3{r27D4{r27D5{[]string{"x"}}}}}}}
}
func r27EmbValue() r27EmbRoot {
	return r27EmbRoot{r27D1Emb{r27D2Emb{r27D3Emb{r27D4Emb{r27D4Fields{r27D5{[]string{"x"}}}}}}}}
}

// The other half of "untagged embed": `struct{ *Base }`. It is the more common
// of the two forms — an optional base, a shared header — and encoding/json
// promotes through it identically, so it must cost no depth either. It is a
// separate branch in promotesIntoParent (json's own "follow the pointer" step),
// and nothing exercised it: the value-embed ladder above passes with that branch
// disabled.
type r27D4EmbPtr struct {
	*r27D4Fields
}
type r27D3EmbPtr struct {
	D4 r27D4EmbPtr `json:"d4"`
}
type r27D2EmbPtr struct {
	D3 r27D3EmbPtr `json:"d3"`
}
type r27D1EmbPtr struct {
	D2 r27D2EmbPtr `json:"d2"`
}
type r27EmbPtrRoot struct {
	D1 r27D1EmbPtr `json:"d1"`
}

func r27EmbPtrValue() r27EmbPtrRoot {
	return r27EmbPtrRoot{r27D1EmbPtr{r27D2EmbPtr{r27D3EmbPtr{r27D4EmbPtr{&r27D4Fields{r27D5{[]string{"x"}}}}}}}}
}

func TestResultShape_UntaggedEmbedSpendsNoDepth(t *testing.T) {
	requireWireAgreement(t, "grouping members into an untagged embed at the depth boundary",
		r27FlatValue(), r27EmbValue(), true)

	requireWireAgreement(t, "grouping members into an untagged embedded POINTER at the depth boundary",
		r27FlatValue(), r27EmbPtrValue(), true)

	// ...and they must agree by both still DESCRIBING the innermost member, not
	// by both having been truncated away. Without this the test above could pass
	// on a budget of zero.
	for name, shape := range map[string]string{
		"flat":             ResultShapeStringForTest(reflect.TypeOf(r27FlatRoot{})),
		"embedded":         ResultShapeStringForTest(reflect.TypeOf(r27EmbRoot{})),
		"embedded pointer": ResultShapeStringForTest(reflect.TypeOf(r27EmbPtrRoot{})),
	} {
		if !strings.Contains(shape, "leaf:[") {
			t.Errorf("%s: the innermost member was truncated away: %s", name, shape)
		}
	}
}

// The user-facing form of the same defect, driven through production's real
// write-then-replay path: the deploy groups d4's member into an embed and every
// in-flight replay hard-fails with a determinism violation that is provably
// untrue — the stored payload reconstructs perfectly.
func TestResultShape_EmbedRefactorDoesNotWedgeReplay(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27FlatRoot, error) {
		return r27FlatValue(), nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[r27FlatRoot, r27EmbRoot](t, h, "settle-deep")
	if err != nil {
		t.Fatalf("a wire-identical embed refactor wedged the replay: %v", err)
	}
	if !reflect.DeepEqual(got, r27EmbValue()) {
		t.Fatalf("the stored payload did not reconstruct: got %+v, stored %s", got, saved.Result)
	}
}

// ---- (b) termination is NOT the depth budget's job --------------------------
//
// Once pointers and untagged embeds spend no budget, the budget can no longer
// bound recursion: these types nest zero JSON levels per hop and would recurse
// forever. A stack overflow is a runtime FATAL error, not a panic — resultShape's
// recover cannot catch it, so it would kill the worker process on both the write
// and the replay path of every Call using the type. Termination therefore needs
// its own bound.

type r27SelfPtr *r27SelfPtr

type r27PtrA *r27PtrB
type r27PtrB *r27PtrA

// Mutually recursive through untagged EMBEDDED pointers: legal Go, promotes at
// every hop, and adds no JSON nesting anywhere.
type r27EmbCycA struct {
	*r27EmbCycB
	Name string `json:"name"`
}
type r27EmbCycB struct {
	*r27EmbCycA
	Kind string `json:"kind"`
}

func TestResultShape_ZeroNestingCyclesTerminate(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
	}{
		{"type T *T", reflect.TypeOf((*r27SelfPtr)(nil)).Elem()},
		{"mutually recursive pointers", reflect.TypeOf((*r27PtrA)(nil)).Elem()},
		{"mutually recursive embedded pointers", reflect.TypeOf(r27EmbCycA{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("resultFingerprint panicked: %v", r)
				}
			}()
			// Reaching the assertion at all is the point: unbounded recursion
			// dies on a fatal stack overflow that takes the process with it.
			_ = ResultFingerprintForTest(tc.typ)
		})
	}
}

// ---- (d) the probe fallback must isolate the offending MEMBER ---------------
//
// A member whose marshaler VALIDATES rejects the fabricated probe. Replacing the
// WHOLE type's value with its zero erased every sibling: nil slices/maps/pointers
// marshal as null and omitempty members vanish, so all nested structure was lost.
// Both faces of that are tested below.

type r27Item struct {
	SKU string `json:"sku"`
}
type r27ItemB struct {
	Ref int `json:"ref"`
}

type r27LeaseIP struct {
	IP    net.IP    `json:"ip"`
	Items []r27Item `json:"items"`
}

// The Go-recommended modernization of the same struct. net.IP and netip.Addr
// both marshal via MarshalText to the same dotted quad, so this is byte-identical
// on the wire.
type r27LeaseAddr struct {
	IP    netip.Addr `json:"ip"`
	Items []r27Item  `json:"items"`
}

// Same as r27LeaseIP but with a genuinely different element member set.
type r27LeaseIPChanged struct {
	IP    net.IP     `json:"ip"`
	Items []r27ItemB `json:"items"`
}

// A TYPE WHOSE PROBE IS REJECTED IS NOT GUARDED. Everything below pins that as
// the DELIBERATE, ACCEPTED policy — see resultShape for why every attempt to keep
// a degraded shape instead false-fired.
func TestResultShape_ValidatingMarshalerIsADeliberateAcceptedMiss(t *testing.T) {
	// The type is not guarded at all: no shape, so replay skips it. Both the
	// original and the modernized form, so neither direction of the refactor can
	// wedge.
	for _, typ := range []reflect.Type{
		reflect.TypeOf(r27LeaseIP{}),
		reflect.TypeOf(r27LeaseIPChanged{}),
		reflect.TypeOf(r27PriceA{}),
		reflect.TypeOf(r27PriceB{}),
		reflect.TypeOf(r27BoxA{}),
		reflect.TypeOf(r27BoxB{}),
		reflect.TypeOf(r27ZoneMap{}),
	} {
		if fp := ResultFingerprintForTest(typ); fp != "" {
			t.Errorf("%s contains a marshaler that rejects the probe, so it must record NO "+
				"shape and be skipped; got fingerprint %q. Reintroducing a stand-in shape "+
				"reintroduces the false fire", typ, fp)
		}
	}

	// THE COST, stated out loud rather than left to be discovered: a genuinely
	// changed result type replays WITHOUT an error, exactly as it did before this
	// feature existed. That is the cheap direction. Do not "fix" this by
	// substituting a value the marshaler accepts — two revisions did and both
	// wedged live replays; see resultShape.
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27LeaseIP, error) {
		return r27LeaseIP{net.ParseIP("10.0.0.1"), []r27Item{{"sku-1"}}}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	if _, _, err := writeThenReplay[r27LeaseIP, r27LeaseIPChanged](t, h, "lease-changed"); err != nil {
		t.Fatalf("this miss is accepted, but the replay must still SUCCEED rather than "+
			"fail some other way: %v", err)
	}
}

// THE INVARIANT, and the reason approach (B) was taken at all.
//
// A member the fingerprint cannot probe must contribute the SAME shape entry
// whatever its Go representation and whatever its `omitempty`. The previous
// revision substituted the member's zero value, and the encoder then treated
// that zero differently per representation: a slice-backed net.IP with
// `omitempty` was DROPPED entirely, a struct-backed netip.Addr was not, a
// pointer became null where a value stayed an object. So byte-identical
// modernizations moved the fingerprint, and a dropped member is indistinguishable
// from a member that was REMOVED from the type — the very change the guard
// exists to catch.
//
// Under (B) the contributed entry is the same in every case because there is no
// entry and no shape: the type is skipped. That is what makes the invariant hold
// by construction rather than by enumeration.
func TestResultShape_UnprobeableMemberContributesTheSameEntryInEveryRepresentation(t *testing.T) {
	reps := []struct {
		name string
		typ  reflect.Type
	}{
		{"slice-backed (net.IP)", reflect.TypeOf(r27RepSlice{})},
		{"slice-backed, omitempty", reflect.TypeOf(r27RepSliceOmit{})},
		{"struct-backed", reflect.TypeOf(r27RepStruct{})},
		{"struct-backed, omitempty", reflect.TypeOf(r27RepStructOmit{})},
		{"pointer-backed", reflect.TypeOf(r27RepPtr{})},
		{"pointer-backed, omitempty", reflect.TypeOf(r27RepPtrOmit{})},
		{"named-scalar-backed", reflect.TypeOf(r27RepScalar{})},
		{"named-scalar-backed, omitempty", reflect.TypeOf(r27RepScalarOmit{})},
	}
	want := ResultShapeStringForTest(reps[0].typ)
	wantFP := ResultFingerprintForTest(reps[0].typ)
	for _, rep := range reps[1:] {
		if got := ResultShapeStringForTest(rep.typ); got != want {
			t.Errorf("%s: shape %q, but %s gives %q — an un-probeable member's contribution "+
				"must not depend on its representation or its omitempty",
				rep.name, got, reps[0].name, want)
		}
		if got := ResultFingerprintForTest(rep.typ); got != wantFP {
			t.Errorf("%s: fingerprint %q vs %q for %s — swapping one representation for "+
				"another cannot move a byte on the wire, so it must not move the fingerprint",
				rep.name, got, wantFP, reps[0].name)
		}
	}
	// And the entry they agree on must be the SKIP sentinel, not some degraded
	// shape they happen to share: a shared degraded shape is a collision, which
	// is the other half of the defect.
	if want != "" || wantFP != "" {
		t.Fatalf("expected no shape at all for an un-probeable member, got shape %q / fp %q",
			want, wantFP)
	}
	// The control: without the un-probeable member these same eight carriers are
	// guarded normally, so the assertion above is not vacuously true of every
	// type in the package.
	if fp := ResultFingerprintForTest(reflect.TypeOf(r27RepControl{})); fp == "" {
		t.Fatal("CONTROL BROKEN: the same carrier struct with an ordinary member records no " +
			"shape either, so this test proves nothing")
	}
}

// The eight carriers. Each holds an un-probeable member in one Go representation,
// once bare and once with `omitempty` — the axis that made the previous revision
// drop the member for some representations and keep it for others.
type r27RepSlice struct {
	M    net.IP `json:"m"`
	Name string `json:"name"`
}
type r27RepSliceOmit struct {
	M    net.IP `json:"m,omitempty"`
	Name string `json:"name"`
}
type r27RepStruct struct {
	M    r27OpaqueStruct `json:"m"`
	Name string          `json:"name"`
}
type r27RepStructOmit struct {
	M    r27OpaqueStruct `json:"m,omitempty"`
	Name string          `json:"name"`
}
type r27RepPtr struct {
	M    *r27OpaqueStruct `json:"m"`
	Name string           `json:"name"`
}
type r27RepPtrOmit struct {
	M    *r27OpaqueStruct `json:"m,omitempty"`
	Name string           `json:"name"`
}
type r27RepScalar struct {
	M    r27Currency `json:"m"`
	Name string      `json:"name"`
}
type r27RepScalarOmit struct {
	M    r27Currency `json:"m,omitempty"`
	Name string      `json:"name"`
}
type r27RepControl struct {
	M    string `json:"m"`
	Name string `json:"name"`
}

// A STRUCT that rejects the probe, so the struct representation is genuinely
// un-probeable rather than merely un-populated. netip.Addr would not do: its
// unexported fields are never populated, so its zero marshals fine and it is
// probeable.
type r27OpaqueStruct struct {
	V string `json:"v"`
}

func (o r27OpaqueStruct) MarshalJSON() ([]byte, error) {
	if o.V != "" {
		return nil, &r27BadCurrency{"opaque"}
	}
	return []byte(`""`), nil
}

// The same defect in ordinary user code, with no net/* type: a validating enum
// whose MarshalJSON accepts its zero value and rejects the fabricated probe.
type r27Currency string

func (c r27Currency) MarshalJSON() ([]byte, error) {
	switch c {
	case "", "USD", "EUR":
		return []byte(`"` + string(c) + `"`), nil
	}
	return nil, &r27BadCurrency{string(c)}
}

type r27BadCurrency struct{ v string }

func (e *r27BadCurrency) Error() string { return "bad currency " + e.v }

type r27TermA struct {
	Days int `json:"days"`
}
type r27TermB struct {
	Months int `json:"months"`
}
type r27PriceA struct {
	Cur   r27Currency `json:"cur"`
	Terms []r27TermA  `json:"terms"`
	Memo  string      `json:"memo,omitempty"`
}
type r27PriceB struct {
	Cur   r27Currency `json:"cur"`
	Terms []r27TermB  `json:"terms"`
	Memo  string      `json:"memo,omitempty"`
}

// The wire-identical modernization must not be rejected end to end, in EITHER
// direction. The forward direction is safe on the recorded side alone (an empty
// recorded shape has always been skipped); the reverse is the one that needed
// call.go to stop treating an empty CURRENT shape as a shape to compare against.
func TestResultShape_NetIPModernizationDoesNotWedgeReplay(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27LeaseIP, error) {
		return r27LeaseIP{net.ParseIP("10.0.0.1"), []r27Item{{"sku-1"}}}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[r27LeaseIP, r27LeaseAddr](t, h, "lease-modern")
	if err != nil {
		t.Fatalf("a wire-identical net.IP -> netip.Addr refactor wedged the replay: %v", err)
	}
	if got.IP.String() != "10.0.0.1" || len(got.Items) != 1 || got.Items[0].SKU != "sku-1" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// THE REVERSE DIRECTION, which is the one the recorded-side skip alone does NOT
// cover: the checkpoint was written from netip.Addr and carries a real shape, and
// the type being replayed INTO is the net.IP form, which records none. Comparing
// "" as though it were a shape rejected this replay outright — a false fire on a
// change that cannot move a byte, and the reason call.go now skips on an empty
// CURRENT shape as well as an empty recorded one.
func TestResultShape_NetIPDemodernizationDoesNotWedgeReplay(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27LeaseAddr, error) {
		return r27LeaseAddr{netip.MustParseAddr("10.0.0.1"), []r27Item{{"sku-1"}}}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	// The premise: these two really are on opposite sides of the shape/no-shape
	// split, so this is not a vacuous pass.
	if a, b := fingerprintOf(r27LeaseAddr{}), fingerprintOf(r27LeaseIP{}); a == "" || b != "" {
		t.Fatalf("FIXTURE BROKEN: expected netip.Addr to record a shape and net.IP not to; got %q / %q", a, b)
	}
	_, got, err := writeThenReplay[r27LeaseAddr, r27LeaseIP](t, h, "lease-legacy")
	if err != nil {
		t.Fatalf("a wire-identical netip.Addr -> net.IP refactor wedged the replay: %v", err)
	}
	if got.IP.String() != "10.0.0.1" || len(got.Items) != 1 || got.Items[0].SKU != "sku-1" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// ---- a fixed-size array's ARITY is a DELIBERATE, ACCEPTED MISS --------------
//
// DO NOT "FIX" THIS. An array's length is part of its wire form, so shrinking
// `[3]float64` to `[2]float64` replays silently truncated data — encoding/json
// discards the extra elements with no error. Recording the arity in the shape was
// implemented and then REVERTED, because a single symmetric equality hash cannot
// express "arity 3 is compatible with unconstrained": whatever spelling gives
// `[3]T` a shape of its own necessarily makes it differ from `[]T`, and widening
// a fixed-size array to a slice is byte-identical on the wire AND decodes to the
// identical value. That trade swaps a LOW-severity miss for a HIGH-severity false
// fire, and this file's stated bias is the other way round: a miss leaves prior
// behaviour alone, a false fire wedges a live workflow with an error that is
// provably untrue.
//
// This test is the durable artefact of that decision. It pins BOTH ends of the
// range — N == 1 and N > 1 — because the reverted design happened to agree with a
// slice at N == 1, so a test that only checked N == 1 would not have noticed.

type r27SliceHolder struct {
	C []float64 `json:"c"`
}
type r27Coords1 struct {
	C [1]float64 `json:"c"`
}
type r27Coords3 struct {
	C [3]float64 `json:"c"`
}

func TestResultShape_ArrayArityIsADeliberateAcceptedMiss(t *testing.T) {
	slice := fingerprintOf(r27SliceHolder{})
	for _, tc := range []struct {
		name string
		v    any
	}{
		{"[1]T", r27Coords1{}},
		{"[3]T", r27Coords3{}},
	} {
		if got := fingerprintOf(tc.v); got != slice {
			t.Errorf("%s and []T must fingerprint the SAME (%s vs %s): telling them apart "+
				"turns the byte-identical widening [N]T -> []T into a false rejection, which "+
				"is strictly worse than the arity miss it buys — see the comment above",
				tc.name, got, slice)
		}
	}
	// The widening really is byte-identical and lossless, which is what makes a
	// false fire on it indefensible.
	requireWireAgreement(t, "[3]float64 -> []float64",
		r27Coords3{[3]float64{10, 20, 30}},
		r27SliceHolder{[]float64{10, 20, 30}}, true)
}

// ...and the accepted miss must be exactly that: a miss the replay survives, not
// a wedge. Driven through production's real write-then-replay path.
func TestResultShape_ArrayToSliceWideningReplaysCleanly(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27Coords3, error) {
		return r27Coords3{[3]float64{10, 20, 30}}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[r27Coords3, r27SliceHolder](t, h, "coords-widen")
	if err != nil {
		t.Fatalf("widening a fixed-size array to a slice wedged the replay: %v", err)
	}
	if len(got.C) != 3 || got.C[0] != 10 || got.C[2] != 30 {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// The array branch's other face: an array of POINTERS has to expose its element,
// or every [N]*T degrades to a null element and two different element types
// become indistinguishable.
type r27SlotA struct {
	Ref string `json:"ref"`
}
type r27SlotB struct {
	Code int `json:"code"`
}
type r27SlotsA struct {
	Slots [2]*r27SlotA `json:"slots"`
}
type r27SlotsB struct {
	Slots [2]*r27SlotB `json:"slots"`
}

func TestResultShape_ArrayOfPointersExposesItsElement(t *testing.T) {
	requireWireAgreement(t, "changed element type of a fixed-size array of pointers",
		r27SlotsA{[2]*r27SlotA{{"r"}, {"r"}}},
		r27SlotsB{[2]*r27SlotB{{1}, {1}}},
		false)
}

// A SLICE ELEMENT is addressable, which is what lets encoding/json reach a
// POINTER-receiver MarshalJSON that the same value never invokes when boxed on
// its own. So a validating pointer-receiver marshaler rejects the probe only in
// that position — which is why the shape for these types depends on WHERE the
// marshaler sits, and why nothing in this file models that itself: the real
// encoder is handed the real value and decides.
type r27Sealed struct {
	V string `json:"v"`
}

func (s *r27Sealed) MarshalJSON() ([]byte, error) {
	if s.V != "" {
		return nil, &r27BadCurrency{"sealed"}
	}
	return []byte(`{"v":""}`), nil
}

type r27SealedB struct {
	Code int `json:"code"`
}

func (s *r27SealedB) MarshalJSON() ([]byte, error) {
	if s.Code != 0 {
		return nil, &r27BadCurrency{"sealed"}
	}
	return []byte(`{"code":0}`), nil
}

type r27BoxA struct {
	Items []r27Sealed `json:"items"`
	Note  string      `json:"note"`
}
type r27BoxB struct {
	Items []r27SealedB `json:"items"`
	Note  string       `json:"note"`
}

// Both record no shape and are skipped; pinned in
// TestResultShape_ValidatingMarshalerIsADeliberateAcceptedMiss along with the
// rest of the probe-rejecting family.

// ---- a validating map KEY rejects the probe too -----------------------------
//
// A map KEY is fabricated by synthesizeMapKey, so a validating key type rejects
// the probe from a position no per-member substitution could ever have reached.
// Deliberately NOT a string kind: encoding/json takes a string-kind map key
// verbatim and never consults its MarshalText, so a string-based fixture would
// pass whatever the policy was.
type r27Zone int

func (z r27Zone) MarshalText() ([]byte, error) {
	if z != 7 {
		return nil, &r27BadCurrency{"zone"}
	}
	return []byte("us"), nil
}

type r27ZoneMap struct {
	M map[r27Zone]int `json:"m"`
}

// A validating map KEY rejects the probe the same way a validating member does,
// and takes the same accepted-miss path; pinned in
// TestResultShape_ValidatingMarshalerIsADeliberateAcceptedMiss.

// ---- (e) a POINTER-receiver marshaler is only reachable where encoding/json
// can take an address --------------------------------------------------------
//
// encoding/json wraps a pointer-receiver MarshalJSON in a condAddrEncoder, which
// falls back to the plain encoder when the value is NOT addressable. This file
// does not model that rule — a previous revision did, and a mirror of an encoder
// rule is exactly the class of divergence resultFingerprint was rebuilt to
// eliminate. The real encoder resolves it, and these cases pin that the outcome
// still tracks it.
//
// Verified against the real encoder (go1.25): a root value, a struct field, a
// map value and an array element are NOT addressable, so `{"id":"a","note":"n"}`
// comes out and the marshaler is never called, and the type gets a full shape; a
// slice element, a struct field INSIDE a slice element, and anything behind a
// pointer ARE, so the marshaler runs, rejects the probe, and the type records no
// shape at all (the accepted miss). The SPLIT between those two groups is what
// this table pins: if addressability stopped showing through, every row would
// land in the same group.

type r27PtrSealed struct {
	ID string `json:"id"`
	// omitempty, so zeroing this member is visible in the shape. Without it the
	// zeroed struct describes exactly like the populated one and none of the
	// assertions below can tell the two paths apart.
	Note string `json:"note,omitempty"`
}

func (s *r27PtrSealed) MarshalJSON() ([]byte, error) {
	if s.ID != "" {
		return nil, &r27BadCurrency{"sealed"}
	}
	return []byte(`{"id":""}`), nil
}

// Byte-for-byte the same struct with NO marshaler. At a non-addressable position
// the two serialize identically, so they must fingerprint identically.
type r27PlainSealed struct {
	ID   string `json:"id"`
	Note string `json:"note,omitempty"`
}

// A different member set behind the same pointer-receiver marshaler: at a
// non-addressable position these serialize differently, so they must NOT
// fingerprint the same.
type r27PtrSealedB struct {
	ID   string `json:"id"`
	Memo string `json:"memo,omitempty"`
}

func (s *r27PtrSealedB) MarshalJSON() ([]byte, error) {
	if s.ID != "" {
		return nil, &r27BadCurrency{"sealed"}
	}
	return []byte(`{"id":""}`), nil
}

type r27FieldSealed struct {
	X r27PtrSealed `json:"x"`
}
type r27FieldPlain struct {
	X r27PlainSealed `json:"x"`
}
type r27FieldSealedB struct {
	X r27PtrSealedB `json:"x"`
}
type r27MapSealed struct {
	M map[string]r27PtrSealed `json:"m"`
}
type r27ArraySealed struct {
	A [2]r27PtrSealed `json:"a"`
}
type r27SliceSealed struct {
	S []r27PtrSealed `json:"s"`
}
type r27SliceFieldSealed struct {
	S []r27FieldSealed `json:"s"`
}
type r27PtrHolder struct {
	P *r27PtrSealed `json:"p"`
}

// A PROMOTED (untagged embedded) struct carries its container's addressability
// through to the members it splices in: json reaches them as v.Field(i).Field(j),
// which is addressable exactly when v is. Verified against the real encoder — the
// same value emits `note` at a root and is rejected inside a slice element.
type r27SealedBase struct {
	X r27PtrSealed `json:"x"`
}
type r27EmbSealedRoot struct {
	r27SealedBase
}
type r27EmbSealedInSlice struct {
	S []r27EmbSealedRoot `json:"s"`
}

// Each want is what the REAL encoder's addressability rules produce. Every one of
// them moves if addressability stops showing through — in either direction.
func TestResultShape_AddressabilityMatchesTheEncoder(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{"an interface-boxed root is not addressable",
			reflect.TypeOf(r27PtrSealed{}), "{id:string,note:string}"},
		{"a struct field inherits: not addressable under a root",
			reflect.TypeOf(r27FieldSealed{}), "{x:{id:string,note:string}}"},
		{"a map value is never addressable",
			reflect.TypeOf(r27MapSealed{}), "{m:{1:{id:string,note:string}}}"},
		{"an array element inherits: not addressable under a root",
			reflect.TypeOf(r27ArraySealed{}), "{a:[{id:string,note:string}]}"},
		{"a slice element is always addressable",
			reflect.TypeOf(r27SliceSealed{}), ""},
		{"a struct field inherits: addressable inside a slice element",
			reflect.TypeOf(r27SliceFieldSealed{}), ""},
		{"a dereferenced pointer is always addressable",
			reflect.TypeOf(r27PtrHolder{}), ""},
		{"a promoted embed inherits: not addressable under a root",
			reflect.TypeOf(r27EmbSealedRoot{}), "{x:{id:string,note:string}}"},
		{"a promoted embed inherits: addressable inside a slice element",
			reflect.TypeOf(r27EmbSealedInSlice{}), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ResultShapeStringForTest(tc.typ)
			if got != tc.want {
				t.Errorf("shape = %q, want %q", got, tc.want)
			}
			// An empty want here means "the encoder reached the marshaler and it
			// rejected the probe" — the accepted miss. Assert the encoder really
			// does reject it, so an empty want can never be satisfied by some
			// unrelated failure to compute a shape.
			if tc.want == "" {
				if _, err := json.Marshal(reflect.New(tc.typ).Elem().Interface()); err == nil {
					probed, _ := synthesize(tc.typ, 0, nil)
					if _, err := json.Marshal(probed.Interface()); err == nil {
						t.Errorf("expected the populated probe of %s to be REJECTED at an "+
							"addressable position, but encoding/json accepted it", tc.typ)
					}
				}
			}
		})
	}
}

// The parity face, stated as the encoder states it: at a non-addressable
// position encoding/json really does emit `note`, so the shape has to describe
// it.
func TestResultShape_NonAddressablePointerMarshalerIsNotConsulted(t *testing.T) {
	v := r27FieldSealed{r27PtrSealed{ID: "a", Note: "n"}}
	wire, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("FIXTURE BROKEN: encoding/json rejected a non-addressable position: %v", err)
	}
	if string(wire) != `{"x":{"id":"a","note":"n"}}` {
		t.Fatalf("FIXTURE BROKEN: encoding/json emitted %s", wire)
	}
	if shape := ResultShapeStringForTest(reflect.TypeOf(v)); !strings.Contains(shape, "note") {
		t.Errorf("encoding/json emits member \"note\" for this type but the shape does not "+
			"describe it: %s", shape)
	}
}

// FALSE FIRE face: giving an existing struct a pointer-receiver MarshalJSON is
// inert on the wire at a non-addressable position, so it must not move the
// fingerprint.
func TestResultShape_AddingAPointerMarshalerIsInertWhereJSONIgnoresIt(t *testing.T) {
	requireWireAgreement(t, "adding a pointer-receiver marshaler at a non-addressable field",
		r27FieldPlain{r27PlainSealed{"a", "n"}},
		r27FieldSealed{r27PtrSealed{"a", "n"}}, true)
}

// MISS face: two different member sets behind that marshaler still serialize
// differently there, so they must still be told apart.
func TestResultShape_SealedMembersAtANonAddressableFieldAreStillToldApart(t *testing.T) {
	requireWireAgreement(t, "note -> memo behind a pointer-receiver marshaler",
		r27FieldSealed{r27PtrSealed{"a", "n"}},
		r27FieldSealedB{r27PtrSealedB{"a", "m"}}, false)
}

// End to end, through production's real write-then-replay path: the inert change
// must not wedge a live workflow.
func TestResultShape_InertPointerMarshalerDoesNotWedgeReplay(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r27FieldPlain, error) {
		return r27FieldPlain{r27PlainSealed{"a", "n"}}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[r27FieldPlain, r27FieldSealed](t, h, "sealed")
	if err != nil {
		t.Fatalf("adding a pointer-receiver marshaler where encoding/json never consults it "+
			"wedged the replay: %v", err)
	}
	if got.X.ID != "a" || got.X.Note != "n" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// ---- (f) a marshaler that ACCEPTS the probe keeps its populated shape -------
//
// Every other validating fixture in this file REJECTS the probe, so all of them
// take the zero path and none of them can tell "probe the member, keep it if it
// is fine" from "zero every member that has a marshaler at all". The second is
// the whole-type collapse this round was opened to fix, reintroduced per member.

type r27Tag struct {
	Name string `json:"name"`
}
type r27Tags []r27Tag

// The conversion drops the method, so this does not recurse.
func (v r27Tags) MarshalJSON() ([]byte, error) { return json.Marshal([]r27Tag(v)) }

type r27Code struct {
	Code int `json:"code"`
}
type r27Codes []r27Code

func (v r27Codes) MarshalJSON() ([]byte, error) { return json.Marshal([]r27Code(v)) }

type r27TagBoxA struct {
	Tags r27Tags `json:"tags"`
}
type r27TagBoxB struct {
	Tags r27Codes `json:"tags"`
}

// r27CleanA / r27CleanB are the STRUCT form of the same fixture, and they carry
// what this section was written to check. See the container pair below for why
// the check moved off r27Tags.
type r27CleanA struct {
	Name string `json:"name"`
}

// The local type drops the method, so this does not recurse.
func (v r27CleanA) MarshalJSON() ([]byte, error) { type raw r27CleanA; return json.Marshal(raw(v)) }

type r27CleanB struct {
	Code int `json:"code"`
}

func (v r27CleanB) MarshalJSON() ([]byte, error) { type raw r27CleanB; return json.Marshal(raw(v)) }

type r27CleanBoxA struct {
	Tag r27CleanA `json:"tag"`
}
type r27CleanBoxB struct {
	Tag r27CleanB `json:"tag"`
}

func TestResultShape_CleanMarshalerKeepsItsPopulatedShape(t *testing.T) {
	requireWireAgreement(t, "changed member type behind a clean value-receiver marshaler",
		r27CleanBoxA{r27CleanA{"n"}}, r27CleanBoxB{r27CleanB{1}}, false)

	if shape := ResultShapeStringForTest(reflect.TypeOf(r27CleanBoxA{})); shape != "{tag:{name:string}}" {
		t.Errorf("a marshaler that accepts the probe must keep its populated shape, got %s", shape)
	}
}

// TestResultShape_CleanContainerMarshalerIsADeliberateAcceptedMiss is what this
// same assertion became once the marshaler sits on a SLICE, and it is a real
// loss stated rather than quietly dropped.
//
// r27Tags/r27Codes ARE clean pass-throughs: their MarshalJSON emits exactly what
// the default encoding would, so the probe's `[{name:string}]` described the type
// correctly and this test used to require it. It no longer holds, because
// NOTHING AVAILABLE TO A PROBE DISTINGUISHES THIS MARSHALER FROM A HOSTILE ONE.
// build synthesizes exactly one element, so `type Bounds []int` emitting
// {"from":b[0],"to":b[1]} at len==2 and the default array otherwise is
// indistinguishable from r27Tags at the only length the probe can produce — and
// that one shaped as `[number]` against the byte-identical struct form's
// {from:number,to:number}, a false fire on a deploy that cannot move a byte.
// Telling them apart needs a second fabricated arity, which is the "substitute a
// value at a boundary and let encoding/json decide its fate" family that every
// false fire in this file has come from.
//
// SO THE COST IS PAID HERE: a result type carrying a container type with its own
// MarshalJSON records no shape and replays exactly as it did before this feature
// existed. The r27Tags -> r27Codes change below really does serialize
// differently and really is no longer told apart. That is a MISS, which leaves
// prior behaviour in place, chosen over a FALSE FIRE, which wedges a live
// workflow — the same trade the interface member, the depth cap, the validating
// marshaler and json.RawMessage all already take. See probeSpeaksForContainer.
func TestResultShape_CleanContainerMarshalerIsADeliberateAcceptedMiss(t *testing.T) {
	// FIXTURE PREMISE: these two really do serialize differently, so what follows
	// is a miss and not a vacuous pass.
	ba, err := json.Marshal(r27TagBoxA{r27Tags{{"n"}}})
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}
	bb, err := json.Marshal(r27TagBoxB{r27Codes{{1}}})
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}
	if string(ba) == string(bb) {
		t.Fatalf("FIXTURE BROKEN — the two types must differ on the wire, both gave %s", ba)
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(r27TagBoxA{}), reflect.TypeOf(r27TagBoxB{})} {
		if shape := ResultShapeStringForTest(typ); shape != "" {
			t.Errorf("%s recorded shape %q; a container carrying its own marshaler "+
				"cannot be described from its type", typ, shape)
		}
	}
}

// ---- (g) the depth budget, pinned at EVERY nesting site ---------------------
//
// Before termination became its own bound, a budget site that stopped charging
// was caught by accident: the probe ran off a self-referential fixture and died
// on a stack overflow. The visited-type set correctly removes that crash, and
// with it the accidental detector — so every nesting site, and the constant
// itself, could be loosened with a green suite. Every such move silently churns
// the fingerprint of every deep result type, which is the whole failure mode
// this file exists to prevent.
//
// WHAT CHANGED IN THIS REVISION. Reaching maxShapeDepth no longer TRUNCATES a
// member to a substituted value; it records no shape for the whole type. So the
// old form of this test — "at the cap the leaf is described, one deeper it is
// null" — no longer describes anything, and its hand-written six-and-seven-level
// fixtures stopped straddling a cap that is now 32. Hand-writing 32-level
// fixtures per site would be unreadable and would rot the next time the constant
// moves, so the boundary is LOCATED instead: each fixture is wrapped in k plain
// nesting levels and k is raised until the shape goes empty. The k at which that
// happens is a pure function of how much budget the site under test charges, and
// it is compared against a hardcoded expectation derived from maxShapeDepth.
//
// A site that stops charging moves its boundary one level DEEPER; a site that
// starts charging when it should not moves it one level SHALLOWER. Both fail.

// r28Leaf is the bottom of every fixture below: one string member, one JSON
// level below whatever contains it.
type r28Leaf struct {
	L string `json:"l"`
}

// The nesting sites, each expressed as a fixture whose leaf string sits a known
// number of JSON levels below the fixture's own position. That number — d in the
// table — is the ONLY thing under test: it is exactly the budget the site
// charges, and the boundary is at k+d == maxShapeDepth.

// d=1. An ordinary tagged struct member. The baseline every other row is read
// against.
type r28SiteTagged struct {
	L string `json:"l"`
}

// d=1. UNTAGGED and not anonymous: json nests it under its Go field name, so it
// charges exactly like a tagged one. promotesIntoParent's tag check
// short-circuits every tagged fixture, so only an untagged one distinguishes
// "promotes because anonymous" from "promotes because it has no json name".
type r28SiteUntagged struct {
	L string
}

// d=1. A POINTER is dereferenced by encoding/json, adds no JSON level and must
// charge nothing — so this sits on the SAME boundary as the baseline. If the
// pointer case ever spends budget, this row moves one level shallower and the
// wire-identical refactor `T -> *T` starts churning every deep fingerprint.
type r28SitePointer struct {
	P *string `json:"p"`
}

// d=1. An UNTAGGED EMBED promotes its members into the parent object, so it adds
// no JSON level either. Same boundary as the baseline.
type r28SiteEmbed struct {
	r28Leaf
}

// d=1. An untagged embedded POINTER promotes identically once json follows it.
type r28SiteEmbedPtr struct {
	*r28Leaf
}

// d=2. A TAGGED embed does NOT promote — json nests it under its tag — so it
// charges one level.
type r28SiteTaggedEmbed struct {
	r28Leaf `json:"e"`
}

// d=2. An embedded NON-STRUCT is emitted under a key named after its type, so it
// nests like any ordinary member and charges one level; its element charges the
// second.
type R28Names []string

type r28SiteEmbedNonStruct struct {
	R28Names
}

// d=2. Slice, array and map each put their element one level below the member
// that holds them.
type r28SiteSlice struct {
	V []string `json:"v"`
}
type r28SiteArray struct {
	V [1]string `json:"v"`
}
type r28SiteMap struct {
	V map[string]string `json:"v"`
}

// r28FirstEmptyK returns the smallest number of plain nesting levels that, wrapped
// around inner, pushes the probe past maxShapeDepth and makes it record no shape.
// pwNestN is the wrapper — one tagged struct member per level, the baseline site.
func r28FirstEmptyK(inner reflect.Type) int {
	for k := 0; k <= maxShapeDepth+8; k++ {
		if ResultShapeStringForTest(pwNestN(inner, k)) == "" {
			return k
		}
	}
	return -1
}

func TestResultShape_DepthBudgetIsChargedAtEveryNestingSite(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		// d is how many JSON levels the fixture puts between its own position
		// and its leaf string. The boundary is where k+d exceeds maxShapeDepth,
		// so the first k that records no shape is maxShapeDepth-d+1.
		d int
		// leaf is a fragment that must appear in the shape one level inside the
		// boundary, so a green row cannot be an empty-vs-empty coincidence.
		leaf string
	}{
		{"tagged struct member (the baseline)", reflect.TypeOf(r28SiteTagged{}), 1, "l:string"},
		{"untagged struct member nests under its Go name", reflect.TypeOf(r28SiteUntagged{}), 1, "L:string"},
		// THE ROW THAT ACTUALLY PINS THE ANONYMITY GUARD. The row above cannot:
		// its member is a STRING, so promotesIntoParent's final
		// `ft.Kind() == reflect.Struct` answers "nests" whether or not
		// `if !sf.Anonymous` is there, and deleting that guard survived the whole
		// suite. Untagged, NOT anonymous and STRUCT-typed is the one combination
		// that distinguishes the two — and it is the most ordinary field in Go.
		{"untagged NON-ANONYMOUS struct member nests, so it charges one",
			reflect.TypeOf(r29SiteUntaggedStruct{}), 2, "Inner:{l:string}"},
		{"a pointer member charges nothing", reflect.TypeOf(r28SitePointer{}), 1, "p:string"},
		{"an untagged embed charges nothing", reflect.TypeOf(r28SiteEmbed{}), 1, "l:string"},
		{"an untagged embedded pointer charges nothing", reflect.TypeOf(r28SiteEmbedPtr{}), 1, "l:string"},
		{"a tagged embed nests, so it charges one", reflect.TypeOf(r28SiteTaggedEmbed{}), 2, "e:{l:string}"},
		{"an embedded non-struct nests, so it charges one", reflect.TypeOf(r28SiteEmbedNonStruct{}), 2, "R28Names:[string]"},
		{"a slice element charges one", reflect.TypeOf(r28SiteSlice{}), 2, "v:[string]"},
		{"an array element charges one", reflect.TypeOf(r28SiteArray{}), 2, "v:[string]"},
		{"a map value charges one", reflect.TypeOf(r28SiteMap{}), 2, "v:{1:string}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantK := maxShapeDepth - tc.d + 1
			gotK := r28FirstEmptyK(tc.typ)
			if gotK != wantK {
				t.Errorf("the probe first records NO shape at %d wrapping levels, want %d.\n"+
					"This site now charges %d level(s) of depth budget instead of %d, so the "+
					"truncation boundary moved and every deep result type's fingerprint just churned.",
					gotK, wantK, maxShapeDepth-gotK+1, tc.d)
				return
			}
			// One level inside the boundary the shape must still be REAL, or the
			// row above is comparing two empties and asserting nothing.
			inside := ResultShapeStringForTest(pwNestN(tc.typ, wantK-1))
			if !strings.Contains(inside, tc.leaf) {
				t.Errorf("one level inside the boundary the shape must still describe the leaf %q, got %s",
					tc.leaf, inside)
			}
		})
	}
}

// PAST THE CAP THERE IS NO SHAPE AT ALL — not a truncated one. That is the
// maintainer's ruling of this revision, and it is the single property that
// retires the false-fire family: at the boundary the walk stops rather than
// substituting a value for encoding/json to reinterpret. A substituted zero
// leaks through `omitempty` one way, a substituted non-nil pointer leaks through
// it the other way, and a nil leaks as JSON null; nothing is emitted here at all,
// so nothing can leak.
func TestResultShape_PastTheDepthCapRecordsNoShapeRatherThanATruncatedOne(t *testing.T) {
	past := pwNestN(reflect.TypeOf(r28SiteSlice{}), maxShapeDepth)
	shape := ResultShapeStringForTest(past)
	if shape != "" {
		t.Fatalf("past the cap the type must record NO shape, got %s.\n"+
			"A partial shape means a value was substituted at the boundary, which is exactly "+
			"how each of the last four revisions shipped a false fire", shape)
	}
	if fp := ResultFingerprintForTest(past); fp != "" {
		t.Errorf("no shape must mean no fingerprint, so replay skips the type; got %q", fp)
	}
	// And the skip is what makes it safe: a type past the cap can neither be
	// refused on replay nor refuse another, because Call compares only when BOTH
	// sides recorded a shape.
	inside := pwNestN(reflect.TypeOf(r28SiteSlice{}), maxShapeDepth-2)
	if ResultFingerprintForTest(inside) == "" {
		t.Errorf("a type comfortably inside the cap must still be guarded")
	}
}

// ---- (h) synthesize hands back a value of EXACTLY the type asked for --------
//
// reflect.New(t.Elem()) produces the UNNAMED pointer type, which is assignable to
// a named one but not identical — so without the conversion a `type P *Foo`
// member is populated with a *Foo. Assignability hides that at every Set site,
// which is why nothing failed; the invariant is worth holding anyway, because
// reflect.Zero(t) is spliced back in on the cycle path and truncate converts back
// to t on the depth boundary.
type r27NamedPtr *r27SlotA

func TestSynthesize_ReturnsExactlyTheRequestedType(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf((*r27NamedPtr)(nil)).Elem(),
		reflect.TypeOf((*r27SelfPtr)(nil)).Elem(),
		reflect.TypeOf((*r27PtrA)(nil)).Elem(),
		reflect.TypeOf((**r27SlotA)(nil)).Elem(),
		reflect.TypeOf(r27FlatRoot{}),
	} {
		v, ok := synthesize(typ, 0, nil)
		if !ok {
			t.Errorf("%s: synthesize failed", typ)
			continue
		}
		if v.Type() != typ {
			t.Errorf("synthesize(%s) returned a %s", typ, v.Type())
		}
	}
}

// ---- a ZERO-LENGTH array member must not switch the whole guard off ---------
//
// build's Array case guards its element population with `t.Len() > 0`. Loosening
// that to `>= 0` compiles, reads harmlessly, and left the entire suite green:
// under it `v.Index(0)` on a [0]T panics, resultShape's recover swallows the
// panic, and EVERY result type containing a zero-length array silently records
// no shape. Nothing noticed, because nothing had a [0]T member.
type r27ZeroArray struct {
	Slots [0]r27Item `json:"slots"`
	Name  string     `json:"name"`
}

func TestResultShape_ZeroLengthArrayMemberKeepsTheGuardOn(t *testing.T) {
	typ := reflect.TypeOf(r27ZeroArray{})
	// The premise: encoding/json marshals this type perfectly well, so there is
	// no legitimate reason for it to record no shape.
	if _, err := json.Marshal(r27ZeroArray{}); err != nil {
		t.Fatalf("FIXTURE BROKEN: encoding/json rejected the type: %v", err)
	}
	shape := ResultShapeStringForTest(typ)
	if shape == "" {
		t.Fatal("a result type with a [0]T member recorded NO shape, so the guard is " +
			"switched off for it and every one of its checkpoints replays unchecked")
	}
	// Not just non-empty: the sibling must actually be described, which is what a
	// panic-and-recover would have erased along with everything else.
	if !strings.Contains(shape, "name") || !strings.Contains(shape, "slots") {
		t.Fatalf("the [0]T member cost the type its real shape: %s", shape)
	}
	if fp := ResultFingerprintForTest(typ); fp == "" {
		t.Fatal("no fingerprint recorded for a type encoding/json marshals fine")
	}
}

// ---- the PAIRWISE property, swept mechanically ------------------------------
//
// Every other test in this package hand-enumerates one pair. But the property
// UPGRADE.md promises is pairwise and universal: two types whose encoder output
// is identical must fingerprint identically. Proving it one hand-written pair at
// a time is how three revisions each shipped a NEW false fire in a pair nobody
// had thought to write down — the T/*T split at the depth cap sat behind a
// comment claiming it was understood.
//
// So this generates the wire-identical variants MECHANICALLY, for every fixture
// listed, and requires the fingerprint to be stable across all of them:
//
//	member T          <-> member *T         (json dereferences; same bytes)
//	member []T        <-> member []*T       (same, per element)
//	member T          <-> member T,omitempty (inert on a POPULATED value, which
//	                                          is the only value the shape sees)
//
// A variant that records NO shape is not a violation on its own — that is the
// accepted-miss path — but it must be no-shape on BOTH sides of a pair, because
// half a pair having a shape is exactly the netip.Addr/net.IP false fire.
func TestResultShape_WireIdenticalVariantsFingerprintTheSame(t *testing.T) {
	fixtures := []reflect.Type{
		reflect.TypeOf(pPlain{}),
		reflect.TypeOf(pExpInner{}),
		reflect.TypeOf(pOmitScalars{}),
		reflect.TypeOf(pStructSlice{}),
		reflect.TypeOf(pNested{}),
		reflect.TypeOf(pArrayHolder{}),
		reflect.TypeOf(pTaggedExp{}),
		reflect.TypeOf(r27Item{}),
		reflect.TypeOf(r27FlatRoot{}),
		reflect.TypeOf(r27PriceA{}),
		reflect.TypeOf(r27ZeroArray{}),
		// Deep enough to land on the truncation boundary, where the T/*T split
		// lived. Without it this sweep would never reach the cap.
		reflect.TypeOf(pCapV1{}),
	}
	shaped := 0
	for _, base := range fixtures {
		t.Run(base.String(), func(t *testing.T) {
			variants := []struct {
				name string
				typ  reflect.Type
			}{
				{"member T", wrapIn(base, "")},
				{"member *T", wrapIn(reflect.PointerTo(base), "")},
				{"member T, omitempty", wrapIn(base, ",omitempty")},
				{"member []T", wrapIn(reflect.SliceOf(base), "")},
				{"member []*T", wrapIn(reflect.SliceOf(reflect.PointerTo(base)), "")},
			}
			// The two groups that are wire-identical WITHIN themselves. []T and T
			// are of course not identical to each other.
			for _, group := range [][]int{{0, 1, 2}, {3, 4}} {
				want := ResultShapeStringForTest(variants[group[0]].typ)
				wantFP := ResultFingerprintForTest(variants[group[0]].typ)
				if want != "" {
					shaped++
				}
				for _, i := range group[1:] {
					if got := ResultShapeStringForTest(variants[i].typ); got != want {
						t.Errorf("%s vs %s: shape %q vs %q — these serialize identically, so a "+
							"deploy swapping one for the other would be REJECTED on replay",
							variants[i].name, variants[group[0]].name, got, want)
					}
					if got := ResultFingerprintForTest(variants[i].typ); got != wantFP {
						t.Errorf("%s vs %s: fingerprint %q vs %q",
							variants[i].name, variants[group[0]].name, got, wantFP)
					}
				}
			}
			// Wire premise, checked rather than assumed: the two representations
			// really do emit the same bytes for a populated value.
			assertSameWire(t, variants[0].typ, variants[1].typ)
			assertSameWire(t, variants[3].typ, variants[4].typ)
		})
	}
	if shaped == 0 {
		t.Fatal("every variant in the sweep recorded no shape, so the comparison was vacuous")
	}
}

// wrapIn builds `struct{ X <typ> ` + "`json:\"x<opt>\"`" + ` }` so a fixture can be
// compared at a MEMBER position, which is where the representation swaps this
// sweep cares about actually happen.
func wrapIn(typ reflect.Type, opt string) reflect.Type {
	return reflect.StructOf([]reflect.StructField{{
		Name: "X",
		Type: typ,
		Tag:  reflect.StructTag(`json:"x` + opt + `"`),
	}})
}

// assertSameWire pins the premise of every pair above: the encoder really does
// emit the same bytes for both representations of the SAME synthesized value. If
// it does not, the pair is not wire-identical and requiring equal fingerprints
// would be wrong rather than right.
func assertSameWire(t *testing.T, a, b reflect.Type) {
	t.Helper()
	av, aok := synthesize(a, 0, nil)
	bv, bok := synthesize(b, 0, nil)
	if !aok || !bok {
		t.Fatalf("synthesize failed for %s / %s", a, b)
	}
	ab, aerr := json.Marshal(av.Interface())
	bb, berr := json.Marshal(bv.Interface())
	if aerr != nil || berr != nil {
		// Both must be refused, or one has a shape and the other does not — which
		// is the netip.Addr/net.IP false fire in miniature.
		if (aerr == nil) != (berr == nil) {
			t.Fatalf("%s and %s are wire-identical but only one marshals (%v / %v)", a, b, aerr, berr)
		}
		return
	}
	if string(ab) != string(bb) {
		t.Fatalf("PREMISE BROKEN: %s emits %s but %s emits %s", a, ab, b, bb)
	}
}
