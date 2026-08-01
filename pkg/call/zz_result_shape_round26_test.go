package call

import (
	"net"
	"reflect"
	"testing"
)

// ---- C1: unexported embedded NON-pointer structs -----------------------------
//
// encoding/json promotes their exported members, so the probe must populate them.
// Left zero, a promoted `omitempty` member disappears and a promoted
// pointer/slice/map member reports as null.

type r26HiddenOE struct {
	CreatedBy string `json:"created_by,omitempty"`
}
type r26EmbedOE struct {
	r26HiddenOE
	Amount int `json:"amount"`
}
type r26FlatOE struct {
	CreatedBy string `json:"created_by,omitempty"`
	Amount    int    `json:"amount"`
}

type r26HiddenMap struct {
	M map[string]int `json:"m"`
}
type r26EmbedMap struct {
	r26HiddenMap
	Amount int `json:"amount"`
}
type r26FlatMap struct {
	M      map[string]int `json:"m"`
	Amount int            `json:"amount"`
}

func TestResultShape_UnexportedEmbeddedStructIsPopulated(t *testing.T) {
	requireWireAgreement(t, "inlining an unexported embed with an omitempty member",
		r26EmbedOE{r26HiddenOE{"bob"}, 5}, r26FlatOE{"bob", 5}, true)
	requireWireAgreement(t, "inlining an unexported embed with a map member",
		r26EmbedMap{r26HiddenMap{map[string]int{"k": 7}}, 5},
		r26FlatMap{map[string]int{"k": 7}, 5}, true)
}

// The blind-spot face of the same bug: with the embed left zero, two genuinely
// different member sets collapsed to the same shape.
type r26HiddenX struct {
	CreatedBy string `json:"created_by,omitempty"`
}
type r26HiddenY struct {
	Approver string `json:"approver,omitempty"`
}
type r26PayoutX struct {
	r26HiddenX
	Amount int `json:"amount"`
}
type r26PayoutY struct {
	r26HiddenY
	Amount int `json:"amount"`
}

func TestResultShape_UnexportedEmbedRealChangeIsCaught(t *testing.T) {
	if fingerprintOf(r26PayoutX{}) == fingerprintOf(r26PayoutY{}) {
		t.Fatal("two different promoted member sets share a fingerprint; " +
			"a real result-type change would replay silently wrong")
	}
}

// ---- C2 / C8: the depth budget must count JSON nesting, not Go levels --------
//
// A pointer adds no JSON nesting. Charging depth for it made []*T truncate sooner
// than []T, so removing a pointer indirection — which cannot change a byte of
// JSON — moved members across the cap and changed the fingerprint.

type r26Line struct {
	SKU string `json:"sku"`
}
type r26OrderPtr struct {
	Lines []*r26Line `json:"lines"`
}
type r26OrderVal struct {
	Lines []r26Line `json:"lines"`
}
type r26RespPtr struct {
	Orders []*r26OrderPtr `json:"orders"`
}
type r26RespVal struct {
	Orders []r26OrderVal `json:"orders"`
}

func TestResultShape_PointerIndirectionSpendsNoDepth(t *testing.T) {
	requireWireAgreement(t, "[]*T -> []T at two levels", r26RespPtr{}, r26RespVal{}, true)

	// And the members must still be visible rather than truncated away.
	shape := ResultShapeStringForTest(reflect.TypeOf(r26RespPtr{}))
	if !containsSub(shape, "sku") {
		t.Fatalf("nested members truncated out of an ordinary 3-deep type: %s", shape)
	}
}

func containsSub(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

// ---- C3: the map KEY TYPE must not leak into the shape ----------------------

type r26MapStr struct {
	M map[string]int `json:"m"`
}
type r26MapInt struct {
	M map[int]int `json:"m"`
}

func TestResultShape_MapKeyTypeDoesNotLeak(t *testing.T) {
	// json renders every map key as a string, so these serialize identically for
	// the same entries and must not be told apart.
	if a, b := fingerprintOf(r26MapStr{}), fingerprintOf(r26MapInt{}); a != b {
		t.Fatalf("map[string]int and map[int]int fingerprint differently (%s vs %s) "+
			"despite identical JSON", a, b)
	}
}

// ---- C7: types that VALIDATE reject the synthesized probe -------------------

type r26Lease struct {
	IP      net.IP `json:"ip"`
	LeaseID string `json:"lease_id"`
}

// net.IP.MarshalText rejects any length other than 0, 4 or 16, so a fabricated
// one-byte address fails to marshal. Falling through to "no shape" silently
// disabled the guard for every result type containing one.
func TestResultShape_ValidatingTypeStillGetsAShape(t *testing.T) {
	if fp := fingerprintOf(r26Lease{}); fp == "" {
		t.Fatal("a result type containing net.IP produced NO shape, so replay " +
			"reads every new checkpoint as pre-upgrade and skips the check entirely")
	}
}

// The shallow []*T case above passes either way — three levels never approach the
// cap. This one sits ON the boundary: it is exactly deep enough that charging a
// pointer a depth level pushes the innermost member past maxShapeDepth, so the
// wire-identical change T -> *T truncates it away and the fingerprint moves.

// A SLICE, deliberately: a truncated value is the zero value, and a zero string
// still marshals as a JSON string — the shape would not move and the test could
// not see the truncation. A nil slice marshals as null, which is a different kind.
type r26D5 struct {
	Leaf []string `json:"leaf"`
}
type r26D4 struct {
	D5 r26D5 `json:"d5"`
}
type r26D3 struct {
	D4 r26D4 `json:"d4"`
}
type r26D2 struct {
	D3 r26D3 `json:"d3"`
}
type r26D1 struct {
	D2 r26D2 `json:"d2"`
}
type r26DeepVal struct {
	D1 r26D1 `json:"d1"`
}

// Identical, except one level is reached through a pointer. The JSON is the same
// object graph; only the Go indirection differs.
type r26D3Ptr struct {
	D4 *r26D4 `json:"d4"`
}
type r26D2Ptr struct {
	D3 r26D3Ptr `json:"d3"`
}
type r26D1Ptr struct {
	D2 r26D2Ptr `json:"d2"`
}
type r26DeepPtr struct {
	D1 r26D1Ptr `json:"d1"`
}

func TestResultShape_PointerAtTheDepthBoundaryDoesNotTruncate(t *testing.T) {
	// Populated on both sides: a nil *r26D4 would marshal as null and the two
	// would differ for a reason that has nothing to do with the depth budget.
	requireWireAgreement(t, "T -> *T at the depth boundary",
		r26DeepVal{r26D1{r26D2{r26D3{r26D4{r26D5{[]string{"x"}}}}}}},
		r26DeepPtr{r26D1Ptr{r26D2Ptr{r26D3Ptr{&r26D4{r26D5{[]string{"x"}}}}}}},
		true)

	// Both must still carry the innermost member; if the budget were spent on the
	// pointer, the pointered one would be truncated to null and agree only by
	// accident of both being truncated.
	for name, shape := range map[string]string{
		"value":   ResultShapeStringForTest(reflect.TypeOf(r26DeepVal{})),
		"pointer": ResultShapeStringForTest(reflect.TypeOf(r26DeepPtr{})),
	} {
		if !containsSub(shape, "leaf") {
			t.Errorf("%s: innermost member truncated away: %s", name, shape)
		}
	}
}
