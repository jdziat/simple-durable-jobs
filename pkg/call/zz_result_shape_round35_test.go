package call

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---------------------------------------------------------------------------
// MECHANISM 1: a CONTAINER type carrying its own marshaler over elements the
// probe cannot populate.
//
// build fills a slice with ONE element; time.Time is all-unexported so that
// element is the ZERO, and a marshaler that reports "no sample" for a zero
// timestamp therefore describes the probe rather than the type.
// ---------------------------------------------------------------------------

// r35cLastSeen is the slice form: the last sample, or null when there is none.
type r35cLastSeen []time.Time

func (s r35cLastSeen) MarshalJSON() ([]byte, error) {
	if len(s) == 0 || s[len(s)-1].IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(s[len(s)-1])
}

func (s *r35cLastSeen) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*s = nil
		return nil
	}
	var ts time.Time
	if err := json.Unmarshal(b, &ts); err != nil {
		return err
	}
	*s = r35cLastSeen{ts}
	return nil
}

// r35cWindow is the ARRAY form. Arity comes from the type, but build populates
// index 0 ONLY, so the marshaler still reads a fabricated zero.
type r35cWindow [2]time.Time

func (w r35cWindow) MarshalJSON() ([]byte, error) {
	if w[0].IsZero() || w[1].IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(map[string]any{"from": w[0], "to": w[1]})
}

// r35cIndex is the MAP form.
type r35cIndex map[string]time.Time

func (m r35cIndex) MarshalJSON() ([]byte, error) {
	for _, v := range m {
		if v.IsZero() {
			return []byte("null"), nil
		}
	}
	return json.Marshal(map[string]time.Time(m))
}

// ---------------------------------------------------------------------------
// MECHANISM 2: a container marshaler that branches on LENGTH. Every element is
// perfectly populatable, so hasUnpopulatedState reports false and simply
// consulting probeSpeaksForType from the container cases would NOT close this.
// build synthesizes exactly one element, so the len==1 branch is what gets
// described.
// ---------------------------------------------------------------------------

type r35cBounds []int

func (b r35cBounds) MarshalJSON() ([]byte, error) {
	if len(b) == 2 {
		return json.Marshal(map[string]int{"from": b[0], "to": b[1]})
	}
	return json.Marshal([]int(b))
}

type r35cBoundsStruct struct {
	From int `json:"from"`
	To   int `json:"to"`
}

// ---------------------------------------------------------------------------
// Result members.
// ---------------------------------------------------------------------------

type r35cSliceV1 struct {
	Seen r35cLastSeen `json:"seen"`
	N    int          `json:"n"`
}

type r35cSliceV2 struct {
	Seen *time.Time `json:"seen"`
	N    int        `json:"n"`
}

type r35cArrayV1 struct {
	W r35cWindow `json:"w"`
}

type r35cArrayV2 struct {
	W *r35cFromTo `json:"w"`
}

type r35cFromTo struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type r35cMapV1 struct {
	Idx r35cIndex `json:"idx"`
}

type r35cMapV2 struct {
	Idx map[string]time.Time `json:"idx"`
}

type r35cBoundsV1 struct {
	B r35cBounds `json:"b"`
}

type r35cBoundsV2 struct {
	B r35cBoundsStruct `json:"b"`
}

// CONTROL from the finding: the SAME marshaler one level up, on a struct that
// wraps the slice, already records NO shape. The gate exists; it is only wired
// into build's struct case.
type r35cWrapper struct{ samples []time.Time }

func (w r35cWrapper) MarshalJSON() ([]byte, error) {
	if len(w.samples) == 0 || w.samples[len(w.samples)-1].IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(w.samples[len(w.samples)-1])
}

type r35cWrappedV1 struct {
	Seen r35cWrapper `json:"seen"`
	N    int         `json:"n"`
}

func shapeOf35c(v any) string {
	return ResultShapeStringForTest(reflect.TypeOf(v))
}

// TestResultShape_ContainerOwnMarshalerRecordsNoShape pins the rule: a slice,
// array or map type that carries its OWN json.Marshaler records NO shape.
//
// Its wire form is a function of its CONTENTS and the probe can only ever
// produce one element, so the shape is not derivable from the type at all —
// the same condition as an interface member or a json.RawMessage, and it takes
// the same answer.
func TestResultShape_ContainerOwnMarshalerRecordsNoShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		v1   any
		v2   any
	}{
		{"slice-typed marshaler", r35cSliceV1{}, r35cSliceV2{}},
		{"array-typed marshaler", r35cArrayV1{}, r35cArrayV2{}},
		{"map-typed marshaler", r35cMapV1{}, r35cMapV2{}},
		{"length-branching slice marshaler", r35cBoundsV1{}, r35cBoundsV2{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := shapeOf35c(tc.v1)
			if got != "" {
				t.Fatalf("recorded shape %q; a container carrying its own marshaler "+
					"describes its CONTENTS, which the probe fabricates, so it must "+
					"record no shape (the simplification shapes as %q)",
					got, shapeOf35c(tc.v2))
			}
		})
	}
}

// TestResultShape_ContainerMarshalerFalseFireIsGone drives the finding's exact
// deploy end to end: the checkpoint written by the slice-typed form must be
// accepted by the byte-identical *time.Time form.
func TestResultShape_ContainerMarshalerFalseFireIsGone(t *testing.T) {
	ts := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	// PREMISE, proved rather than asserted: the two types are byte-identical on
	// both branches, so no replay may distinguish them.
	for _, row := range []struct {
		name string
		a, b any
	}{
		{"present", r35cSliceV1{Seen: r35cLastSeen{ts}, N: 3}, r35cSliceV2{Seen: &ts, N: 3}},
		{"absent", r35cSliceV1{N: 3}, r35cSliceV2{N: 3}},
	} {
		ab, err := json.Marshal(row.a)
		if err != nil {
			t.Fatalf("%s: marshal v1: %v", row.name, err)
		}
		bb, err := json.Marshal(row.b)
		if err != nil {
			t.Fatalf("%s: marshal v2: %v", row.name, err)
		}
		if string(ab) != string(bb) {
			t.Fatalf("%s: fixture is not byte-identical: %s vs %s", row.name, ab, bb)
		}
		t.Logf("%-8s shared wire: %s", row.name, ab)
	}

	h, herr := handler.NewHandler(func(_ context.Context, _ string) (r35cSliceV1, error) {
		return r35cSliceV1{Seen: r35cLastSeen{ts}, N: 3}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[r35cSliceV1, r35cSliceV2](t, h, "last-seen")
	t.Logf("persisted result bytes: %s  persisted shape: %q", saved.Result, saved.ResultShape)
	if err != nil {
		t.Fatalf("FALSE FIRE at replay: the deploy cannot move a byte, yet Call "+
			"refused it and returned %+v: %v", got, err)
	}
	if got.Seen == nil || !got.Seen.Equal(ts) || got.N != 3 {
		t.Fatalf("replay decoded losslessly? got %+v want seen=%v n=3", got, ts)
	}
}

// TestResultShape_ContainerRuleIsConsistentWithTheStructCase pins the CONTROL
// that made this an inconsistency rather than a design choice: the same
// marshaler on a struct wrapping the slice already recorded no shape.
func TestResultShape_ContainerRuleIsConsistentWithTheStructCase(t *testing.T) {
	if got := shapeOf35c(r35cWrappedV1{}); got != "" {
		t.Fatalf("control regressed: the struct-wrapped form must still record no shape, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// THE OTHER DIRECTION. A rule that disarmed a plain slice would be far too
// wide, so every family that must KEEP its shape is checked by execution.
// ---------------------------------------------------------------------------

type r35cPlain struct {
	Names  []string             `json:"names"`
	Ages   map[string]int       `json:"ages"`
	Pair   [2]int               `json:"pair"`
	Stamps []time.Time          `json:"stamps"`
	Rows   []r35cRow            `json:"rows"`
	Deep   map[string][]float64 `json:"deep"`
	D      time.Duration        `json:"d"`
	Raw    []byte               `json:"raw"`
	Named  r35cNamedSlice       `json:"named"`
}

// r35cNamedSlice is a NAMED container with no marshaler of its own — the case
// closest to the rule's edge, and the one that would prove it too wide.
type r35cNamedSlice []string

type r35cRow struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// r35cHex is a named []byte carrying only an encoding.TextMarshaler. Its wire
// form is a JSON STRING by construction whatever its contents, so it cannot
// misrepresent structure and must keep its shape — the same line
// probeSpeaksForType already draws.
type r35cHex []byte

func (h r35cHex) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%x", []byte(h))), nil
}

func (h *r35cHex) UnmarshalText(b []byte) error { *h = r35cHex(b); return nil }

// r35cUUID is the [16]byte-with-MarshalText family (google/uuid, gofrs/uuid).
type r35cUUID [16]byte

func (u r35cUUID) MarshalText() ([]byte, error) { return []byte(fmt.Sprintf("%x", u[:])), nil }

type r35cTextContainers struct {
	H r35cHex  `json:"h"`
	U r35cUUID `json:"u"`
}

func TestResultShape_PlainContainersKeepTheirShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
		want string
	}{
		{"plain containers and scalars", r35cPlain{},
			"{ages:{1:number},d:number,deep:{1:[number]},named:[string],names:[string]," +
				"pair:[number],raw:string,rows:[{a:number,b:string}],stamps:[string]}"},
		{"text-marshaler containers", r35cTextContainers{}, "{h:string,u:string}"},
		// This repository's ONLY production MarshalJSON. core.UUID is a named
		// STRING, not a container, so the rule must not reach it — checked here
		// rather than argued, because "no real type is affected" is the claim a
		// too-wide rule always comes with.
		{"the repo's own marshaler type", struct {
			ID core.UUID `json:"id"`
		}{}, "{id:string}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shapeOf35c(tc.v); got != tc.want {
				t.Fatalf("shape = %q, want %q — the container rule must not reach a "+
					"container that carries no json.Marshaler of its own", got, tc.want)
			}
		})
	}
}

// TestResultShape_NetIPIsUnchangedByTheContainerRule records what net.IP does and
// WHY, because it is the container everyone reaches for when asking whether this
// rule went too wide.
//
// It does not implement json.Marshaler at all — only encoding.TextMarshaler — so
// the container rule never sees it. It already recorded NO shape before this
// change, for an unrelated reason: its MarshalText VALIDATES, rejects the
// one-byte probe build fabricates, and resultShape's marshal error path fires.
// That is the pre-existing validating-marshaler accepted miss, not a new cost.
func TestResultShape_NetIPIsUnchangedByTheContainerRule(t *testing.T) {
	ipType := reflect.TypeOf(net.IP{})
	if ipType.Implements(jsonMarshalerType) || reflect.PointerTo(ipType).Implements(jsonMarshalerType) {
		t.Fatal("net.IP grew a json.Marshaler; the cost note above needs rewriting")
	}
	if !ipType.Implements(textMarshalerType) {
		t.Fatal("net.IP lost its TextMarshaler; the cost note above needs rewriting")
	}
	if _, err := json.Marshal(net.IP{1}); err == nil {
		t.Fatal("net.IP stopped validating the probe; it may now be guarded and the note is stale")
	}
	type withIP struct {
		IP net.IP `json:"ip"`
		N  int    `json:"n"`
	}
	if got := shapeOf35c(withIP{}); got != "" {
		t.Fatalf("net.IP shape = %q; it recorded none before this change too", got)
	}
}

// TestResultShape_RawMessageIsSubsumedByTheContainerRule asserts the claim
// build's Slice case now makes about its own json.RawMessage clause.
//
// That clause is kept, and it is now redundant: no fixture can red it, because
// the container rule refuses RawMessage too. Rather than leave the claim as
// prose, check it — the container rule alone must answer "no shape" for
// RawMessage. If a later narrowing of the rule breaks that, this reds and the
// comment beside the clause stops being a lie even though behaviour has not
// changed.
func TestResultShape_RawMessageIsSubsumedByTheContainerRule(t *testing.T) {
	if probeSpeaksForContainer(jsonRawMessageType) {
		t.Fatal("the container rule no longer covers json.RawMessage; build's dedicated " +
			"clause is now the only thing holding that behaviour up and the comment " +
			"beside it, which says the two agree, is wrong")
	}
	type withRaw struct {
		R json.RawMessage `json:"r"`
		N int             `json:"n"`
	}
	if got := shapeOf35c(withRaw{}); got != "" {
		t.Fatalf("json.RawMessage must still record no shape, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// WHAT THIS COSTS, measured rather than argued.
//
// Each type below recorded a real shape before the container rule and records
// none after it, so a result type carrying one replays exactly as it did before
// the feature existed. The `want` column is what the shape WAS, taken by running
// the same fixture against the parent commit — it is in the fixture name so a
// reader can see the size of the trade without re-deriving it.
//
//	type Hex []byte      MarshalJSON -> hex string        {h:string}    -> none
//	type Set map[K]struct{}  marshals as a sorted array   {s:[string]}  -> none
//	type Ordered []entry     marshals as an object        {o:{x:number}} -> none
//
// THE THIRD ONE IS NOT PURELY A LOSS and is worth reading twice: its recorded
// key was `x`, which is the STRING build fabricated for the entry, not anything
// the type declares. Renaming nothing and changing nothing about that type would
// have moved that key the moment build's placeholder changed, and any two
// ordered maps with different real keys fingerprinted the same. So for that
// family the old shape was a false fire waiting on a fixture, not coverage.
// ---------------------------------------------------------------------------

type r35cHexJSON []byte

func (h r35cHexJSON) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%x", []byte(h)))
}

type r35cSet map[string]struct{}

func (s r35cSet) MarshalJSON() ([]byte, error) {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	return json.Marshal(out)
}

type r35cOrderedEntry struct {
	K string
	V int
}

type r35cOrdered []r35cOrderedEntry

func (o r35cOrdered) MarshalJSON() ([]byte, error) {
	m := make(map[string]int, len(o))
	for _, e := range o {
		m[e.K] = e.V
	}
	return json.Marshal(m)
}

func TestResultShape_ContainerRuleAcceptedMisses(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    any
		was  string
	}{
		{"scalar-emitting []byte marshaler", struct {
			H r35cHexJSON `json:"h"`
		}{}, "{h:string}"},
		{"set backed by a map", struct {
			S r35cSet `json:"s"`
		}{}, "{s:[string]}"},
		{"ordered map backed by a slice", struct {
			O r35cOrdered `json:"o"`
		}{}, "{o:{x:number}}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shapeOf35c(tc.v); got != "" {
				t.Fatalf("shape = %q; this family records no shape by design (it was %q "+
					"before the container rule)", got, tc.was)
			}
		})
	}
}

// TestResultShape_ContainerMarshalerReachesEveryPosition checks the rule fires
// wherever the container can sit, not only as a named struct member: as the
// RESULT type itself, behind a pointer, as a map value and as a slice element.
//
// The final row is a CONTROL that was already green: an embedded named slice
// PROMOTES its MarshalJSON onto the parent struct, so build's struct case gates
// it through probeSpeaksForType and always did. It is here so a later
// restructuring of that case cannot quietly drop it.
func TestResultShape_ContainerMarshalerReachesEveryPosition(t *testing.T) {
	type ptrMember struct {
		S *r35cLastSeen `json:"s"`
	}
	type mapValue struct {
		M map[string]r35cLastSeen `json:"m"`
	}
	type sliceElem struct {
		S []r35cLastSeen `json:"s"`
	}
	type embedded struct {
		r35cLastSeen
		N int `json:"n"`
	}
	for _, tc := range []struct {
		name string
		typ  reflect.Type
	}{
		{"result type itself", reflect.TypeOf(r35cLastSeen{})},
		{"behind a pointer", reflect.TypeOf(ptrMember{})},
		{"as a map value", reflect.TypeOf(mapValue{})},
		{"as a slice element", reflect.TypeOf(sliceElem{})},
		{"embedded", reflect.TypeOf(embedded{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResultShapeStringForTest(tc.typ); got != "" {
				t.Fatalf("shape = %q, want no shape", got)
			}
		})
	}
}

// r35cPtrRecv carries a POINTER-receiver marshaler on a named slice. It is
// reachable through any address — a slice element, anything behind a pointer —
// and invisible to a value-form probe, which is why the check reads the pointer
// form for both receiver kinds exactly as probeSpeaksForType does.
type r35cPtrRecv []int

func (p *r35cPtrRecv) MarshalJSON() ([]byte, error) {
	if p == nil || len(*p) != 2 {
		return json.Marshal([]int(*p))
	}
	return json.Marshal(map[string]int{"lo": (*p)[0], "hi": (*p)[1]})
}

func TestResultShape_PointerReceiverContainerMarshalerIsCovered(t *testing.T) {
	type holder struct {
		P *r35cPtrRecv `json:"p"`
	}
	if got := shapeOf35c(holder{}); got != "" {
		t.Fatalf("a pointer-receiver marshaler on a named slice is reachable through "+
			"an address, so it must record no shape; got %q", got)
	}
}

// TestResultShape_ContainerMarshalerTerminates bounds the new check against the
// self-referential container types the unwrap walk has already hung on once:
// `type S []S`, `type M map[string]M` and `type A [1]*A` are legal Go, and a
// check that walked a container's element type instead of asking about the
// container itself would spin on every one of them. The assertion is only that
// each call RETURNS.
func TestResultShape_ContainerMarshalerTerminates(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"self slice":        reflect.TypeOf((*r35cSelfSlice)(nil)).Elem(),
		"self map":          reflect.TypeOf((*r35cSelfMap)(nil)).Elem(),
		"array of self ptr": reflect.TypeOf((*r35cSelfArr)(nil)).Elem(),
		"self slice member": reflect.TypeOf(struct {
			S r35cSelfSlice `json:"s"`
		}{}),
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = ResultShapeStringForTest(typ)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("hung: a container rule must not walk a container's element " +
					"type without a bound of its own")
			}
		})
	}
}

type r35cSelfSlice []r35cSelfSlice
type r35cSelfMap map[string]r35cSelfMap
type r35cSelfArr [1]*r35cSelfArr
