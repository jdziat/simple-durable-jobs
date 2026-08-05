package call

import (
	"encoding/json"
	"reflect"
	"testing"
)

// ---------------------------------------------------------------------------
// THE TWO PRODUCTION MECHANISMS NO SHIPPED TEST EXERCISED
//
// Both were found by mutation: a single line switched off in each, and the whole
// suite stayed green. Neither could cause a false fire — both fail open, which
// call.go skips — but an untested mechanism is one a future maintainer deletes as
// apparently-dead code with a green CI, and this repo has a documented history of
// comments claiming a pin that does not exist.

// ---- maxShapeNodes ----------------------------------------------------------
//
// The per-probe node budget. maxShapeDepth bounds how DEEP the walk goes and says
// nothing about how WIDE: a struct with k struct-typed members nested d levels
// costs k^d value constructions, and raising the depth cap from 6 to 32 raised
// that ceiling with it. The budget is what stops it.
//
// Two independent single-line mutations switched it off entirely — `if *budget <=
// 0` to `if false`, and dropping the `*budget--` — and every shipped test stayed
// green. The implementer's stated check (lowering the constant to 20 reds five
// test groups) only proves the budget does not OVER-trip. Nothing proved it ever
// trips at all.
//
// r29Wide builds the shape that separates the two bounds: branching factor b,
// nested d JSON levels. Node count is ~b^d while JSON depth is only d, so b and d
// can be chosen to blow the node budget while sitting far inside the depth cap.

func r29Wide(branch, depth int) reflect.Type {
	t := reflect.StructOf([]reflect.StructField{
		{Name: "L", Type: reflect.TypeOf(""), Tag: `json:"l"`},
	})
	for i := 0; i < depth; i++ {
		fields := make([]reflect.StructField, 0, branch)
		for j := 0; j < branch; j++ {
			fields = append(fields, reflect.StructField{
				Name: "F" + string(rune('A'+j)),
				Type: t,
				Tag:  reflect.StructTag(`json:"f` + string(rune('a'+j)) + `"`),
			})
		}
		t = reflect.StructOf(fields)
	}
	return t
}

func TestResultShape_NodeBudgetTripsAndFailsOpen(t *testing.T) {
	// THE CONTROLS COME FIRST, because they are what make the assertion below
	// mean "the BUDGET tripped" rather than "something tripped".
	//
	// Same JSON depth, branching 2: ~2^7 nodes, comfortably inside the budget. If
	// this records no shape, the fixture is being stopped by the DEPTH cap and the
	// budget assertion proves nothing.
	narrow := r29Wide(2, 7)
	if s := ResultShapeStringForTest(narrow); s == "" {
		t.Fatalf("a 7-level type is far inside maxShapeDepth=%d and must record a shape; "+
			"if it does not, the assertion below is observing the depth cap", maxShapeDepth)
	}
	// Same branching factor, shallower: ~8^5 = 32768 nodes, still inside the
	// budget. This isolates NODE COUNT as the only variable that changes.
	shallow := r29Wide(8, 5)
	if s := ResultShapeStringForTest(shallow); s == "" {
		t.Fatalf("8^5 nodes is inside maxShapeNodes=%d and must record a shape", maxShapeNodes)
	}

	// THE ASSERTION. ~8^7 = 2.1M node constructions at JSON depth 7. The depth cap
	// cannot stop this and the cycle set cannot either — every type in it is
	// distinct and acyclic. Only the node budget can.
	big := r29Wide(8, 7)
	if s := ResultShapeStringForTest(big); s != "" {
		t.Fatalf("a type costing ~8^7 node constructions must exhaust maxShapeNodes=%d and "+
			"record NO shape, got %d chars of shape.\n"+
			"The budget is not being charged, so one probe can build an unbounded value; the "+
			"depth cap does not bound width and the two controls above prove neither depth nor "+
			"cycles stopped this fixture.", maxShapeNodes, len(s))
	}
	// Exhaustion FAILS OPEN, like every other boundary here: no shape means no
	// fingerprint, and call.go skips the comparison whenever either side is empty.
	// So a budget trip can only downgrade a guarded type to an unguarded one.
	if fp := ResultFingerprintForTest(big); fp != "" {
		t.Fatalf("an exhausted probe must fingerprint empty so replay skips it, got %q", fp)
	}

	// AND IT IS PER-PROBE, not a package-level counter. If the budget were shared,
	// running the exhausting probe first would poison every later one — including
	// on other goroutines — and the guard would silently switch itself off for the
	// rest of the process. The controls are re-read AFTER the exhausting probe,
	// with the memo bypassed by re-deriving the types, so a shared counter shows up
	// here as an empty shape.
	if s := ResultShapeStringForTest(r29Wide(2, 7)); s == "" {
		t.Fatal("a small type records no shape AFTER a big probe exhausted its budget; the " +
			"budget is shared between probes instead of being per-probe, so one wide result " +
			"type disables the guard for every other type in the process")
	}
	if s := ResultShapeStringForTest(r29Wide(8, 5)); s == "" {
		t.Fatal("the second control also went empty after the exhausting probe; the node budget " +
			"is not per-probe")
	}
}

// ---- promotesIntoParent's anonymity guard -----------------------------------
//
// promotesIntoParent's doc comment claims its agreement with the encoder "is
// pinned two ways", naming TestResultShape_DepthBudgetIsChargedAtEveryNestingSite
// as putting "each of the four answers this function can give — anonymous,
// tagged, embedded pointer, embedded non-struct — on the truncation boundary".
//
// Three of the four were pinned. The ANONYMITY guard was not: deleting
// `if !sf.Anonymous { return false }` survived the entire suite. The row meant to
// cover it declares `type r28SiteUntagged struct{ L string }`, whose member is a
// STRING — so the function's final `ft.Kind() == reflect.Struct` check answers
// "nests" with or without the anonymity guard, and the row cannot tell them apart.
// pwNestN, which builds every depth placement in the pairwise sweep, always emits
// a TAGGED field, so the `name != ""` guard masks it there too.
//
// The untested combination is the most ordinary field in Go: untagged,
// NOT anonymous, and struct-typed. Without the guard, encoding/json nests it under
// its Go name while this function would say it promotes, the walk runs one level
// past the budget for every such hop, and a chain of them compounds — churning the
// fingerprint of every deep result type carrying one.
//
// A row for it is added to that table, so the comment's claim is now true where it
// is made. The test below is the second half: it reads the expectation off the
// REAL encoder first, and asserts BOTH answers — nested member and promoting embed
// — in one place, so the pair distinguishes them rather than each standing alone.

// r29SiteUntaggedStruct's member is untagged, not anonymous, and STRUCT-typed —
// the one combination that distinguishes "promotes because anonymous" from
// "promotes because it is a struct with no json name". json nests it under "Inner",
// so it charges one level and its leaf charges a second: d=2.
type r29SiteUntaggedStruct struct {
	Inner r29Leaf
}

type r29Leaf struct {
	L string `json:"l"`
}

func TestResultShape_UntaggedNonAnonymousStructMemberNests(t *testing.T) {
	// The encoder's own answer first, so the expectation below is read off
	// encoding/json rather than asserted from memory.
	b, err := json.Marshal(r29SiteUntaggedStruct{Inner: r29Leaf{L: "x"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"Inner":{"l":"x"}}` {
		t.Fatalf("encoding/json nests an untagged non-anonymous struct member under its Go "+
			"name; got %s, so the depth expectation below is derived from the wrong premise", b)
	}

	// d=2, so the probe first records no shape at maxShapeDepth-2+1 wrapping
	// levels — exactly one level SHALLOWER than a site that charges nothing.
	// Without the anonymity guard, promotesIntoParent would call this a promotion,
	// charge zero, and the boundary would move one level deeper.
	const d = 2
	wantK := maxShapeDepth - d + 1
	gotK := r28FirstEmptyK(reflect.TypeOf(r29SiteUntaggedStruct{}))
	if gotK != wantK {
		t.Fatalf("the probe first records NO shape at %d wrapping levels, want %d.\n"+
			"An untagged, NON-ANONYMOUS, struct-typed member is NESTED by encoding/json "+
			"(see the marshal above), so it must charge one level of depth budget. Charging "+
			"zero means promotesIntoParent stopped checking sf.Anonymous: the walk then runs a "+
			"level past the budget for every such hop, and every deep result type's fingerprint "+
			"churns.", gotK, wantK)
	}
	// One level inside the boundary the shape must still be REAL, or the check
	// above is comparing two empties.
	inside := ResultShapeStringForTest(pwNestN(reflect.TypeOf(r29SiteUntaggedStruct{}), wantK-1))
	if inside == "" {
		t.Fatal("at one level inside the boundary the type must still record a shape")
	}

	// AND THE COMMENT'S OTHER HALF: the anonymous answer itself. An untagged
	// EMBEDDED struct promotes, so it charges nothing and its boundary sits one
	// level deeper than the nested member above. Asserting both in one test is
	// what makes the pair distinguish the two answers rather than one of them.
	embedded := reflect.StructOf([]reflect.StructField{
		{Name: "R29Leaf", Type: reflect.TypeOf(r29Leaf{}), Anonymous: true},
	})
	if got, want := r28FirstEmptyK(embedded), maxShapeDepth-1+1; got != want {
		t.Fatalf("an untagged EMBED promotes and must charge nothing: boundary at %d wrapping "+
			"levels, want %d", got, want)
	}
}

// ---- map keys this file cannot render ---------------------------------------
//
// The last sibling of the substitution family, in synthesizeMapKey's default
// branch. It substituted reflect.Zero for any key kind the switch does not name,
// and the rendered key NAME then came from the value's encoding.TextMarshaler —
// so the Go representation decided the shape again:
//
//	map[K]int   with a value-receiver MarshalText -> {"k":1}, shape {k:number}
//	map[*K]int  with the same marshaler           -> {"k":1}, shape {:number}
//
// Byte-identical wire, two different shapes, replay refused. It now records no
// shape, like every other boundary in the file.

type r29TextKey struct{ N int }

func (k r29TextKey) MarshalText() ([]byte, error) { return []byte("k"), nil }
func (k *r29TextKey) UnmarshalText([]byte) error  { return nil }

func TestResultShape_UnrenderableMapKeyRecordsNoShape(t *testing.T) {
	valueKeyed := map[r29TextKey]int{{N: 1}: 1}
	ptrKeyed := map[*r29TextKey]int{{N: 1}: 1}

	// STEP 1, the premise: the two really are wire-identical.
	ba, err := json.Marshal(valueKeyed)
	if err != nil {
		t.Fatalf("marshal map[K]int: %v", err)
	}
	bb, err := json.Marshal(ptrKeyed)
	if err != nil {
		t.Fatalf("marshal map[*K]int: %v", err)
	}
	if string(ba) != string(bb) {
		t.Fatalf("the pair is not wire-identical (%s vs %s), so it proves nothing", ba, bb)
	}

	// STEP 2: both record NO shape. Equal-but-non-empty would mean a value is
	// being substituted for the key again.
	for name, typ := range map[string]reflect.Type{
		"map[K]int":  reflect.TypeOf(valueKeyed),
		"map[*K]int": reflect.TypeOf(ptrKeyed),
	} {
		if s := ResultShapeStringForTest(typ); s != "" {
			t.Errorf("%s: a map key this file cannot render must record NO shape, got %q.\n"+
				"The key name then comes from the value's TextMarshaler, and %s is the same wire "+
				"as the other form — so a byte-identical change between them was refused on replay.",
				name, s, ba)
		}
		if fp := ResultFingerprintForTest(typ); fp != "" {
			t.Errorf("%s: no shape must fingerprint empty, got %q", name, fp)
		}
	}

	// THE CONTROL, so this is not green because maps stopped working: the key
	// kinds synthesizeMapKey does render still record a real, identical shape.
	for _, typ := range []reflect.Type{
		reflect.TypeOf(map[string]int(nil)),
		reflect.TypeOf(map[int]int(nil)),
		reflect.TypeOf(map[uint8]int(nil)),
	} {
		if s := ResultShapeStringForTest(typ); s != "{1:number}" {
			t.Errorf("%s must still record {1:number}, got %q", typ, s)
		}
	}
}
