package call

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The fingerprint's whole job is to describe what encoding/json SERIALIZES. Every
// defect found in it so far came from reasoning about that instead of checking:
// embedded structs are promoted, an unexported embedded struct still participates,
// a TAGGED embedded struct is nested rather than promoted, and ",string" puts a
// number on the wire as a string.
//
// So this test does not assert hand-written expectations. It marshals a populated
// value with the real encoding/json and requires the shape's top-level key set to
// equal the JSON object's actual top-level key set.

// topLevelShapeFields splits a "{a:number,b:{...}}" shape into name -> sub-shape,
// ignoring anything nested inside braces or brackets.
func topLevelShapeFields(shape string) map[string]string {
	out := map[string]string{}
	if !strings.HasPrefix(shape, "{") || !strings.HasSuffix(shape, "}") {
		return out
	}
	body := shape[1 : len(shape)-1]
	depth, start := 0, 0
	flush := func(seg string) {
		if i := strings.Index(seg, ":"); i >= 0 {
			out[seg[:i]] = seg[i+1:]
		}
	}
	for i, r := range body {
		switch r {
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case ',':
			if depth == 0 {
				flush(body[start:i])
				start = i + 1
			}
		}
	}
	if start < len(body) {
		flush(body[start:])
	}
	return out
}

// topLevelShapeKeys is the sorted name set of topLevelShapeFields.
func topLevelShapeKeys(shape string) []string {
	f := topLevelShapeFields(shape)
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func jsonTopLevelKeys(t *testing.T, v any) []string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type pInner struct {
	A string `json:"a"`
}

type pTaggedEmbed struct {
	pInner `json:"nested"`
	B      int `json:"b"`
}
type pPtrEmbed struct {
	*pInner
	B int `json:"b"`
}
type pNonStructEmbed struct {
	pStr
	B int `json:"b"`
}
type pStr string
type PExpStr string
type pExpNonStructEmbed struct {
	PExpStr
	B int `json:"b"`
}
type pPlain struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func TestResultShape_JSONParity_MatchesWhatEncodingJSONEmits(t *testing.T) {
	cases := []struct {
		name string
		val  any
	}{
		{"plain", pPlain{"x", 1}},
		{"tagged embedded struct is nested, not promoted", pTaggedEmbed{pInner{"x"}, 1}},
		{"embedded pointer to struct is promoted", pPtrEmbed{&pInner{"x"}, 1}},
		{"unexported embedded non-struct is dropped", pNonStructEmbed{"s", 1}},
		{"exported embedded non-struct is named by its type", pExpNonStructEmbed{"s", 1}},
		{"promotion conflict drops the ambiguous name", ambiguous{}},
		{"shallower field wins a promotion conflict", shadowed{}},
		{"exported embedded struct with a tag is nested", pTaggedExp{pExpInner{"x"}, 1}},
		{"conflict resolved across two levels of embedding", pDeepConflict{}},
		{"omitempty with a populated value still appears", pOmitPopulated{"set", "b"}},
		// The string probe was the only one pinned, so zeroing the numeric and
		// boolean probes — a plausible "simplify the probe" edit — dropped every
		// omitempty numeric/bool member from the shape with the whole suite green,
		// and `{Amount int json:"amount,omitempty"}` collapsed to `{}`.
		{"omitempty on every scalar kind still appears", pOmitScalars{7, 8, 2.5, true, "set"}},
		// A fixed-size array whose element is a POINTER: nothing exercised the
		// array branch at all, so disabling its element population left the suite
		// green while every [N]*T degraded to `[null]` and [2]*A and [2]*B became
		// indistinguishable.
		{"fixed-size array of pointers exposes its element", pArrayHolder{[2]*pExpInner{{"x"}, {"y"}}}},
		// A POINTER-receiver marshaler at a NON-addressable position:
		// encoding/json's condAddrEncoder falls back to the plain encoder there
		// and emits every member, so the shape has to as well. Requiring the
		// pointer-boxed form to marshal made the shape drop `note`, and this
		// harness — whose whole job is exactly this comparison — had no fixture
		// that could see it.
		{"pointer-receiver marshaler at a non-addressable field", pSealedHolder{pSealed{"a", "n"}}},
		// Map fixtures use the key "1" because that is what synthesizeMapKey
		// produces: a map KEY is data, not type structure, so the comparison is
		// only like-for-like when both sides use the same entry.
		{"named map type", pNamedMapHolder{pNamedMap{"1": 1}}},
		{"slice of structs", pStructSlice{[]pExpInner{{"x"}}}},
		{"nested containers", pNested{
			Groups: []pGroup{{Name: "g", Tags: []string{"t"}, Counts: map[string]int{"1": 1}}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shape := ResultShapeStringForTest(reflect.TypeOf(tc.val))
			got := topLevelShapeKeys(shape)
			want := jsonTopLevelKeys(t, tc.val)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("shape keys %v != encoding/json keys %v\n  shape: %s", got, want, shape)
			}
			assertValueKinds(t, tc.val, shape)
		})
	}
}

// ---- full-depth differential comparison -------------------------------------
//
// The previous version of this harness compared the TOP-LEVEL key set and the
// TOP-LEVEL value kinds, and descended only opportunistically: it bailed out
// whenever the top level was not a JSON object, it never reported a member the
// SHAPE records that encoding/json does not emit below the top level, and it
// silently ignored an array whose element shape disagreed past one hop. A review
// identified that shallowness as the direct structural cause of two defects that
// four rounds of review had otherwise missed.
//
// assertShapeParity replaces it. Both sides are turned into the same tree —
// member names and JSON kinds at EVERY depth — and compared node for node in BOTH
// directions:
//
//	a member encoding/json emits but the shape omits  -> a real change there can
//	                                                     never be detected (a miss)
//	a member the shape records but json never emits   -> a change to a field that
//	                                                     is not on the wire FIRES
//	                                                     (the unacceptable direction)
//	a kind mismatch at any depth                      -> both, depending which way
//
// Two traps this repo has already fallen into are baked into the rules:
//   - every fixture is POPULATED. `omitempty` drops a key only when the value is
//     empty and encoding/json cannot promote through a nil embedded pointer, so a
//     zero-valued fixture manufactures divergences that a type-level fingerprint
//     is right to ignore.
//   - KINDS are compared, not just key sets. A TextMarshaler emits a string where
//     its fields say object; a key-only comparison waved that straight through.

// shapeNode is one node of the structure both sides are reduced to.
type shapeNode struct {
	kind    string // object | array | string | number | bool | null | unknown
	members map[string]*shapeNode
	elem    *shapeNode // array element; nil when the side records none
}

// parseShape parses the shape grammar describe() emits:
//
//	object  {name:shape,name:shape}
//	array   [shape] or []
//	scalar  string | number | bool | null | unknown
func parseShape(s string) (*shapeNode, error) {
	p := &shapeParser{s: s}
	n, err := p.value()
	if err != nil {
		return nil, err
	}
	if p.i != len(p.s) {
		return nil, fmt.Errorf("trailing shape text %q", p.s[p.i:])
	}
	return n, nil
}

type shapeParser struct {
	s string
	i int
}

var shapeScalars = []string{"string", "number", "bool", "null", "unknown"}

func (p *shapeParser) value() (*shapeNode, error) {
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("unexpected end of shape at %d", p.i)
	}
	switch p.s[p.i] {
	case '{':
		p.i++
		n := &shapeNode{kind: "object", members: map[string]*shapeNode{}}
		if p.i < len(p.s) && p.s[p.i] == '}' {
			p.i++
			return n, nil
		}
		for {
			j := strings.IndexByte(p.s[p.i:], ':')
			if j < 0 {
				return nil, fmt.Errorf("object member without ':' at %d", p.i)
			}
			key := p.s[p.i : p.i+j]
			p.i += j + 1
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			n.members[key] = v
			if p.i >= len(p.s) {
				return nil, fmt.Errorf("unterminated object at %d", p.i)
			}
			switch p.s[p.i] {
			case ',':
				p.i++
			case '}':
				p.i++
				return n, nil
			default:
				return nil, fmt.Errorf("unexpected %q in object at %d", p.s[p.i], p.i)
			}
		}
	case '[':
		p.i++
		n := &shapeNode{kind: "array"}
		if p.i < len(p.s) && p.s[p.i] == ']' {
			p.i++
			return n, nil
		}
		e, err := p.value()
		if err != nil {
			return nil, err
		}
		n.elem = e
		if p.i >= len(p.s) || p.s[p.i] != ']' {
			return nil, fmt.Errorf("unterminated array at %d", p.i)
		}
		p.i++
		return n, nil
	default:
		for _, lit := range shapeScalars {
			if strings.HasPrefix(p.s[p.i:], lit) {
				p.i += len(lit)
				return &shapeNode{kind: lit}, nil
			}
		}
		return nil, fmt.Errorf("unrecognised shape token at %d: %q", p.i, p.s[p.i:])
	}
}

// jsonNode reduces decoded JSON to the same tree, using describe()'s own rule for
// arrays: the element shape is read off element 0, so a fixture is only ever
// compared like-for-like.
func jsonNode(v any) *shapeNode {
	switch x := v.(type) {
	case map[string]any:
		n := &shapeNode{kind: "object", members: map[string]*shapeNode{}}
		for k, e := range x {
			n.members[k] = jsonNode(e)
		}
		return n
	case []any:
		n := &shapeNode{kind: "array"}
		if len(x) > 0 {
			n.elem = jsonNode(x[0])
		}
		return n
	case string:
		return &shapeNode{kind: "string"}
	case float64:
		return &shapeNode{kind: "number"}
	case bool:
		return &shapeNode{kind: "bool"}
	case nil:
		return &shapeNode{kind: "null"}
	default:
		return &shapeNode{kind: "unknown"}
	}
}

// diffShape walks both trees together. want is what encoding/json ACTUALLY
// emitted, which is the only authority; got is what the fingerprint recorded.
func diffShape(path string, want, got *shapeNode, out *[]string) {
	if got.kind == "unknown" {
		return // records nothing, so it can neither miss nor false fire
	}
	if want.kind != got.kind {
		*out = append(*out, fmt.Sprintf(
			"%s: encoding/json emits a %s but the shape records a %s", path, want.kind, got.kind))
		return
	}
	switch want.kind {
	case "object":
		seen := map[string]bool{}
		names := make([]string, 0, len(want.members)+len(got.members))
		for k := range want.members {
			names = append(names, k)
			seen[k] = true
		}
		for k := range got.members {
			if !seen[k] {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		for _, k := range names {
			w, inJSON := want.members[k]
			g, inShape := got.members[k]
			switch {
			case inJSON && !inShape:
				*out = append(*out, fmt.Sprintf(
					"%s.%s: emitted by encoding/json but ABSENT from the shape (a change to it can never be detected)", path, k))
			case !inJSON && inShape:
				*out = append(*out, fmt.Sprintf(
					"%s.%s: recorded by the shape but NEVER emitted by encoding/json (a change to it FALSE FIRES)", path, k))
			default:
				diffShape(path+"."+k, w, g, out)
			}
		}
	case "array":
		switch {
		case want.elem != nil && got.elem == nil:
			*out = append(*out, fmt.Sprintf(
				"%s: encoding/json emits a non-empty array but the shape records NO element shape", path))
		case want.elem == nil && got.elem != nil:
			*out = append(*out, fmt.Sprintf(
				"%s: the shape records an element shape (%s) for an array encoding/json emits EMPTY", path, got.elem.kind))
		case want.elem != nil && got.elem != nil:
			diffShape(path+"[0]", want.elem, got.elem, out)
		}
	}
}

// assertShapeParity is the whole differential check: marshal the POPULATED value
// with the real encoder and require the recorded shape to agree with it at every
// depth, in both directions.
func assertShapeParity(t *testing.T, v any, shape string) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("fixture does not marshal, so there is nothing to compare against: %v", err)
	}
	if shape == "" {
		t.Errorf("no shape recorded for %T even though encoding/json marshals it fine (%s); "+
			"the guard is silently OFF for this type", v, b)
		return
	}
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("re-decoding %s: %v", b, err)
	}
	got, err := parseShape(shape)
	if err != nil {
		t.Fatalf("shape %q is not parseable: %v", shape, err)
	}
	var diffs []string
	diffShape("$", jsonNode(decoded), got, &diffs)
	if len(diffs) > 0 {
		t.Errorf("shape does not match what encoding/json emits\n  json:  %s\n  shape: %s\n  %s",
			b, shape, strings.Join(diffs, "\n  "))
	}
}

// assertAcceptedMiss is the OTHER outcome the comparator has to be able to
// express, and it exists so an empty shape can never pass silently.
//
// A type whose fabricated probe encoding/json REJECTS records no shape at all and
// is skipped by replay — the deliberate policy, see resultShape. Parity then has
// nothing to compare: the shape is not wrong, it is absent. But "absent" must not
// become a wildcard that any bug can hide behind, so this asserts the SPECIFIC
// reason: the populated probe really is refused by the real encoder. A type that
// records no shape for any other cause still fails here.
func assertAcceptedMiss(t *testing.T, typ reflect.Type, shape string) {
	t.Helper()
	if shape != "" {
		t.Errorf("%s was expected to be an accepted miss (no shape, guard skipped) but "+
			"recorded %q; a stand-in shape for an un-probeable type is how the last two "+
			"revisions false-fired", typ, shape)
		return
	}
	probe, ok := synthesize(typ, 0, nil)
	if !ok {
		t.Fatalf("%s could not even be synthesized, so the empty shape has a different cause", typ)
	}
	if _, err := json.Marshal(probe.Interface()); err == nil {
		t.Errorf("%s records no shape, but its populated probe marshals cleanly — so the "+
			"accepted-miss policy does not explain the empty shape and something else is "+
			"switching the guard off", typ)
	}
}

// assertValueKinds is kept as the name the older assertions call, now backed by
// the full-depth comparison above.
func assertValueKinds(t *testing.T, v any, shape string) {
	t.Helper()
	assertShapeParity(t, v, shape)
}

// ",string" puts a number on the wire as a JSON string, so it must fingerprint as
// a string -- identical to a plain string field, and different from a plain number.
type pStringOpt struct {
	N int `json:",string"`
}
type pStringField struct {
	N string `json:"N"`
}
type pNumberField struct {
	N int `json:"N"`
}

func TestResultShape_StringOptionFollowsTheWireNotTheGoKind(t *testing.T) {
	opt := ResultFingerprintForTest(reflect.TypeOf(pStringOpt{}))
	if str := ResultFingerprintForTest(reflect.TypeOf(pStringField{})); opt != str {
		t.Errorf(`int with json:",string" is a JSON string on the wire, so it must match a string field: %s != %s`, opt, str)
	}
	if num := ResultFingerprintForTest(reflect.TypeOf(pNumberField{})); opt == num {
		t.Errorf(`int with json:",string" must NOT match a bare number field: both %s`, num)
	}
}

// ---- tagged-vs-untagged ties ------------------------------------------------
//
// encoding/json's dominantField breaks a same-depth name collision in favour of
// the TAGGED field and EMITS it; it drops the name only when the tied fields have
// the same taggedness. Treating every same-depth tie as ambiguous made a dead,
// never-serialized field load-bearing to the fingerprint.

type pTieDirect struct {
	Name  string // never serialized: the tagged field below wins
	Alias string `json:"Name"`
}
type pTieWinnerOnly struct {
	Alias string `json:"Name"`
}
type pEmbUntagged struct{ Name string }
type pEmbTagged struct {
	Alias string `json:"Name"`
}
type pTieViaEmbeds struct {
	pEmbUntagged
	pEmbTagged
}

func TestResultShape_TaggedFieldWinsATieAndIsEmitted(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
	}{
		{"tagged beats untagged at the same depth", pTieDirect{"dead", "v"}},
		{"same, one level down through embeds", pTieViaEmbeds{pEmbUntagged{"dead"}, pEmbTagged{"v"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shape := ResultShapeStringForTest(reflect.TypeOf(tc.val))
			got, want := topLevelShapeKeys(shape), jsonTopLevelKeys(t, tc.val)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("shape keys %v != encoding/json keys %v\n  shape: %s", got, want, shape)
			}
		})
	}
}

// Deleting the dead untagged field is byte-identical on the wire, so it must not
// change the fingerprint.
func TestResultShape_DroppingTheLoserOfATieIsNotAChange(t *testing.T) {
	a := ResultFingerprintForTest(reflect.TypeOf(pTieDirect{}))
	b := ResultFingerprintForTest(reflect.TypeOf(pTieWinnerOnly{}))
	if a != b {
		t.Fatalf("removing a never-serialized field changed the fingerprint (%s -> %s)", a, b)
	}
}

// ---- json.Number ------------------------------------------------------------

type pNum struct {
	Amount json.Number `json:"amount"`
}
type pFloat struct {
	Amount float64 `json:"amount"`
}
type pStrAmount struct {
	Amount string `json:"amount"`
}

// json.Number is declared as a string but serializes as raw digits, so swapping it
// for float64 to stop losing precision is byte-identical and must not fire.
func TestResultShape_JSONNumberIsANumber(t *testing.T) {
	if n, f := ResultFingerprintForTest(reflect.TypeOf(pNum{})), ResultFingerprintForTest(reflect.TypeOf(pFloat{})); n != f {
		t.Errorf("json.Number and float64 serialize identically but fingerprint %s != %s", n, f)
	}
	if n, s := ResultFingerprintForTest(reflect.TypeOf(pNum{})), ResultFingerprintForTest(reflect.TypeOf(pStrAmount{})); n == s {
		t.Errorf("json.Number must not match a real string field: both %s", n)
	}
}

// ---- custom MarshalJSON -----------------------------------------------------

type pMoneyV1 struct{ Cents int }

func (m pMoneyV1) MarshalJSON() ([]byte, error) { return []byte(`"USD:500"`), nil }

type pMoneyV2 struct {
	Amount   int
	Currency string
}

func (m pMoneyV2) MarshalJSON() ([]byte, error) { return []byte(`"USD:500"`), nil }

// A type with its own MarshalJSON emits something unrelated to its fields, so the
// fields cannot describe its wire form. Reading them anyway made a byte-identical
// refactor of such a type FALSE FIRE — the one outcome this check must never
// produce. All json.Marshalers share one opaque shape instead: changes among them
// go undetected, the documented and far cheaper failure direction.
func TestResultShape_CustomMarshalerDoesNotFalseFire(t *testing.T) {
	b1, err := json.Marshal(pMoneyV1{500})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := json.Marshal(pMoneyV2{500, "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("fixture broken: wire forms differ (%s vs %s)", b1, b2)
	}
	if a, b := ResultFingerprintForTest(reflect.TypeOf(pMoneyV1{})), ResultFingerprintForTest(reflect.TypeOf(pMoneyV2{})); a != b {
		t.Fatalf("two Marshalers with identical wire output fingerprint differently (%s vs %s); "+
			"a byte-identical refactor would wedge in-flight replays", a, b)
	}
}

// ---- encoding.TextMarshaler -------------------------------------------------
//
// json's second fallback after json.Marshaler. It always emits a JSON STRING
// regardless of the type's fields, so two TextMarshalers with entirely different
// fields serialize identically and must fingerprint identically.

type pTextV1 struct{ X int }

func (pTextV1) MarshalText() ([]byte, error) { return []byte("hi"), nil }

type pTextV2 struct {
	Y string
	Z bool
}

func (pTextV2) MarshalText() ([]byte, error) { return []byte("hi"), nil }

type pTextHolderV1 struct {
	T pTextV1 `json:"t"`
}
type pTextHolderV2 struct {
	T pTextV2 `json:"t"`
}

func TestResultShape_TextMarshalerIsAString(t *testing.T) {
	b1, _ := json.Marshal(pTextHolderV1{})
	b2, _ := json.Marshal(pTextHolderV2{})
	if string(b1) != string(b2) {
		t.Fatalf("fixture broken: %s vs %s", b1, b2)
	}
	a := ResultFingerprintForTest(reflect.TypeOf(pTextHolderV1{}))
	c := ResultFingerprintForTest(reflect.TypeOf(pTextHolderV2{}))
	if a != c {
		t.Fatalf("two TextMarshalers emitting identical JSON (%s) fingerprint differently (%s vs %s); "+
			"a refactor that changes nothing on the wire would wedge in-flight replays", b1, a, c)
	}
	// And the shape must predict a string, not an object.
	assertValueKinds(t, pTextHolderV1{}, ResultShapeStringForTest(reflect.TypeOf(pTextHolderV1{})))
}

// ---- invalid tag names ------------------------------------------------------

// isValidTag: json ignores a tag name containing illegal characters and falls back
// to the Go field name.
type pBadTag struct {
	//nolint:staticcheck // SA5008: the illegal tag name IS the fixture — json
	// ignores it and falls back to the field name, and the shape must too.
	V string `json:"a\"b"`
}

func TestResultShape_JSONParity_InvalidTagNameFallsBackToTheFieldName(t *testing.T) {
	val := pBadTag{"x"}
	shape := ResultShapeStringForTest(reflect.TypeOf(val))
	got, want := topLevelShapeKeys(shape), jsonTopLevelKeys(t, val)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shape keys %v != encoding/json keys %v (json ignores an illegal tag name)\n  shape: %s",
			got, want, shape)
	}
}

// Additional fixtures from the differential sweep, kept so the behaviour they
// pin cannot regress.

type pExpInner struct {
	A string `json:"a"`
}
type pTaggedExp struct {
	pExpInner `json:"nested"`
	B         int `json:"b"`
}

type pLvl2 struct {
	ID string `json:"id"`
}
type pLvl1 struct{ pLvl2 }

// nolint:govet // the repeated "id" is the fixture: depth 0 must beat depth 2.
type pDeepConflict struct {
	pLvl1
	ID int `json:"id"`
}

type pOmitPopulated struct {
	A string `json:"a,omitempty"`
	B string `json:"b"`
}

// One omitempty member per scalar probe in synthesize, so a probe that stops
// being non-empty drops the member from the shape while this fixture's populated
// value still emits it.
type pOmitScalars struct {
	N   int     `json:"n,omitempty"`
	U   uint    `json:"u,omitempty"`
	F   float64 `json:"f,omitempty"`
	B   bool    `json:"b,omitempty"`
	Set string  `json:"set"`
}

type pArrayHolder struct {
	Slots [2]*pExpInner `json:"slots"`
}

type pSealed struct {
	ID   string `json:"id"`
	Note string `json:"note,omitempty"`
}

// Rejects anything populated, so a probe that reaches it through a pointer fails
// and the member degrades — which is only correct where encoding/json would have
// reached it too.
func (s *pSealed) MarshalJSON() ([]byte, error) {
	if s.ID != "" {
		return nil, errors.New("sealed")
	}
	return []byte(`{"id":""}`), nil
}

type pSealedHolder struct {
	X pSealed `json:"x"`
}

type pNamedMap map[string]int
type pNamedMapHolder struct {
	M pNamedMap `json:"m"`
}
type pStructSlice struct {
	S []pExpInner `json:"s"`
}

// A fixture with real nesting, so the parity walk exercises element and value
// shapes rather than only top-level members.
type pGroup struct {
	Name   string         `json:"name"`
	Tags   []string       `json:"tags"`
	Counts map[string]int `json:"counts"`
}
type pNested struct {
	Groups []pGroup `json:"groups"`
}

// ---- fixtures the old table could not express -------------------------------
//
// Every one of these puts the interesting construct at DEPTH, not at the top
// level, because the top level is the one position the previous harness checked.

// A VALIDATING marshaler. net.IP's MarshalText rejects any length other than 0, 4
// or 16, so the populated probe synthesize builds (a one-byte slice) is refused —
// which sends resultShape down its zero-value fallback for the WHOLE type, not
// just for the offending member.
type pIPField struct {
	IP   net.IP `json:"ip"`
	Name string `json:"name,omitempty"`
}
type pIPSlice struct {
	Addrs []net.IP `json:"addrs"`
}
type pIPAtDepth struct {
	Endpoints []pIPField `json:"endpoints"`
	Label     string     `json:"label"`
}

// The same member with and without `omitempty`, at depth. Populated, the two are
// byte-identical on the wire, so they must agree — and each must agree with the
// real encoder.
type pOmitInner struct {
	A string `json:"a,omitempty"`
	B string `json:"b"`
}
type pNoOmitInner struct {
	A string `json:"a"`
	B string `json:"b"`
}
type pOmitAtDepth struct {
	Rows []pOmitInner `json:"rows"`
}
type pNoOmitAtDepth struct {
	Rows []pNoOmitInner `json:"rows"`
}

// A POINTER-RECEIVER MarshalJSON. encoding/json may only use it where the value is
// addressable, which is a property of the POSITION, not the type: a field of an
// interface-boxed struct is not addressable, a slice element is (the backing array
// is), a map value is not, and a pointer always is. Recording the pointer-boxed
// form everywhere false-fired in revision 2.
type pPtrMarshal struct {
	N int `json:"n"`
}

func (p *pPtrMarshal) MarshalJSON() ([]byte, error) { return []byte(`"pm"`), nil }

type pPtrMarshalPositions struct {
	Field pPtrMarshal            `json:"field"` // NOT addressable
	Ptr   *pPtrMarshal           `json:"ptr"`   // addressable
	Slice []pPtrMarshal          `json:"slice"` // addressable (backing array)
	Map   map[string]pPtrMarshal `json:"map"`   // NOT addressable
	Array [1]pPtrMarshal         `json:"array"` // NOT addressable here
}

// Embeds at depth: a TAGGED embed nests, an untagged one promotes, and both have
// only ever been checked at the top level.
type pEmbedsAtDepth struct {
	Tagged   []pTaggedExp `json:"tagged"`
	Untagged []pPtrEmbed  `json:"untagged"`
	Deep     pTaggedExp   `json:"deep"`
	DeepPtr  *pPtrEmbed   `json:"deepPtr"`
}

func TestResultShape_JSONParity_AtDepth(t *testing.T) {
	ip := net.IPv4(203, 0, 113, 9)
	cases := []struct {
		name string
		val  any
		// acceptedMiss: encoding/json marshals the REAL value fine, but rejects the
		// fabricated probe, so the type records no shape and the guard skips it
		// entirely. There is no shape to compare; assertAcceptedMiss pins that
		// exact reason instead. See resultShape for why every attempt to supply a
		// stand-in shape here false-fired.
		acceptedMiss bool
	}{
		{"validating marshaler in a struct field", pIPField{ip, "n"}, true},
		{"validating marshaler inside a slice element", pIPSlice{[]net.IP{ip}}, true},
		{"validating marshaler two levels down", pIPAtDepth{[]pIPField{{ip, "n"}}, "l"}, true},
		{"omitempty member at depth, populated", pOmitAtDepth{[]pOmitInner{{"a", "b"}}}, false},
		{"the same member without omitempty at depth", pNoOmitAtDepth{[]pNoOmitInner{{"a", "b"}}}, false},
		{"pointer-receiver marshaler at every addressability", pPtrMarshalPositions{
			Field: pPtrMarshal{1},
			Ptr:   &pPtrMarshal{1},
			Slice: []pPtrMarshal{{1}},
			// The key is "1" because that is what synthesizeMapKey produces; a map
			// KEY is data, not structure, so only a like-for-like key compares.
			Map:   map[string]pPtrMarshal{"1": {1}},
			Array: [1]pPtrMarshal{{1}},
		}, false},
		{"tagged and untagged embeds at depth", pEmbedsAtDepth{
			Tagged:   []pTaggedExp{{pExpInner{"x"}, 1}},
			Untagged: []pPtrEmbed{{&pInner{"x"}, 1}},
			Deep:     pTaggedExp{pExpInner{"x"}, 1},
			DeepPtr:  &pPtrEmbed{&pInner{"x"}, 1},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeOf(tc.val)
			shape := ResultShapeStringForTest(typ)
			if tc.acceptedMiss {
				assertAcceptedMiss(t, typ, shape)
				return
			}
			assertShapeParity(t, tc.val, shape)
		})
	}
}

// Populated, `omitempty` changes nothing on the wire, so adding or removing it
// must not move the fingerprint at depth any more than it does at the top level.
func TestResultShape_JSONParity_OmitemptyAtDepthIsNotAWireChange(t *testing.T) {
	a, _ := json.Marshal(pOmitAtDepth{[]pOmitInner{{"a", "b"}}})
	b, _ := json.Marshal(pNoOmitAtDepth{[]pNoOmitInner{{"a", "b"}}})
	if string(a) != string(b) {
		t.Fatalf("fixture broken: %s vs %s", a, b)
	}
	x := ResultFingerprintForTest(reflect.TypeOf(pOmitAtDepth{}))
	y := ResultFingerprintForTest(reflect.TypeOf(pNoOmitAtDepth{}))
	if x != y {
		t.Errorf("adding omitempty to a populated member at depth is byte-identical (%s) "+
			"but changed the fingerprint (%s -> %s)", a, x, y)
	}
}

// A validating marshaler used to degrade more than ITSELF. resultShape marshals
// one probe for the WHOLE type, and when net.IP rejected it a fallback re-marshalled
// a substituted value — first the zero of the whole type, then the zero of just the
// offending member. In that degraded mode the shape was read off zero values, which
// is exactly what the populated probe exists to avoid, and the consequence was not
// a miss: `T -> *T` is byte-identical on the wire when the value is set, yet putting
// a net.IP anywhere in the same type split the two apart and false-fired.
//
// There is no fallback now. A rejected probe records NO shape for the whole type
// and replay skips it, so this test pins the ABSENCE of the split: both forms
// fingerprint to the same thing, because both fingerprint to nothing.
type pDegradeSub struct {
	A string `json:"a"`
}
type pDegradeVal struct {
	IP  net.IP      `json:"ip"`
	Sub pDegradeSub `json:"sub"`
}
type pDegradePtr struct {
	IP  net.IP       `json:"ip"`
	Sub *pDegradeSub `json:"sub"`
}
type pControlVal struct {
	Sub pDegradeSub `json:"sub"`
}
type pControlPtr struct {
	Sub *pDegradeSub `json:"sub"`
}

func TestResultShape_JSONParity_ValidatingMarshalerMustNotDegradeTheWholeType(t *testing.T) {
	ip := net.IPv4(203, 0, 113, 9)
	a, _ := json.Marshal(pDegradeVal{ip, pDegradeSub{"x"}})
	b, _ := json.Marshal(pDegradePtr{ip, &pDegradeSub{"x"}})
	if string(a) != string(b) {
		t.Fatalf("fixture broken: %s vs %s", a, b)
	}
	// Control: without the validating member, T -> *T does not move the fingerprint.
	if x, y := ResultFingerprintForTest(reflect.TypeOf(pControlVal{})),
		ResultFingerprintForTest(reflect.TypeOf(pControlPtr{})); x != y {
		t.Fatalf("control broken: T -> *T already fires without a validating marshaler (%s vs %s)", x, y)
	}
	x := ResultFingerprintForTest(reflect.TypeOf(pDegradeVal{}))
	y := ResultFingerprintForTest(reflect.TypeOf(pDegradePtr{}))
	if x != y {
		t.Errorf("T -> *T is byte-identical on the wire (%s) and does not fire on its own, "+
			"but with a validating marshaler in the same type it FALSE FIRES (%s vs %s)\n"+
			"  shape T:  %s\n  shape *T: %s", a, x, y,
			ResultShapeStringForTest(reflect.TypeOf(pDegradeVal{})),
			ResultShapeStringForTest(reflect.TypeOf(pDegradePtr{})))
	}
}

// The depth cap used to have the same asymmetry, for the same reason: the cap
// test ran BEFORE the Pointer case, so at the boundary a *T was left nil (null)
// while a T was left zero (an object), and a chain long enough to reach the cap
// fired on a refactor that cannot change a byte. Nothing is substituted at the
// boundary now — reaching it records no shape for the whole type — so an
// eight-level chain like this one sits comfortably inside a cap of 32, is fully
// guarded, and must agree. This is the deep-but-armed region; the boundary itself
// is covered by TestResultShape_PastTheDepthCapIsFailOpenInProduction.
type pCapLeaf struct {
	V string `json:"v"`
}
type pCapV7 struct {
	N pCapLeaf `json:"n"`
}
type pCapV6 struct {
	N pCapV7 `json:"n"`
}
type pCapV5 struct {
	N pCapV6 `json:"n"`
}
type pCapV4 struct {
	N pCapV5 `json:"n"`
}
type pCapV3 struct {
	N pCapV4 `json:"n"`
}
type pCapV2 struct {
	N pCapV3 `json:"n"`
}
type pCapV1 struct {
	N pCapV2 `json:"n"`
}

type pCapP7 struct {
	N *pCapLeaf `json:"n"`
}
type pCapP6 struct {
	N pCapP7 `json:"n"`
}
type pCapP5 struct {
	N pCapP6 `json:"n"`
}
type pCapP4 struct {
	N pCapP5 `json:"n"`
}
type pCapP3 struct {
	N pCapP4 `json:"n"`
}
type pCapP2 struct {
	N pCapP3 `json:"n"`
}
type pCapP1 struct {
	N pCapP2 `json:"n"`
}

func TestResultShape_JSONParity_DepthCapMustNotSplitTFromPointerToT(t *testing.T) {
	val := pCapV1{pCapV2{pCapV3{pCapV4{pCapV5{pCapV6{pCapV7{pCapLeaf{"x"}}}}}}}}
	ptr := pCapP1{pCapP2{pCapP3{pCapP4{pCapP5{pCapP6{pCapP7{&pCapLeaf{"x"}}}}}}}}
	a, _ := json.Marshal(val)
	b, _ := json.Marshal(ptr)
	if string(a) != string(b) {
		t.Fatalf("fixture broken: %s vs %s", a, b)
	}
	// Both sides are also checked against the real encoder, so it is visible WHERE
	// the two shapes stop agreeing with it.
	assertShapeParity(t, val, ResultShapeStringForTest(reflect.TypeOf(val)))
	assertShapeParity(t, ptr, ResultShapeStringForTest(reflect.TypeOf(ptr)))
	x := ResultFingerprintForTest(reflect.TypeOf(pCapV1{}))
	y := ResultFingerprintForTest(reflect.TypeOf(pCapP1{}))
	if x != y {
		t.Errorf("at the depth cap, T -> *T is byte-identical on the wire (%s) but FALSE FIRES (%s vs %s)\n"+
			"  shape T:  %s\n  shape *T: %s", a, x, y,
			ResultShapeStringForTest(reflect.TypeOf(pCapV1{})),
			ResultShapeStringForTest(reflect.TypeOf(pCapP1{})))
	}
}

// A harness that always passes is worse than no harness, and this one is now big
// enough to break silently. These cases drive the comparator directly with a
// hand-built shape/JSON pair and require the divergence to be REPORTED, so gutting
// diffShape reds here instead of quietly greening the whole file.
func TestResultShape_JSONParity_ComparatorReportsNestedDivergence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		json  string
		shape string
		want  string
	}{
		{"member missing from the shape at depth 3", `{"a":{"b":{"c":1,"d":2}}}`, `{a:{b:{c:number}}}`,
			"$.a.b.d: emitted by encoding/json but ABSENT from the shape"},
		{"member the shape invents at depth 3", `{"a":{"b":{"c":1}}}`, `{a:{b:{c:number,d:number}}}`,
			"$.a.b.d: recorded by the shape but NEVER emitted by encoding/json"},
		{"kind mismatch at depth 3", `{"a":{"b":{"c":"s"}}}`, `{a:{b:{c:number}}}`,
			"$.a.b.c: encoding/json emits a string but the shape records a number"},
		{"kind mismatch inside an array element", `{"a":[{"b":"s"}]}`, `{a:[{b:number}]}`,
			"$.a[0].b: encoding/json emits a string but the shape records a number"},
		{"array element shape missing", `{"a":[1]}`, `{a:[]}`,
			"$.a: encoding/json emits a non-empty array but the shape records NO element shape"},
		{"a TextMarshaler string where the shape says object", `{"t":"hi"}`, `{t:{x:number}}`,
			"$.t: encoding/json emits a string but the shape records a object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var decoded any
			if err := json.Unmarshal([]byte(tc.json), &decoded); err != nil {
				t.Fatal(err)
			}
			got, err := parseShape(tc.shape)
			if err != nil {
				t.Fatalf("parseShape(%q): %v", tc.shape, err)
			}
			var diffs []string
			diffShape("$", jsonNode(decoded), got, &diffs)
			joined := strings.Join(diffs, "\n")
			if !strings.Contains(joined, tc.want) {
				t.Fatalf("comparator did not report the divergence\n  want substring: %s\n  got: %s", tc.want, joined)
			}
		})
	}
	// And an agreeing pair must produce nothing, or every fixture above would be
	// failing for the wrong reason.
	var decoded any
	if err := json.Unmarshal([]byte(`{"a":[{"b":1,"c":null}],"d":true}`), &decoded); err != nil {
		t.Fatal(err)
	}
	shape, err := parseShape(`{a:[{b:number,c:null}],d:bool}`)
	if err != nil {
		t.Fatal(err)
	}
	var diffs []string
	diffShape("$", jsonNode(decoded), shape, &diffs)
	if len(diffs) > 0 {
		t.Fatalf("agreeing pair reported %v", diffs)
	}
}
