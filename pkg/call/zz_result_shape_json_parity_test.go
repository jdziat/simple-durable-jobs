package call

import (
	"encoding/json"
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

// jsonKindOf reports the JSON kind of a raw value, which is what a shape entry has
// to agree with.
func jsonKindOf(raw json.RawMessage) string {
	t := strings.TrimSpace(string(raw))
	switch {
	case t == "":
		return "empty"
	case t == "null":
		return "null"
	case t == "true" || t == "false":
		return "bool"
	case t[0] == '"':
		return "string"
	case t[0] == '{':
		return "object"
	case t[0] == '[':
		return "array"
	default:
		return "number"
	}
}

// shapeKindOf maps a shape entry to the JSON kind it predicts. "any", "marshaler"
// and the depth-truncation marker predict nothing and are reported as such.
func shapeKindOf(shape string) string {
	switch {
	case strings.HasPrefix(shape, "{"):
		return "object"
	case strings.HasPrefix(shape, "["):
		return "array"
	case strings.HasPrefix(shape, "map["):
		return "object"
	case shape == "any", shape == "marshaler", shape == "...":
		return "unconstrained"
	default:
		return shape // string / number / bool / a bare reflect kind
	}
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

func TestResultShape_MatchesWhatEncodingJSONEmits(t *testing.T) {
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
		{"named map type", pNamedMapHolder{pNamedMap{"k": 1}}},
		{"slice of structs", pStructSlice{[]pExpInner{{"x"}}}},
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

// assertValueKinds checks that each field's shape predicts the JSON KIND actually
// emitted. Comparing key sets alone is not enough: a type implementing
// encoding.TextMarshaler emits a JSON string while its fields say "object", and a
// key-only check waves that through.
func assertValueKinds(t *testing.T, v any, shape string) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return // not an object at top level; the key check already skipped it
	}
	fields := topLevelShapeFields(shape)
	for name, raw := range m {
		sub, ok := fields[name]
		if !ok {
			continue // the key check reports this
		}
		predicted := shapeKindOf(sub)
		if predicted == "unconstrained" {
			continue
		}
		if actual := jsonKindOf(raw); actual != predicted {
			t.Errorf("field %q: shape predicts a %s but encoding/json emitted a %s (%s)\n  shape: %s",
				name, predicted, actual, raw, shape)
		}
	}
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

func TestResultShape_InvalidTagNameFallsBackToTheFieldName(t *testing.T) {
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

type pNamedMap map[string]int
type pNamedMapHolder struct {
	M pNamedMap `json:"m"`
}
type pStructSlice struct {
	S []pExpInner `json:"s"`
}
