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

// topLevelShapeKeys pulls the field names out of a "{a:...,b:...}" shape string,
// ignoring anything nested inside braces or brackets.
func topLevelShapeKeys(shape string) []string {
	if !strings.HasPrefix(shape, "{") || !strings.HasSuffix(shape, "}") {
		return nil
	}
	body := shape[1 : len(shape)-1]
	var keys []string
	depth, start := 0, 0
	flush := func(seg string) {
		if i := strings.Index(seg, ":"); i >= 0 {
			keys = append(keys, seg[:i])
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shape := ResultShapeStringForTest(reflect.TypeOf(tc.val))
			got := topLevelShapeKeys(shape)
			want := jsonTopLevelKeys(t, tc.val)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("shape keys %v != encoding/json keys %v\n  shape: %s", got, want, shape)
			}
		})
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
