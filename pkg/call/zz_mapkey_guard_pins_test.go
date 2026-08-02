package call

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// These pin the two clauses of synthesizeMapKey's TextMarshaler guard that a
// mutation sweep found unpinned: the METHOD SET it asks about, and the KINDS it
// covers. Both mutants left the whole package green while re-opening the
// round-30 false fire, so each gets a fixture that names the property directly.

// mkPtrOnly declares MarshalText on the POINTER receiver only. encoding/json
// reaches a map key through reflect's non-addressable iteration, so it can never
// call this — the key is rendered from its integer kind instead. Narrowing the
// guard to the pointer method set would therefore refuse a type json never
// consults, and widening it to check the pointer set would disarm nothing here.
type mkPtrOnly int

func (m *mkPtrOnly) MarshalText() ([]byte, error) { return []byte("ptr-" + strconv.Itoa(int(*m))), nil }

// mkUnsigned is an UNSIGNED kind with a value-receiver MarshalText. It exists
// because narrowing the guard to signed ints only (Int..Int64) passed the entire
// suite: every other marshaler-keyed fixture is signed.
type mkUnsigned uint16

func (m mkUnsigned) MarshalText() ([]byte, error) { return []byte("u-" + strconv.Itoa(int(m))), nil }

// mkFloatKey covers the remaining non-string kind json renders through a
// marshaler, so the guard is pinned across the whole class rather than at one
// representative.
type mkFloatKey float64

func (m mkFloatKey) MarshalText() ([]byte, error) { return []byte("f"), nil }

// TestSynthesizeMapKey_GuardAsksAboutTheVALUEMethodSet pins the method-set half.
//
// A pointer-receiver MarshalText on a map key is NOT consulted by encoding/json,
// so such a key is rendered from its kind and its shape is a property of the
// type. The guard must therefore leave it alone — asking about the pointer method
// set instead would record no shape for a type that was perfectly describable,
// turning a guarded type unguarded for no reason.
func TestSynthesizeMapKey_GuardAsksAboutTheVALUEMethodSet(t *testing.T) {
	// PREMISE, proven against the real encoder rather than assumed: json renders
	// this key from its integer kind, NOT through the pointer-receiver marshaler.
	b, err := json.Marshal(map[mkPtrOnly]int{7: 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"7":1}`, string(b),
		"FIXTURE BROKEN: encoding/json consulted a POINTER-receiver MarshalText on a map key; "+
			"if that ever becomes true the guard must switch method sets with it")

	require.NotEmpty(t, ResultShapeStringForTest(reflect.TypeOf(map[mkPtrOnly]int{})),
		"a key whose marshaler encoding/json never consults is described by its KIND, so the "+
			"type stays guarded; asking about the pointer method set would disarm it needlessly")
}

// TestSynthesizeMapKey_GuardCoversEveryNonStringKind pins the kind half. The
// guard says "not String AND implements TextMarshaler", and each of these would
// survive a narrowing to signed integers alone.
func TestSynthesizeMapKey_GuardCoversEveryNonStringKind(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"unsigned": reflect.TypeOf(map[mkUnsigned]int{}),
		"float":    reflect.TypeOf(map[mkFloatKey]int{}),
	} {
		t.Run(name, func(t *testing.T) {
			// PREMISE: json really does render this key through the marshaler, so
			// the rendered NAME would otherwise land in the shape.
			require.Empty(t, ResultShapeStringForTest(typ),
				"a %s-kind key with a value-receiver MarshalText has its rendered NAME put in the "+
					"shape by encoding/json, so the shape would track the marshaler's output for a "+
					"FABRICATED key rather than the type — such a type must record no shape", name)
		})
	}
}
