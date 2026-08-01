package call

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

// resultFingerprint describes the JSON SHAPE of a Call's result type, so replay
// can tell "the handler now returns a different type" from "the stored payload
// happens to look different".
//
// WHY A WRITE-TIME FINGERPRINT AND NOT AN INSPECTION OF THE STORED BYTES. Three
// early attempts tried to infer a type change from checkpoint.Result — strict
// whole-payload decode, then a per-key probe, then a null probe — and each one
// hard-failed replays whose type had NOT changed (a dropped field, a nested field
// under an all-zero value, a required-fields UnmarshalJSON). That is not three
// bugs: a type change and a legitimately-different-but-valid payload are
// indistinguishable in the bytes. The information exists only where the checkpoint
// is WRITTEN, so that is where it is recorded.
//
// WHY IT MARSHALS INSTEAD OF WALKING reflect. This function used to mirror
// encoding/json's field-resolution rules by hand: promotion of embedded structs,
// tagged embeds nesting rather than promoting, dominantField's tie rule, the
// ",string" option, json.Number, json.Marshaler, encoding.TextMarshaler. Four
// consecutive review rounds each found a divergence in that mirror, and each fix
// introduced the next one — because the rules are numerous, interacting, and not
// where you would guess. ",string" is INERT on a struct field. A pointer-receiver
// MarshalJSON is never used on the interface-boxed value Call actually marshals.
//
// A divergence is not cosmetic here. It makes a byte-identical refactor FALSE
// FIRE: replay refuses a checkpoint that would have decoded perfectly, and a live
// workflow wedges with an error message that is provably untrue.
//
// So the shape is no longer derived from the type by hand. A representative value
// of the type is marshalled with the REAL encoder, exactly the way Call marshals a
// result, and the shape is read off the JSON that comes back. The mirror is gone
// and with it the entire class of divergence: this cannot disagree with
// encoding/json, because it IS encoding/json.
//
// STRUCTURAL, not nominal. The shape is JSON member names and kinds, never a Go
// type name or package path. Moving a type between packages or renaming it is not
// a semantic change and must not trip replay; changing what it SERIALIZES is, and
// does. Two distinct types with identical shape are deliberately interchangeable —
// if the shape matches, the stored result reconstructs faithfully, which is the
// only property replay needs.
func resultFingerprint(t reflect.Type) string {
	if t == nil {
		return ""
	}
	if cached, ok := fingerprintCache.Load(t); ok {
		return cached.(string)
	}
	fp := ""
	if shape, ok := resultShape(t); ok {
		sum := sha256.Sum256([]byte(shape))
		fp = hex.EncodeToString(sum[:8])
	}
	fingerprintCache.Store(t, fp)
	return fp
}

// fingerprintCache memoizes the shape per result type, the way encoding/json
// caches its own field resolution. A type's shape cannot change while the process
// runs, and without this the marshal round-trip would run on both the write and
// the replay path of EVERY nested Call — measured at 4.5us and 43 allocations,
// versus 26ns and none once cached. Growth is bounded by the number of distinct
// result types in the binary. The empty shape is cached too, so a type json
// cannot marshal is not retried on every call.
var fingerprintCache sync.Map

// resultShape returns the pre-hash shape string. ok is false when no shape can be
// determined — a nil type, or one encoding/json refuses (a channel or func field).
// Both mean "record no shape", which makes replay skip the check: a type whose
// shape cannot be computed must never be able to wedge a replay.
func resultShape(t reflect.Type) (shape string, ok bool) {
	if t == nil {
		return "", false
	}
	// A user's MarshalJSON/MarshalText can panic, and synthesize feeds it a value
	// it has never seen — so a marshaler that is perfectly safe on real data can
	// still blow up on the probe. This runs inside every nested Call INCLUDING the
	// replay path, where nothing else would marshal that type, so an escaping panic
	// would be a new production crash introduced by the guard itself. Treat it as
	// "no shape", the same fail-open used for a type json cannot marshal: a result
	// type whose shape cannot be computed must never be able to break a Call.
	defer func() {
		if r := recover(); r != nil {
			shape, ok = "", false
		}
	}()
	v, ok := synthesize(t, 0)
	if !ok {
		return "", false
	}
	// Marshalling the INTERFACE-BOXED value is what makes this faithful: it is
	// precisely what Call does with a handler's result, so addressability — which
	// decides whether encoding/json may use a pointer-receiver MarshalJSON — comes
	// out the same here as in production.
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return "", false
	}
	// These two error checks are deliberately double-guarding and only fail
	// TOGETHER: when Marshal fails, b is nil and Unmarshal fails too. Mutating
	// either one alone leaves the suite green, which reads like dead code but is
	// not — removing BOTH makes an unmarshalable type produce a shape, and
	// TestResultShape_UnmarshalableTypeYieldsNoShape reds. Probe them as a pair.
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		return "", false
	}
	var sb strings.Builder
	describe(&sb, decoded)
	return sb.String(), true
}

// maxShapeDepth bounds synthesis so a self-referential type (a tree node, a linked
// list) cannot spin. Beyond it a value is left zero, so a pointer serializes as
// null. The bound applies identically on both sides, so it can only make two types
// look the same, never make one type look like it changed.
const maxShapeDepth = 6

// synthesize builds a representative value of t with its fields populated, so the
// marshalled JSON exposes every member the type can emit.
//
// It does NOT pre-screen types encoding/json cannot marshal (a channel or func
// field). An earlier version did, and a mutation control showed the branch was
// unreachable in effect: json.Marshal rejects those types anyway, so resultShape's
// error path already produces "no shape". One tested mechanism beats two where
// only one can ever fire.
//
// Population matters for two reasons: `omitempty` drops an empty value, and an
// embedded POINTER must be non-nil for encoding/json to promote through it. A
// zero-valued probe would silently under-report both.
func synthesize(t reflect.Type, depth int) (reflect.Value, bool) {
	if depth > maxShapeDepth {
		return reflect.Zero(t), true
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem, ok := synthesize(t.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(elem)
		return p, true

	case reflect.Interface:
		// Nothing concrete to put here, so it stays nil and serializes as null.
		// That is a real, stable shape — and crucially NOT the empty sentinel, so
		// tightening Call[any] to a concrete type is still caught.
		return reflect.Zero(t), true

	case reflect.Struct:
		v := reflect.New(t).Elem()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			f := v.Field(i)
			if !f.CanSet() {
				// Unexported. A plain unexported field is never serialized, so leave
				// it alone. An unexported EMBEDDED field is different: encoding/json
				// still promotes its exported members. A non-pointer embed marshals
				// fine from its zero value, but a POINTER one must be non-nil or json
				// has nothing to promote through and the members vanish from the
				// shape. reflect cannot set an unexported field, so reach the
				// already-addressable storage directly to populate just that case.
				if !sf.Anonymous || sf.Type.Kind() != reflect.Pointer || !f.CanAddr() {
					continue
				}
				f = reflect.NewAt(sf.Type, unsafe.Pointer(f.UnsafeAddr())).Elem()
			}
			sub, ok := synthesize(sf.Type, depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			f.Set(sub)
		}
		return v, true

	case reflect.Slice:
		// json.RawMessage is a []byte holding arbitrary JSON. Filling it with
		// arbitrary BYTES produces invalid JSON and the marshal fails, which would
		// silently disable the guard for any result type containing one. It
		// constrains nothing, so give it a valid, stable stand-in.
		if t == jsonRawMessageType {
			return reflect.ValueOf(json.RawMessage("null")).Convert(t), true
		}
		elem, ok := synthesize(t.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		s := reflect.MakeSlice(t, 1, 1)
		s.Index(0).Set(elem)
		return s, true

	case reflect.Array:
		v := reflect.New(t).Elem()
		if t.Len() > 0 {
			elem, ok := synthesize(t.Elem(), depth+1)
			if !ok {
				return reflect.Value{}, false
			}
			// Only element 0: describe reads an array's shape from its first
			// element, so populating the rest cannot change the result and a large
			// array would otherwise cost one reflect Set per element.
			v.Index(0).Set(elem)
		}
		return v, true

	case reflect.Map:
		key, ok := synthesize(t.Key(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		val, ok := synthesize(t.Elem(), depth+1)
		if !ok {
			return reflect.Value{}, false
		}
		m := reflect.MakeMap(t)
		m.SetMapIndex(key, val)
		return m, true

	case reflect.String:
		// json.Number is a string whose contents must parse as a number or
		// encoding/json rejects it. Any other string only needs to be non-empty so
		// `omitempty` keeps it.
		if t == jsonNumberType {
			return reflect.ValueOf(json.Number("1")).Convert(t), true
		}
		return reflect.ValueOf("x").Convert(t), true

	case reflect.Bool:
		return reflect.ValueOf(true).Convert(t), true

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(int64(1)).Convert(t), true

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		return reflect.ValueOf(uint64(1)).Convert(t), true

	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(1.5).Convert(t), true

	default:
		return reflect.Zero(t), true
	}
}

var (
	jsonNumberType     = reflect.TypeOf(json.Number(""))
	jsonRawMessageType = reflect.TypeOf(json.RawMessage(nil))
)

// describe renders decoded JSON as a canonical shape: object members sorted by
// name, array element shape taken from the first element, scalars as their JSON
// kind. Values never appear — only structure — so two runs of the same type always
// agree, while a changed member set does not.
func describe(b *strings.Builder, v any) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("{")
		for i, k := range keys {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(k)
			b.WriteString(":")
			describe(b, x[k])
		}
		b.WriteString("}")
	case []any:
		b.WriteString("[")
		if len(x) > 0 {
			describe(b, x[0])
		}
		b.WriteString("]")
	case string:
		b.WriteString("string")
	case float64:
		b.WriteString("number")
	case bool:
		b.WriteString("bool")
	case nil:
		b.WriteString("null")
	default:
		b.WriteString("unknown")
	}
}

// ResultFingerprintForTest exposes resultFingerprint to the package's external
// tests. It is not part of the public API surface consumers use.
func ResultFingerprintForTest(t reflect.Type) string { return resultFingerprint(t) }

// ResultShapeStringForTest exposes the pre-hash shape, so a test can check it
// against what encoding/json actually emits. Not part of the public API surface.
func ResultShapeStringForTest(t reflect.Type) string {
	s, _ := resultShape(t)
	return s
}
