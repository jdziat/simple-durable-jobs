package call

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"sort"
	"strings"
)

// resultFingerprint describes the JSON SHAPE of a Call's result type, so replay
// can tell "the handler now returns a different type" from "the stored payload
// happens to look different".
//
// WHY A WRITE-TIME FINGERPRINT AND NOT AN INSPECTION OF THE STORED BYTES. Three
// earlier attempts tried to infer a type change from checkpoint.Result — strict
// whole-payload decode, then a per-key probe, then a null probe — and each one
// hard-failed replays whose type had NOT changed (a dropped field, a nested field
// under an all-zero value, a required-fields UnmarshalJSON). That is not three
// bugs: a type change and a legitimately-different-but-valid payload are
// indistinguishable in the bytes. The information exists only where the checkpoint
// is WRITTEN, so that is where it is recorded.
//
// Because both sides compute this from the TYPE, an unchanged type always produces
// an identical fingerprint. There is no payload involved and therefore no false
// fire — including for types with a custom UnmarshalJSON, whose accepted shape need
// not resemble their struct at all.
//
// STRUCTURAL, not nominal. The fingerprint is the set of JSON field names and
// kinds, NOT the type's name or package path. Moving a type between packages or
// renaming it is not a semantic change and must not trip replay; changing its
// FIELDS is, and does. Two distinct types with identical shape are deliberately
// interchangeable — if the shape matches, the stored result reconstructs
// faithfully, which is the only property replay needs.
func resultFingerprint(t reflect.Type) string {
	if t == nil {
		return ""
	}
	var b strings.Builder
	writeShape(&b, t, 0)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:8])
}

// maxShapeDepth bounds recursion so a self-referential type (a tree node, a linked
// list) cannot spin. Beyond it the shape is truncated, which is safe: truncation is
// applied identically on both sides, so it can only make two types look the same,
// never make one type look like it changed.
const maxShapeDepth = 6

func writeShape(b *strings.Builder, t reflect.Type, depth int) {
	if depth > maxShapeDepth {
		b.WriteString("...")
		return
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		type field struct{ name, shape string }
		fields := make([]field, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue // unexported: never serialized, so not part of the shape
			}
			name := f.Name
			if tag := f.Tag.Get("json"); tag != "" {
				parts := strings.Split(tag, ",")
				if parts[0] == "-" {
					continue // explicitly not serialized
				}
				if parts[0] != "" {
					name = parts[0]
				}
			}
			var sub strings.Builder
			writeShape(&sub, f.Type, depth+1)
			fields = append(fields, field{name, sub.String()})
		}
		// Sorted so field ORDER is not part of the identity: reordering a struct's
		// fields does not change what JSON it accepts.
		sort.Slice(fields, func(i, j int) bool { return fields[i].name < fields[j].name })
		b.WriteString("{")
		for i, f := range fields {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(f.name)
			b.WriteString(":")
			b.WriteString(f.shape)
		}
		b.WriteString("}")
	case reflect.Slice, reflect.Array:
		b.WriteString("[")
		writeShape(b, t.Elem(), depth+1)
		b.WriteString("]")
	case reflect.Map:
		b.WriteString("map[")
		writeShape(b, t.Key(), depth+1)
		b.WriteString("]")
		writeShape(b, t.Elem(), depth+1)
	case reflect.Interface:
		// An interface accepts anything, so it constrains nothing.
		b.WriteString("any")
	default:
		b.WriteString(t.Kind().String())
	}
}

// ResultFingerprintForTest exposes resultFingerprint to the package's external
// tests. It is not part of the public API surface consumers use.
func ResultFingerprintForTest(t reflect.Type) string { return resultFingerprint(t) }
