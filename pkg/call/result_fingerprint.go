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
//
// The rule this file must obey, and got wrong once: the shape has to be what
// encoding/json ACTUALLY SERIALIZES, not what the Go struct literally declares.
// Where those differ — embedded fields, which json promotes into the parent — a
// literal reading makes the fingerprint nominal in disguise (renaming an embedded
// type changed the shape while the bytes stayed identical) AND blind (an unexported
// embedded type's promoted fields vanished from the shape entirely, so swapping the
// whole field set went undetected). See collectShapeFields.
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

// shapeField is one field as encoding/json would emit it. embedDepth is how many
// embedded structs were traversed to reach it, which is what Go's promotion
// conflict rule is decided on.
type shapeField struct {
	name       string
	shape      string
	embedDepth int
}

// collectShapeFields walks a struct the way encoding/json does, promoting embedded
// struct fields into the parent instead of treating them as a field named after
// their type.
//
// Two subtleties, both of which were live defects here:
//
//   - An embedded struct whose TYPE is unexported still has its exported fields
//     promoted and serialized by encoding/json. So the usual "skip PkgPath != in"
//     test must NOT be applied to it, or the promoted fields disappear from the
//     shape and a completely different field set fingerprints identically.
//   - An embedded field WITH a json tag is not promoted; json treats it as an
//     ordinary field under the tag name.
func collectShapeFields(t reflect.Type, depth, embedDepth int, out *[]shapeField) {
	if embedDepth > maxShapeDepth {
		return // pathological embed chain (or an embedded *T cycle): stop.
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		tag := f.Tag.Get("json")
		if tag == "-" {
			continue // explicitly not serialized
		}
		tagName := ""
		if tag != "" {
			tagName = strings.Split(tag, ",")[0]
		}

		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		// Untagged embedded struct: json promotes its fields into this object, so
		// the shape must too. Deliberately BEFORE the unexported check — an
		// unexported embedded type still contributes its exported fields.
		if f.Anonymous && tagName == "" && ft.Kind() == reflect.Struct {
			collectShapeFields(ft, depth, embedDepth+1, out)
			continue
		}

		if f.PkgPath != "" {
			continue // unexported and not a promoting embed: never serialized
		}

		name := f.Name
		if tagName != "" {
			name = tagName
		}
		var sub strings.Builder
		writeShape(&sub, f.Type, depth+1)
		*out = append(*out, shapeField{name: name, shape: sub.String(), embedDepth: embedDepth})
	}
}

// resolveShapeFields applies Go's promotion conflict rule: for a given JSON name
// the shallowest embedding wins, and if two fields tie at that shallowest depth
// neither is serialized at all.
func resolveShapeFields(in []shapeField) []shapeField {
	byName := make(map[string][]shapeField, len(in))
	order := make([]string, 0, len(in))
	for _, f := range in {
		if _, seen := byName[f.name]; !seen {
			order = append(order, f.name)
		}
		byName[f.name] = append(byName[f.name], f)
	}
	out := make([]shapeField, 0, len(order))
	for _, name := range order {
		group := byName[name]
		best := group[0].embedDepth
		for _, g := range group {
			if g.embedDepth < best {
				best = g.embedDepth
			}
		}
		n := 0
		var winner shapeField
		for _, g := range group {
			if g.embedDepth == best {
				n++
				winner = g
			}
		}
		if n == 1 {
			out = append(out, winner)
		}
		// n > 1: ambiguous promotion, json emits nothing for this name.
	}
	return out
}

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
		var fields []shapeField
		collectShapeFields(t, depth, 0, &fields)
		fields = resolveShapeFields(fields)
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
		// An interface accepts anything, so it constrains nothing. Note this is a
		// real, non-empty shape: an interface result must be distinguishable from
		// "no shape recorded", or tightening Call[any] to a concrete type would be
		// mistaken for a pre-upgrade checkpoint and skipped.
		b.WriteString("any")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		// JSON has ONE number type. Widening int to int64 leaves the wire format and
		// the decode identical, so it must not read as a type change.
		b.WriteString("number")
	default:
		b.WriteString(t.Kind().String())
	}
}

// ResultFingerprintForTest exposes resultFingerprint to the package's external
// tests. It is not part of the public API surface consumers use.
func ResultFingerprintForTest(t reflect.Type) string { return resultFingerprint(t) }
