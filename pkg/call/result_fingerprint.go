package call

import (
	"crypto/sha256"
	"encoding"
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
	v, ok := synthesize(t, 0, nil)
	if !ok {
		return "", false
	}
	// Marshalling the INTERFACE-BOXED value is what makes this faithful: it is
	// precisely what Call does with a handler's result, so addressability — which
	// decides whether encoding/json may use a pointer-receiver MarshalJSON — comes
	// out the same here as in production, without this file having to model it.
	b, err := json.Marshal(v.Interface())
	if err != nil {
		// A REJECTED PROBE RECORDS NO SHAPE. THIS IS THE POLICY, NOT A BACKSTOP.
		//
		// A marshaler may VALIDATE: net.IP{1} is not a legal address, and an
		// ordinary enum's MarshalJSON rejects a value that is not one of its
		// cases. Both marshal perfectly in production; only the FABRICATED probe
		// offends them. Two revisions tried to keep a shape anyway by substituting
		// a value the marshaler would accept — first zeroing the whole type, then
		// zeroing just the offending member — and BOTH produced a false fire,
		// because a substituted Go value is then reinterpreted by the encoder and
		// what it emits depends on the member's REPRESENTATION:
		//
		//   - `omitempty` on a slice-backed member (net.IP) makes the zero EMPTY,
		//     so encoding/json DROPS it and the member vanishes from the shape;
		//     the same member as a struct (netip.Addr) is never dropped and keeps
		//     its entry. The net.IP -> netip.Addr modernization, which cannot move
		//     a byte, therefore moved the fingerprint.
		//   - a member vanishing is indistinguishable from that member having been
		//     REMOVED from the type, which is precisely the change this guard
		//     exists to catch, so the substitution also silently half-disabled it.
		//   - zeroing reaches through the type: `Sub pDegradeSub` stayed an object
		//     while `Sub *pDegradeSub` became null, splitting the byte-identical
		//     refactor `T -> *T` apart the moment a validating marshaler appeared
		//     anywhere else in the same type.
		//
		// There is no third substitution to try. Opacity is not expressible as a
		// Go value, because the encoder gets to reinterpret any value handed to
		// it. So the type records NO SHAPE and replay skips the check for it —
		// which is ALREADY the documented rule for a type encoding/json cannot
		// marshal at all (UPGRADE.md: "a type whose shape cannot be computed must
		// never be able to fail a replay"), extended to cover "the probe was
		// rejected".
		//
		// THE COST IS AN ACCEPTED MISS, pinned by
		// TestResultShape_ValidatingMarshalerIsADeliberateAcceptedMiss: a result
		// type containing a validating marshaler is not guarded at all, so a real
		// change to it replays as before this feature existed. That is the cheap
		// direction. A miss leaves prior behaviour in place; a false fire rejects
		// a replay that would have succeeded and wedges a live workflow.
		return "", false
	}
	// UNREACHABLE BY CONSTRUCTION, kept rather than discarded with `_`. Every
	// error path above returns, so b here is always output json.Marshal produced
	// and therefore always valid JSON. It used to be half of a double-guard with
	// the Marshal check; now that a Marshal failure returns immediately, no test
	// can red this branch and none should be written to pretend otherwise. It
	// stays because silently dropping a returned error is worse than an
	// unreachable check that fails open the same way everything else here does.
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		return "", false
	}
	var sb strings.Builder
	describe(&sb, decoded)
	return sb.String(), true
}

// maxShapeDepth bounds how far the probe descends, so a type that nests without
// end (a tree node, a linked list, a `map[string]any`-shaped chain) cannot spin.
//
// REACHING IT RECORDS NO SHAPE AT ALL. The walk does not substitute a value and
// carry on; it fails, resultShape returns ok=false, and the type is skipped by
// replay exactly as a type encoding/json cannot marshal already is. That is the
// SAME fail-open used everywhere else in this file, and it is the whole reason
// this constant no longer has a "known residual" paragraph under it.
//
// WHY, because four consecutive revisions each shipped one new false fire and
// every one had this shape: at a boundary where synthesis stops, a VALUE was
// substituted and encoding/json was left to decide its fate — and that fate turns
// on the Go REPRESENTATION, not on the wire form.
//
//	rev 1  array probe        [N]T probes N elements, []T always probes 1
//	rev 2  unprobeable member pointer-boxed marshaler selected only when addressable
//	rev 3  unprobeable member omitempty DROPS zero-of-slice but KEEPS zero-of-struct
//	rev 4  depth cap          omitempty KEEPS a non-nil *T but DROPS a zero scalar
//
// A fifth, older than all of them, sat in build's `case reflect.Interface`: a nil
// interface is EMPTY to encoding/json, so `omitempty` dropped an interface member
// that production always populates while its absence recorded null. Same family,
// same cure, and it is gone the same way.
//
// There is no substitute that fixes this, because opacity is not expressible as a
// Go value: a zero leaks through `omitempty` one way, a non-nil pointer leaks
// through it the other way, and a nil leaks as JSON null. The predecessor of this
// comment described a `truncate` helper that allocated through pointer chains to
// dodge exactly one of those leaks; it dodged one and kept three. Removing the
// substitution removes the family.
//
// THE ASYMMETRY THAT MAKES FAILING OPEN FREE. Call skips the comparison when
// EITHER side records no shape, so a type that trips this bound can never be
// refused on replay and can never refuse another type. Tripping it can only
// downgrade a guarded type to an unguarded one — a miss, which leaves prior
// behaviour in place. A false fire rejects a replay that would have succeeded and
// wedges a live workflow.
//
// IT COUNTS JSON NESTING LEVELS AND NOTHING ELSE. A pointer and an UNTAGGED
// EMBEDDED STRUCT each add zero nesting — encoding/json dereferences the one and
// PROMOTES the other's members straight into the parent object — so neither may
// spend budget. Charging them made the wire-identical refactors `T -> *T` and
// "group these members into an embed" (or its inverse, inlining one) shift every
// member below them a level deeper, across the cap. A TAGGED embed does nest, and
// does spend.
//
// THE NUMBER IS MEASURED, not guessed. An AST walk of every struct type declared
// in this repository — all packages, all tests, all examples, plus the 264 Go
// snippets fenced in the Markdown under docs/, README.md and UPGRADE.md; 672
// struct types in total — scored each one by this same accounting (pointer and
// untagged embed free, every other field/element/map value one level). The
// deepest type declared anywhere outside pkg/call's own synthetic cap fixtures is
// jobsv1.ListWorkflowsResponse at 5 levels
// (.Workflows[] -> WorkflowSummary.RootJob -> Job.Args[]); the deepest type
// actually used as a Call result is jobs_test.Order at 2. Thirty-two is more than
// six times the deepest declared type and sixteen times the deepest real result
// type, which makes failing open effectively unreachable for real code while
// still bounding a type that genuinely nests forever.
const maxShapeDepth = 32

// maxShapeNodes bounds the TOTAL work of one probe, which the depth bound alone
// does not: a struct with k struct-typed members nested d levels deep costs k^d
// value constructions, and raising maxShapeDepth from 6 to 32 raises the ceiling
// on that with it. Exhausting the budget records no shape, the same single
// outcome every other boundary here has, so it fails open and cannot false-fire
// for the reason given above. It is per-probe and passed down the walk rather
// than held in a package variable, because resultShape runs concurrently on every
// worker goroutine.
//
// It is deliberately far above anything real: the deepest measured type in this
// repository builds well under a thousand nodes, and the whole probe is memoized
// per type by fingerprintCache, so this is paid at most once per result type per
// process.
//
// THAT IT EVER TRIPS IS PINNED by TestResultShape_NodeBudgetTripsAndFailsOpen. It
// was not, for one revision: two independent single-line mutations switched the
// budget off entirely — the check and the decrement — and the whole suite stayed
// green, which would have let a later maintainer delete it as dead code. Lowering
// the constant until other tests red proves only that it does not OVER-trip. The
// pinning fixture separates the two bounds a branching-8 type nested 7 JSON levels
// (~2.1M nodes, depth far inside the cap), and its controls hold depth and
// branching fixed one at a time so a green row cannot be the depth cap in
// disguise.
const maxShapeNodes = 100000

// nest descends into a member that encoding/json will put one JSON level deeper.
// Past the budget the WHOLE probe fails; see maxShapeDepth for why nothing is
// substituted here.
func nest(t reflect.Type, depth int, budget *int) (reflect.Value, bool) {
	if depth >= maxShapeDepth {
		return reflect.Value{}, false
	}
	// A fresh free-path: this hop added nesting, so nothing seen above it can
	// still be part of a zero-progress cycle.
	return synthesizeWithin(t, depth+1, nil, budget)
}

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
//
// TERMINATION IS ITS OWN BOUND, SEPARATE FROM maxShapeDepth. Once pointers and
// untagged embeds correctly spend no nesting budget, the budget no longer bounds
// recursion at all: `type T *T`, `type A *B; type B *A`, and two structs that
// embed pointers to each other are all legal Go that recurse forever while adding
// zero JSON levels. That is not a caught error — a stack overflow is a runtime
// FATAL, which resultShape's recover cannot catch, so the guard would kill the
// worker process on the write AND replay path of every Call using the type.
//
// `free` is therefore the list of types entered since the last nesting level was
// added. Re-entering one means the walk is cycling without making progress, so
// the value is left zero and the recursion ends. Deliberately a visited-type set
// and NOT a shared step counter: a counter's trip point depends on how many
// unrelated siblings were walked first, so adding a field somewhere else in the
// type could move it — which is the false-fire shape this file exists to avoid.
//
// `free` is extended with a plain append, which may let two SIBLING branches
// write the same index of one backing array. That is harmless and not worth a
// copy: the walk is strictly depth-first, so no sibling's slice is live while
// another's is being extended, and the value written is `t` — the shared parent —
// in both branches anyway.
//
// THE CYCLE PATH STILL SUBSTITUTES A ZERO, and it is the one boundary the
// "delete every value substitution" rule does not reach. The depth cap and the
// interface case landed on an ARBITRARY member type — chosen by nesting distance,
// or by a member simply being declared `any` — which is why what they substituted
// leaked; a free-path hit lands only on a type that reaches ITSELF through
// pointers and untagged embeds, and the walk has to hand back some value of that
// exact type or it cannot terminate at all. It is stated here rather than papered
// over.
//
// A ZERO ALSO REACHES THE ENCODER FROM build's `default:` BRANCH, for the kinds
// no case above claims — chan, func, complex, unsafe.Pointer. encoding/json
// refuses every one of them, so resultShape's marshal fails and the type records
// no shape regardless of what was put there; that zero cannot reach a shape.
// Those two are the whole list. Every other stopping point in this file — an
// unprobeable marshaler, the depth cap, the node budget, an interface member, a
// map key this file cannot render — records NO SHAPE instead of substituting.
func synthesize(t reflect.Type, depth int, free []reflect.Type) (reflect.Value, bool) {
	budget := maxShapeNodes
	return synthesizeWithin(t, depth, free, &budget)
}

// synthesizeWithin is synthesize threading the per-probe node budget. synthesize
// is the entry point and seeds a fresh budget; every recursive step goes through
// here so one probe cannot be charged another probe's work.
func synthesizeWithin(t reflect.Type, depth int, free []reflect.Type, budget *int) (reflect.Value, bool) {
	if *budget <= 0 {
		return reflect.Value{}, false
	}
	*budget--
	for _, seen := range free {
		if seen == t {
			return reflect.Zero(t), true
		}
	}
	return build(t, depth, free, budget)
}

// build is synthesize's kind switch, split out so the free-path check and the
// probe validation apply to every kind without each case repeating them.
//
// IT DOES NOT MODEL ADDRESSABILITY, deliberately. Whether encoding/json may take
// a value's address decides whether a POINTER-receiver MarshalJSON is reachable
// at that position (condAddrEncoder), and a previous revision threaded an
// `addressable` flag through here to mirror those rules so a member could be
// probed in the form the encoder would select. Nothing probes any more — a
// rejected probe records no shape for the whole type, see resultShape — so the
// flag had no consumer, and a mirror of an encoder rule with no consumer is a
// divergence waiting to happen. The real encoder is handed the real value and
// resolves addressability itself, which is exactly the property this file is
// built on.
func build(t reflect.Type, depth int, free []reflect.Type, budget *int) (reflect.Value, bool) {
	switch t.Kind() {
	case reflect.Pointer:
		// Dereferenced by encoding/json, so it adds no JSON nesting and spends no
		// budget; see maxShapeDepth. What a pointer points AT is always
		// addressable.
		elem, ok := synthesizeWithin(t.Elem(), depth, append(free, t), budget)
		if !ok {
			return reflect.Value{}, false
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(elem)
		if p.Type() != t {
			// A NAMED pointer type (`type P *Foo`, or `type T *T`): reflect.New
			// gives the unnamed *Foo, which is assignable but not identical.
			p = p.Convert(t)
		}
		return p, true

	case reflect.Interface:
		// AN INTERFACE ANYWHERE IN THE TYPE RECORDS NO SHAPE AT ALL. This was the
		// LAST place a value was substituted and encoding/json left to decide its
		// fate, which is the exact mechanism of all four earlier false fires — and
		// it was live at nesting depth 0.
		//
		// The substitution was `reflect.Zero(t)`, a nil interface. encoding/json's
		// isEmptyValue reports a nil interface as EMPTY, so:
		//
		//   - WITHOUT `omitempty` the member is recorded as `null`;
		//   - WITH `omitempty` the member is DROPPED and disappears from the shape.
		//
		// Adding `,omitempty` to an interface member production always populates —
		// `Meta any `json:"meta"`` -> `Meta any `json:"meta,omitempty"`` — cannot
		// move a byte, yet it moved the fingerprint and hard-failed replay. That is
		// a false fire, at depth 0, on an ordinary edit. The same substitution
		// half-disabled the guard in the other direction too: with `omitempty` the
		// member vanishes, so DELETING it entirely is invisible.
		//
		// There is no value that fixes this, for the reason given at maxShapeDepth:
		// opacity is not expressible as a Go value. A nil leaks as null, a non-nil
		// pointer leaks through `omitempty` the other way, and a zero leaks through
		// it the first way. So the type records NO SHAPE and replay skips it,
		// exactly as an unprobeable type and an over-cap type already do. One rule,
		// no exceptions.
		//
		// THE COST IS AN ACCEPTED MISS, and a real one: `Call[any]` and any struct
		// carrying a `Meta any` are now UNGUARDED — not merely unguarded at the
		// interface member, but unguarded entirely, because the whole type records
		// nothing. Their non-interface members used to be compared and no longer
		// are. UPGRADE.md states this plainly and
		// TestResultShape_InterfaceMemberIsADeliberateAcceptedMiss pins it. It is
		// still the cheap direction: a miss leaves prior behaviour in place, a false
		// fire wedges a live workflow. And it is what the docs already promised —
		// "a change behind an `interface` member, which has no concrete value to
		// inspect" was already listed among the changes this cannot catch.
		return reflect.Value{}, false

	case reflect.Struct:
		v := reflect.New(t).Elem()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			f := v.Field(i)
			if !f.CanSet() {
				// Unexported. A plain unexported field is never serialized, so leave
				// it alone. An unexported EMBEDDED field is different: encoding/json
				// promotes its exported members, so it must be populated like any
				// other — a nil embedded pointer has nothing to promote through, and
				// a zero embedded struct drops any promoted `omitempty` member and
				// reports a promoted pointer/slice/map as null. reflect cannot set an
				// unexported field, so reach its already-addressable storage.
				if !sf.Anonymous || !f.CanAddr() {
					continue
				}
				f = reflect.NewAt(sf.Type, unsafe.Pointer(f.UnsafeAddr())).Elem()
			}
			var sub reflect.Value
			var ok bool
			if promotesIntoParent(sf) {
				// Its members land in THIS object, so it adds no nesting.
				//
				// THIS APPEND IS A SECOND CUT, NOT THE LOAD-BEARING ONE, and no
				// test can kill it — stated here so the next reviewer does not
				// spend an afternoon looking for one. A zero-nesting cycle is
				// built only from pointer hops and promoting embeds, and a
				// struct cannot embed itself by value (illegal Go), so every
				// such cycle contains at least one pointer. An infinite walk
				// over a finite type graph must therefore repeat a POINTER type,
				// which the Pointer case's append catches first. Cutting here as
				// well only ends the walk one hop earlier on some rings;
				// termination does not depend on it.
				sub, ok = synthesizeWithin(sf.Type, depth, append(free, t), budget)
			} else {
				sub, ok = nest(sf.Type, depth, budget)
			}
			if !ok {
				return reflect.Value{}, false
			}
			f.Set(sub)
			// A MEMBER THE PARENT IS ABOUT TO DROP RECORDS NO SHAPE FOR THE WHOLE
			// TYPE. See omitzeroWouldDrop: the value is the one build actually
			// produced, so this is the same "trust the probe only where it speaks
			// for the type" rule the struct case already applies to a marshaler.
			if tagHasOption(sf.Tag, "omitzero") && omitzeroWouldDrop(sub) {
				return reflect.Value{}, false
			}
		}
		if !probeSpeaksForType(t, v) {
			return reflect.Value{}, false
		}
		return v, true

	case reflect.Slice:
		// json.RawMessage holds ARBITRARY JSON, so its wire form is a property of
		// the value and not of the type — exactly like an interface member, and it
		// fails open for exactly the same reason.
		//
		// This used to substitute the stand-in json.RawMessage("null"), which
		// SUBSTITUTION IS THE BUG: it pinned every RawMessage member in the shape
		// as `null`, so the ordinary tightening "we carried this as raw JSON while
		// the schema settled, now it is typed" — RawMessage -> a struct, a string
		// or a map — was REFUSED on replay even though it cannot move a byte.
		// Verified: {"r":{"n":1}} on both sides, shapes {r:null} vs {r:{n:number}}.
		//
		// Substituting a value at a boundary and letting encoding/json decide its
		// fate is the single root cause of every false fire this file has shipped.
		// A type whose shape cannot be computed records NO shape and is skipped.
		if t == jsonRawMessageType {
			return reflect.Value{}, false
		}
		// A slice's backing array lives on the heap, so its elements are
		// addressable however the slice itself is held.
		elem, ok := nest(t.Elem(), depth, budget)
		if !ok {
			return reflect.Value{}, false
		}
		s := reflect.MakeSlice(t, 1, 1)
		s.Index(0).Set(elem)
		return s, true

	case reflect.Array:
		v := reflect.New(t).Elem()
		if t.Len() > 0 {
			// An array is stored inline, so its elements are addressable exactly
			// when the array itself is.
			elem, ok := nest(t.Elem(), depth, budget)
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
		// A fixed key literal, so the KEY TYPE does not leak into the shape: json
		// renders every map key as a string, and map[int]V and map[string]V produce
		// byte-identical JSON for the same entries. Synthesizing "x" for one and 1
		// for the other made them fingerprint differently for no wire reason.
		//
		// THAT INVARIANT IS ONLY TRUE FOR THE KEY TYPES synthesizeMapKey ACCEPTS,
		// and the accepting is the point: a key type whose rendered text
		// encoding/json takes from the key's own MarshalText is refused there, so
		// no marshaler's rendering of a fabricated key can reach a shape. See its
		// doc comment — the claim above was once written as unconditional and was
		// false for `map[Status]int`.
		key, ok := synthesizeMapKey(t.Key())
		if !ok {
			return reflect.Value{}, false
		}
		// A map value is never addressable — reflect will not hand out its
		// address, and neither will encoding/json.
		val, ok := nest(t.Elem(), depth, budget)
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

// probeSpeaksForType reports whether the wire form the probe produces for t can
// be trusted to describe the type rather than merely the value the probe managed
// to build. When it cannot, the whole probe fails and t records NO SHAPE — the
// same single outcome every other boundary in this file has, and for the same
// reason: a shape that is wrong is a FALSE FIRE, and a false fire wedges a live
// workflow while a miss only leaves prior behaviour in place.
//
// THE HAZARD. build's struct case cannot set a plain unexported field — reflect
// forbids it and, unlike an unexported EMBEDDED field, there is nothing to
// promote through, so it is left zero. That is harmless for an ordinary struct:
// encoding/json never serializes an unexported field, so state the probe could
// not populate cannot reach the wire. It stops being harmless the moment the
// type carries its own MARSHALER, because a marshaler may read whatever it likes
// — and one that reads ONLY unexported state describes the ZERO of that state.
//
// That case does not fail open on its own. A VALIDATING marshaler (net.IP, an
// enum) REJECTS the fabricated probe, resultShape's marshal errors, and the type
// records no shape. This one ACCEPTS it and returns valid JSON, so a shape IS
// recorded — the zero's structure, presented as the type's.
//
// The idiom that makes it bite is the standard Option/Maybe: an unexported
// `present bool` beside an unexported value, MarshalJSON emitting null when
// absent and the payload when present (samber/mo, moznion/go-optional, every
// hand-rolled equivalent). The probe's Option is ABSENT, so `Opt Option[Inner]`
// shapes as `{opt:null}`. The ordinary simplification `Option[Inner] -> *Inner`
// is byte-identical for every value — present emits the object, absent emits
// null — and shapes as `{opt:{a:number,b:string}}`, so replay refuses a
// checkpoint that decodes into the new type losslessly. Same mechanism, second
// wire form: a set backed by an unexported map that marshals as a sorted array
// probes as an EMPTY array, so `{tags:[]}` and the byte-identical `[]string`
// form's `{tags:[string]}` split apart too.
//
// WHY NOT "ANY TYPE WITH A MARSHALER RECORDS NO SHAPE". That rule is correct and
// unusable: time.Time carries a marshaler over three unexported fields, so every
// result with a timestamp — which is most of them — would go unguarded, and the
// feature would be disarmed for ordinary code rather than for an exotic corner.
//
// THE LINE DRAWN INSTEAD: trust the probe's wire form exactly when it is a JSON
// SCALAR. A scalar has no internal structure for the probe to have got wrong —
// describe records its KIND and nothing else, and the kind of `""` is the kind
// of "10.0.0.1". An object, an array or null all carry structure that comes from
// the state the probe could not set: an object's member set, an array's element
// shape, and null's everything. So time.Time (a string), netip.Addr (a string,
// empty at the zero), *big.Int (a number) and a decimal-as-string keep exactly
// the shapes they had, while the Option (null) and the set ([]) record none.
//
// THAT LAST SENTENCE IS ONLY TRUE OF THE MEMBER'S OWN BYTES. Whether the PARENT
// emits the member at all is a separate question, and `,omitzero` answers it from
// the very zero this function just decided to trust — so time.Time, netip.Addr
// and big.Int keep their shapes here and are dropped by the parent there. See
// omitzeroWouldDrop, which build consults for each member as it is built.
//
// Note the check runs on the built VALUE rather than on a rule about the type,
// which is what keeps it out of the mirror-encoding/json business this file was
// rebuilt to escape: the real encoder is handed the real value and its answer is
// read back.
//
// encoding.TextMarshaler IS DELIBERATELY NOT CONSIDERED, and that is not an
// oversight to be "fixed" later: encoding/json renders a TextMarshaler's output
// as a JSON STRING, always, so its wire form is scalar by construction and can
// never misrepresent structure. Naming it here would add a condition no test
// could ever distinguish from its absence — the redundant-clause failure this
// file has been through before.
//
// RESIDUAL, stated rather than papered over: a marshaler whose ZERO emits a
// scalar of one kind while populated values emit another — `""` when absent and
// an object when present — is still described by its zero and can still false
// fire. Nothing in a probe that cannot populate the state can distinguish that
// from netip.Addr, and disarming it means disarming time.Time. The second
// residual is that the unexported state is only looked for on t ITSELF; a
// marshaler that reaches into an exported member's unexported fields is not
// detected. Both are narrower than the hazard closed here.
func probeSpeaksForType(t reflect.Type, v reflect.Value) bool {
	if !hasUnpopulatedState(t) {
		return true
	}
	// THE POINTER FORM IS READ FOR BOTH RECEIVER KINDS, and that is one branch on
	// purpose. A VALUE-receiver MarshalJSON is in *T's method set too and produces
	// the same bytes through it, so a separate value-form branch would be a clause
	// no fixture could ever distinguish from its absence. A POINTER-receiver one
	// is reachable ONLY through an address — a slice element, anything behind a
	// pointer — and is invisible to a value-form probe, which is the half a
	// value-only check would miss.
	//
	// Where a pointer-receiver marshaler is NOT reachable (a plain struct field,
	// a map value, an array element) encoding/json emits the ordinary struct
	// encoding and the probe would have been right, so refusing there costs a
	// miss. This file does not model addressability (see build) and will not start
	// for this: the cost of being conservative is a miss, the cost of being wrong
	// is a wedge.
	pt := reflect.PointerTo(t)
	if !pt.Implements(jsonMarshalerType) {
		return true
	}
	p := reflect.New(t)
	p.Elem().Set(v)
	return marshalsToScalar(p)
}

// hasUnpopulatedState reports whether build left any state inside t at its zero
// because reflect cannot write it.
//
// A plain unexported field qualifies outright: build skips it entirely.
//
// An unexported EMBEDDED field is different and needs recursion, which an earlier
// version of this function got wrong. build DOES write it — it reaches the
// addressable storage (see build's struct case) — so the field itself is not the
// problem. But it writes only what build could produce, and build cannot set that
// embedded type's OWN plain unexported fields. So a marshaler on the outer type
// reading through the embed still describes a zero, and the false fire this whole
// guard exists to stop survives one level down. The earlier godoc asserted the
// opposite ("an unexported EMBEDDED field is populated through its addressable
// storage") as though writing the field settled the question; it does not.
//
// visited bounds the walk. Mutually-embedded pointer types (`type a struct{ *b }`
// with `type b struct{ *a }`) are legal Go, and without it this recurses forever
// on the write path AND the replay path, where recover() cannot catch the stack
// overflow.
func hasUnpopulatedState(t reflect.Type) bool {
	return hasUnpopulatedStateSeen(t, nil)
}

func hasUnpopulatedStateSeen(t reflect.Type, visited []reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for _, seen := range visited {
		if seen == t {
			return false // already accounted for on this path
		}
	}
	visited = append(visited, t)

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		// A PLAIN UNEXPORTED field is the only one build never touches at all.
		if sf.PkgPath != "" && !sf.Anonymous {
			return true
		}

		// EVERYTHING ELSE IS WRITTEN — and that settles nothing, which is the
		// mistake this function has now made twice. build writes the field, but
		// only with what build could produce, so a marshaler on the OUTER type
		// still reads a zero through it. The first version said that of an
		// unexported embed; the second said it of anything EXPORTED, skipping the
		// field with "exported: build sets it outright". Capitalising one letter of
		// this package's own pinned Option fixture re-opened the bug, and the
		// textbook `type NullTime struct{ time.Time }` — an exported embed of a
		// type made entirely of unexported fields — recorded its ZERO's shape, so
		// `NullTime -> *time.Time` false-fired on a deploy that cannot move a byte.
		//
		// So recurse through every written field, exported or not, named or
		// embedded. Being wrong in this direction costs a MISS: the type records no
		// shape and replay skips it. Being wrong the other way wedges a live
		// workflow.
		//
		// It does not over-disarm, because this is not the last gate.
		// probeSpeaksForType only consults it for a type that HAS a marshaler, and
		// then still trusts a probe whose wire form is a scalar — so
		// `struct{ time.Time; X int }` with a promoted MarshalJSON keeps its shape.
		ft := sf.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if hasUnpopulatedStateSeen(ft, visited) {
			return true
		}
	}
	return false
}

// omitzeroWouldDrop reports whether encoding/json's `omitzero` option would omit
// a member holding v — the value build actually produced, not a rule about the
// member's type.
//
// WHY THE PROBE NEEDS THIS AT ALL. build cannot set unexported fields, so a
// member type made only of them — time.Time, netip.Addr, big.Int, a decimal
// carried as a struct — is probed at its ZERO. probeSpeaksForType then trusts it,
// and rightly: its wire form IN ISOLATION is a scalar, and a scalar has no
// structure the probe could have got wrong. But that is a statement about the
// MEMBER's own bytes and says nothing about whether the PARENT emits the member
// at all. With `omitzero` the parent drops it, so the shape loses a member
// production — which never has a zero timestamp — always writes.
//
// The result is the documented false-fire family reaching a new boundary. On a
// member declared Created time.Time, the tag edit json:"created" ->
// json:"created,omitzero" is byte-identical for every non-zero timestamp, yet it
// moves the shape from {created:string,n:number} to {n:number}. Both are
// non-empty, so call.go's fail-open guard does not skip it, and replay hard-fails
// on an edit that cannot move a byte.
//
// THE ANSWER IS THE FILE'S SETTLED RULE, NOT A LIST OF TYPES. A shape the probe
// cannot be trusted to have produced is no shape at all, so a dropped member
// takes the whole type's shape with it. Asking about the VALUE — rather than
// naming time.Time and netip.Addr — is what keeps this from rotting as the next
// such type appears, and needs no knowledge of any of them.
//
// THIS MIRRORS AN encoding/json RULE, WHICH THIS FILE OTHERWISE REFUSES TO DO,
// so here is why it is safe to: it is a deliberate OVER-approximation, and every
// way it can disagree with the encoder costs a MISS, never a fire. json drops the
// member when the type (or its pointer) has an `IsZero() bool` and that reports
// true, and otherwise when reflect reports the value zero. This returns true if
// EITHER holds, which is a superset of json's answer in all four of its cases:
//
//   - no IsZero anywhere: both reduce to v.IsZero() — exact;
//   - T has IsZero: json consults only the method, this also drops a
//     reflect-zero value whose method says false — a miss;
//   - only *T has IsZero: json boxes an unaddressable value to call it, which is
//     what the second box below does — same answer;
//   - t is a pointer with IsZero: json drops a nil, and so does v.IsZero().
//
// So an over-drop records no shape and leaves the guard skipped, which is the
// direction this file has chosen at every other boundary; an under-drop would
// wedge a live workflow, and none is reachable.
//
// A user's IsZero may panic on a value it has never seen, exactly as a MarshalJSON
// may; resultShape's recover turns that into the same "no shape".
func omitzeroWouldDrop(v reflect.Value) bool {
	if v.IsZero() {
		// Also the only case where a promoted value-receiver IsZero could be
		// called on a nil pointer, so the calls below cannot nil-panic.
		return true
	}
	if z, ok := v.Interface().(interface{ IsZero() bool }); ok {
		return z.IsZero()
	}
	// A POINTER-RECEIVER IsZero is not in T's method set but IS in *T's, and
	// encoding/json boxes an unaddressable value to reach it — so a member whose
	// state the probe could not set is dropped while reflect still reports the
	// value non-zero. Reading the pointer form second rather than instead is what
	// makes the two branches distinguishable: a value-receiver method answers
	// identically through either form, a pointer-receiver one answers only here.
	p := reflect.New(v.Type())
	p.Elem().Set(v)
	if z, ok := p.Interface().(interface{ IsZero() bool }); ok {
		return z.IsZero()
	}
	return false
}

// tagHasOption reports whether a json struct tag carries the named option — the
// comma-separated list after the name, compared whole so that a name or another
// option merely CONTAINING want does not match.
func tagHasOption(tag reflect.StructTag, want string) bool {
	_, opts, _ := strings.Cut(tag.Get("json"), ",")
	for opts != "" {
		var opt string
		opt, opts, _ = strings.Cut(opts, ",")
		if opt == want {
			return true
		}
	}
	return false
}

// marshalsToScalar reports whether v serializes to a JSON string, number or
// boolean — a wire form with no internal structure.
//
// It is written as a REJECTLIST of the three structured forms rather than an
// allowlist of the scalar ones, because the rejectlist is provably complete: the
// first byte of anything json.Marshal produces is one of `{ [ n " t f -` or a
// digit, so excluding object, array and null leaves exactly the scalars. An
// allowlist would carry clauses for `-` and each digit that no fixture could
// distinguish from their absence.
//
// THE ERROR BRANCH CANNOT BE KILLED BY A TEST, and is kept anyway — stated here
// so the next reviewer does not spend an afternoon writing the fixture that
// would. json.Marshal returns nil bytes with its error, so deleting the branch
// indexes b[0] on nil, and the resulting panic is caught by resultShape's
// recover and turned into the SAME "no shape" this returns. The whole suite
// stays green through that mutation. Reaching the outcome deliberately beats
// reaching it through a recovered index panic, and nothing checks for EMPTY
// output beside it: a successful Marshal never returns an empty document, so a
// length check could not be told apart from this one either.
func marshalsToScalar(v reflect.Value) bool {
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return false
	}
	switch b[0] {
	case '{', '[', 'n':
		return false
	}
	return true
}

// promotesIntoParent reports whether encoding/json splices sf's members straight
// into the PARENT object instead of nesting them under a key of their own — the
// one struct-field case that adds no JSON nesting level.
//
// This is the only encoding/json rule left in this file, and it is deliberately
// only ever consulted for DEPTH ACCOUNTING: the shape itself still comes from the
// real encoder, so getting this wrong cannot invent or drop a member — it can
// only move where truncation lands, which changes what a DEEP type fingerprints
// to. Saying "nests" when json promotes is exactly the status quo this replaced;
// saying "promotes" when json nests lets the walk run one level past the budget
// for every hop it gets wrong, so a chain of them compounds.
//
// Its agreement with the encoder is pinned two ways: the promotion fixtures in
// zz_result_shape_json_parity_test.go compare against real encoder key sets, and
// TestResultShape_DepthBudgetIsChargedAtEveryNestingSite puts each answer this
// function can give on the truncation boundary, where a wrong answer moves the
// shape. THAT SECOND CLAIM WAS FALSE WHEN FIRST WRITTEN and the mutation that
// proved it is now in the table: the guard below distinguishes an ANONYMOUS field
// from an ordinary one, and every row that existed had either a tag (which the
// `name != ""` check short-circuits) or a non-struct member type (which the
// `ft.Kind()` check short-circuits), so deleting `if !sf.Anonymous` survived the
// whole suite. The row "untagged NON-ANONYMOUS struct member" is the combination
// that separates them, and TestResultShape_UntaggedNonAnonymousStructMemberNests
// asserts both halves of the distinction against the real encoder's output.
func promotesIntoParent(sf reflect.StructField) bool {
	if !sf.Anonymous {
		return false
	}
	// A json name of its own makes json nest it rather than promote it; an
	// explicit "-" drops it, which is not promotion either.
	if name, _, _ := strings.Cut(sf.Tag.Get("json"), ","); name != "" {
		return false
	}
	ft := sf.Type
	if ft.Name() == "" && ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
	}
	// Anything that is not a struct is emitted as an ordinary member named after
	// its type, so it nests like any other member.
	return ft.Kind() == reflect.Struct
}

// synthesizeMapKey builds the same key -- "1" -- whatever the key type, so two
// map types that serialize identically fingerprint identically.
//
// ANY OTHER KEY KIND RECORDS NO SHAPE, by the same one rule as everywhere else in
// this file. The kinds below are the ones whose rendered key this function fully
// controls; every other kind reaches encoding/json only through an
// encoding.TextMarshaler, and the key NAME is then whatever that marshaler makes
// of the value handed to it — which is the substitution family again, and it was
// live: `map[K]int` and `map[*K]int` for a K with a value-receiver MarshalText
// both marshal to `{"k":1}`, but zeroing the pointer form gives a nil key whose
// rendered name is empty, so the two shaped `{k:number}` and `{:number}` and a
// byte-identical change of one to the other was refused on replay. A key type
// json cannot use at all (a struct with no TextMarshaler) already recorded no
// shape via the marshal error; this makes the outcome the same for both.
//
// "WHOSE RENDERED KEY THIS FUNCTION CONTROLS" IS NOT A STATEMENT ABOUT THE KIND.
// The kind switch alone got that wrong, and the case it got wrong is the enum-
// keyed count map — `ByStatus map[Status]int` — which is about as ordinary as a
// result member gets. encoding/json's resolveKeyName short-circuits on
// reflect.String FIRST and only THEN looks for an encoding.TextMarshaler, so:
//
//	map[string]V, map[NamedString]V   the key text is taken verbatim; a
//	                                  MarshalText on the key type is never called
//	map[Status]V (Status an int)      the marshaler is ALWAYS called, before the
//	                                  strconv branch is ever reached
//
// So for an integer or uint key that declares MarshalText the shape's key name is
// MarshalText(K(1)) — "active", only because that is what Status(1) happened to
// spell that day. Inserting one constant at the FRONT of the same iota block
// moves every code by one while every name stays attached to its own state: an
// edit that provably cannot move a byte, because every persisted key is written
// by NAME. It moved the fingerprint from aaeb546ea684c9ac to 9492bef0fd81f2c5 and
// refused every in-flight replay with "written from a different result type",
// about a result type whose declaration had not changed. A shape that is a
// function of a FABRICATED VALUE rather than of the type is not a shape, so such
// a key records NO SHAPE, like every other boundary here.
//
// THE STRING CASE STAYS, and that boundary is load-bearing rather than a
// convenience: json never consults a string-kind key's marshaler, so disarming it
// would buy a strictly larger accepted miss for no false fire at all.
// TestResultShape_StringKindMapKeyIgnoresItsMarshalText pins both halves.
func synthesizeMapKey(t reflect.Type) (reflect.Value, bool) {
	// MIRRORS resolveKeyName'S ORDER, which is the whole of the rule: String is
	// tested before TextMarshaler there, so it is tested before it here too.
	if t.Kind() != reflect.String && t.Implements(textMarshalerType) {
		// The VALUE method set, not the pointer one: a map key reaches
		// encoding/json through reflect's non-addressable iteration, so only a
		// value-receiver MarshalText is ever in play.
		return reflect.Value{}, false
	}
	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf("1").Convert(t), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(int64(1)).Convert(t), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		return reflect.ValueOf(uint64(1)).Convert(t), true
	default:
		return reflect.Value{}, false
	}
}

var (
	jsonNumberType     = reflect.TypeOf(json.Number(""))
	jsonRawMessageType = reflect.TypeOf(json.RawMessage(nil))
	jsonMarshalerType  = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	// Consulted for MAP KEYS ONLY. Elsewhere in this file a TextMarshaler is
	// deliberately not considered — see probeSpeaksForType — because its output is
	// always a JSON string and cannot misrepresent structure. A map key is the one
	// position where that output becomes a member NAME.
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
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
		// ARITY IS DELIBERATELY NOT RECORDED. A fixed-size array's length is part
		// of its wire form, so `[3]float64 -> [2]float64` replays silently
		// truncated — an ACCEPTED MISS, pinned by
		// TestResultShape_ArrayArityIsADeliberateAcceptedMiss. Recording the
		// length here was tried and reverted: one symmetric equality hash cannot
		// express "arity 3 is compatible with unconstrained", so it necessarily
		// made the byte-identical widening `[3]float64 -> []float64` — the single
		// most common evolution of such a field — hard-fail replay. A miss leaves
		// prior behaviour alone; a false fire wedges a live workflow.
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
