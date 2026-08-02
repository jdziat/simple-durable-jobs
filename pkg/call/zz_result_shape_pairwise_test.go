package call

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---------------------------------------------------------------------------
// PAIRWISE WIRE-IDENTITY SWEEP
//
// The parity table next door proves shape(T) == encoder(one populated value of
// T). That is NOT the property UPGRADE.md promises. The promise is PAIRWISE:
//
//	two result types whose encoder output is IDENTICAL must fingerprint
//	IDENTICALLY, because a deploy that swaps one for the other cannot move a
//	single byte and must never be refused on replay.
//
// Those are different properties, and the difference is where every defect of
// the last four revisions lived. Each was found only because a human hand-wrote
// the specific T/*T pair that exposed it; the parity table reports such a case
// as a plain "the shape says null here", and cannot tell anyone whether that is
// a false fire or a harmless miss. The table can only ever see ONE type at a
// time, and a false fire is a statement about TWO.
//
//	rev 1  array probe        [N]T probes N elements, []T always probes 1
//	rev 2  unprobeable member pointer-boxed marshaler selected only when addressable
//	rev 3  unprobeable member omitempty DROPS zero-of-slice but KEEPS zero-of-struct
//	rev 4  depth cap          omitempty KEEPS a non-nil *T but DROPS a zero scalar
//
// Every one of those is the same mechanism: at a boundary where synthesis stops,
// a VALUE is substituted and encoding/json is left to decide its fate — and that
// fate turns on the Go REPRESENTATION rather than on the wire form.
//
// So this file stops hand-writing the pair. It GENERATES wire-identical variants
// of every fixture — T/*T, []T/[]*T, omitempty toggled on a POPULATED member,
// members grouped into an untagged embed vs inlined, a named type vs its
// underlying, [N]T vs []T holding N elements — places each one at nine different
// depths (top level, level 1, level 2, inside a slice element, inside a map
// value, and four computed from maxShapeDepth that straddle the budget), and
// requires the two fingerprints to be equal.
//
// THE ORDER OF THE THREE STEPS IS LOAD-BEARING:
//
//	1. marshal BOTH populated values and require the wire bytes to be
//	   byte-identical. If they are not, the pair is invalid — it is not a false
//	   fire for two types that serialize differently to fingerprint differently
//	   — so the pair is SKIPPED and said so. A previous round burned itself
//	   asserting on a pair that was never wire-identical in the first place.
//	2. only then require the fingerprints to be equal.
//	3. on failure report the pair, the shared wire bytes, and BOTH shapes, so the
//	   finding is actionable without re-deriving it.
//
// THIS SUITE IS EXPECTED TO BE RED. It is a detector for a known-live bug
// family, not a regression test for a fixed one. A failure here is a FINDING.
// Weakening a rule to make it green re-creates exactly the blind spot that let
// four revisions ship a false fire each.
//
// The values are populated by populatePairwise below — a POPULATOR WRITTEN HERE,
// deliberately not the package's own synthesize. synthesize is the code under
// test; using it to build the values would make the harness agree with the
// defect by construction.

// ---- an independent populator ---------------------------------------------

// populatePairwise builds a fully populated value of t: every member non-empty,
// every pointer non-nil, every slice sliceLen long, every array full. Non-empty
// everywhere is what makes an `omitempty` toggle wire-identical — an omitted
// member is omitted on BOTH sides and the pair proves nothing.
//
// Values depend only on the KIND, never on a field name or position, so two
// variants of one fixture receive the same value at every corresponding
// position. Divergence in the wire bytes then means a real structural
// difference, which is precisely what step 1 has to detect.
func populatePairwise(t reflect.Type, sliceLen, hops int) (reflect.Value, bool) {
	if t == nil {
		return reflect.Value{}, false
	}
	if hops > 4*maxShapeDepth+64 {
		// Only a self-referential fixture can reach this, and none of the
		// generated ones is. It exists so a future fixture cannot hang the sweep.
		//
		// IT RETURNS FALSE, NOT A ZERO. A zero here would under-populate one side
		// silently, which is precisely how an `omitempty` pair degrades into
		// comparing two omissions and asserting nothing. False makes pwCheckPair
		// report the pair as INVALID and skip it out loud instead. The bound is
		// tied to maxShapeDepth because the placements nest that deep: a fixed 40
		// was already inside the reach of the cap placements once the cap moved.
		return reflect.Value{}, false
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem, ok := populatePairwise(t.Elem(), sliceLen, hops+1)
		if !ok {
			return reflect.Value{}, false
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(elem)
		if p.Type() != t {
			p = p.Convert(t)
		}
		return p, true

	case reflect.Struct:
		if fill, ok := pwOpaqueFills[t]; ok {
			// A type made only of UNEXPORTED fields. The ordinary walk below would
			// leave it at its ZERO, and a zero is exactly what `omitzero` drops —
			// so the member would vanish from ONE side's wire, the pair would be
			// reported not-wire-identical and SKIPPED, and the rule that exists to
			// reach these types would assert nothing at all. Same reason net.IP
			// carries a literal below: the harness must be able to build a value
			// the real encoder accepts and keeps.
			return fill(), true
		}
		v := reflect.New(t).Elem()
		for i := 0; i < t.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				// Every generated fixture is fully exported; an unexported member
				// would make the two sides incomparable rather than wrong.
				continue
			}
			sub, ok := populatePairwise(t.Field(i).Type, sliceLen, hops+1)
			if !ok {
				return reflect.Value{}, false
			}
			f.Set(sub)
		}
		return v, true

	case reflect.Slice:
		if t == jsonRawMessageType {
			return reflect.ValueOf(json.RawMessage("null")).Convert(t), true
		}
		if t == pwNetIPType {
			// net.IP is a []byte with a VALIDATING MarshalText: an arbitrary
			// one-byte fill is not a legal address and the marshal fails. The
			// harness has to be able to build a value the real encoder accepts,
			// or a type carrying one could never appear in either sweep.
			return reflect.ValueOf(net.IP{192, 0, 2, 1}).Convert(t), true
		}
		s := reflect.MakeSlice(t, sliceLen, sliceLen)
		for i := 0; i < sliceLen; i++ {
			elem, ok := populatePairwise(t.Elem(), sliceLen, hops+1)
			if !ok {
				return reflect.Value{}, false
			}
			s.Index(i).Set(elem)
		}
		return s, true

	case reflect.Array:
		v := reflect.New(t).Elem()
		for i := 0; i < t.Len(); i++ {
			elem, ok := populatePairwise(t.Elem(), sliceLen, hops+1)
			if !ok {
				return reflect.Value{}, false
			}
			v.Index(i).Set(elem)
		}
		return v, true

	case reflect.Map:
		// One entry under the key that renders as "1" whatever the key type, so
		// map[int]V and map[string]V produce byte-identical JSON.
		var key reflect.Value
		switch t.Key().Kind() {
		case reflect.String:
			key = reflect.ValueOf("1").Convert(t.Key())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			key = reflect.ValueOf(int64(1)).Convert(t.Key())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			key = reflect.ValueOf(uint64(1)).Convert(t.Key())
		default:
			return reflect.Value{}, false
		}
		val, ok := populatePairwise(t.Elem(), sliceLen, hops+1)
		if !ok {
			return reflect.Value{}, false
		}
		m := reflect.MakeMap(t)
		m.SetMapIndex(key, val)
		return m, true

	case reflect.String:
		if t == jsonNumberType {
			return reflect.ValueOf(json.Number("1")).Convert(t), true
		}
		return reflect.ValueOf("x").Convert(t), true
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(t), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflect.ValueOf(int64(1)).Convert(t), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return reflect.ValueOf(uint64(1)).Convert(t), true
	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(1.5).Convert(t), true
	case reflect.Interface:
		// AN INTERFACE MEMBER IS POPULATED WITH A CONCRETE VALUE. A nil interface
		// is EMPTY to encoding/json, so leaving it zero would make `omitempty` drop
		// it on BOTH sides and the omitempty rule would compare two omissions and
		// assert nothing — the precise vacuity this harness's self-check exists to
		// prevent, and the reason the interface false fire went unseen for five
		// revisions. The value depends only on the KIND, like every other case
		// here, so two variants of one fixture receive identical content.
		if t.NumMethod() != 0 {
			// No fabricable value satisfies a method set; the pair is reported
			// INVALID and skipped out loud rather than silently under-populated.
			return reflect.Value{}, false
		}
		v := reflect.New(t).Elem()
		v.Set(reflect.ValueOf(map[string]any{"k": "v"}))
		return v, true
	default:
		return reflect.Zero(t), true
	}
}

// ---- fixtures the rules are applied to -------------------------------------

type pwLeaf struct {
	S string  `json:"s"`
	N int     `json:"n"`
	B bool    `json:"b"`
	F float64 `json:"f"`
}

type pwOmitLeaf struct {
	S  string  `json:"s,omitempty"`
	N  int     `json:"n,omitempty"`
	Sl []int   `json:"sl,omitempty"`
	P  *pwLeaf `json:"p,omitempty"`
}

type pwMid struct {
	Leaf   pwLeaf            `json:"leaf"`
	Items  []pwLeaf          `json:"items"`
	ByName map[string]pwLeaf `json:"by_name"`
	Tag    string            `json:"tag"`
}

var pwNetIPType = reflect.TypeOf(net.IP(nil))

// ---- members the PROBE cannot populate --------------------------------------
//
// Every fixture above is a scalar, slice, map, pointer or ordinary struct, and
// `build` populates all of them non-zero. That is what made the whole
// `member-omitzero-toggled` family unreachable: `omitempty` is INERT on a struct
// member, so the existing toggle rule cannot express the hazard, and no fixture
// existed whose member the probe leaves at its ZERO.
//
// These three are the common shapes of that member — a type made only of
// unexported fields, whose wire form comes from a marshaler. `build` cannot set
// unexported fields, so it probes them at the zero value; `omitzero` then drops
// exactly what the probe produced, while production, which never carries a zero
// timestamp, always emits it.
//
// They are given to the omitzero rule only, not folded into structFixtures: every
// other rule would acquire a new pair set in the same change, and a sweep whose
// coverage moves for two reasons at once cannot attribute a failure to either.
type PwOpaque struct {
	Created time.Time  `json:"created"`
	Peer    netip.Addr `json:"peer"`
	Amount  big.Int    `json:"amount"`
	N       int        `json:"n"`
}

// pwOpaqueFills gives the harness a POPULATED value for each — reflect cannot
// reach their fields either, so the populator needs a literal per type, exactly
// as it already does for net.IP.
var pwOpaqueFills = map[reflect.Type]func() reflect.Value{
	reflect.TypeOf(time.Time{}): func() reflect.Value {
		return reflect.ValueOf(time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC))
	},
	reflect.TypeOf(netip.Addr{}): func() reflect.Value {
		return reflect.ValueOf(netip.MustParseAddr("10.0.0.1"))
	},
	reflect.TypeOf(big.Int{}): func() reflect.Value {
		return reflect.ValueOf(*big.NewInt(42))
	},
}

// Named types whose underlying type is a wire-identical stand-in for them.
type PwName string
type PwCount int
type PwList []pwLeaf
type PwDict map[string]int

// A group of members that can be embedded untagged or inlined; the two are
// byte-identical because encoding/json PROMOTES an untagged embed's members
// straight into the parent object.
type PwGroup struct {
	G1 string `json:"g1"`
	G2 int    `json:"g2"`
}

// ---- pair generation --------------------------------------------------------

type pwPair struct {
	rule     string
	name     string
	a, b     reflect.Type
	sliceLen int
}

func pwFields(t reflect.Type) []reflect.StructField {
	out := make([]reflect.StructField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		f.Index = nil
		out = append(out, f)
	}
	return out
}

// pwRetag rewrites field i's json tag.
func pwRetag(fields []reflect.StructField, i int, tag string) []reflect.StructField {
	out := append([]reflect.StructField(nil), fields...)
	out[i].Tag = reflect.StructTag(tag)
	return out
}

// pwJSONName returns a field's json name and its option list.
func pwJSONName(f reflect.StructField) (string, []string) {
	tag := f.Tag.Get("json")
	name, rest, _ := strings.Cut(tag, ",")
	if name == "" {
		name = f.Name
	}
	var opts []string
	if rest != "" {
		opts = strings.Split(rest, ",")
	}
	return name, opts
}

func pwHasOpt(opts []string, want string) bool {
	for _, o := range opts {
		if o == want {
			return true
		}
	}
	return false
}

// pwUnderlying returns t's underlying type — the type a `type X = ...`-style
// de-naming would leave behind. Renaming or un-naming a type cannot move a byte.
func pwUnderlying(t reflect.Type) (reflect.Type, bool) {
	switch t.Kind() {
	case reflect.String:
		return reflect.TypeOf(""), true
	case reflect.Int:
		return reflect.TypeOf(int(0)), true
	case reflect.Int64:
		return reflect.TypeOf(int64(0)), true
	case reflect.Float64:
		return reflect.TypeOf(float64(0)), true
	case reflect.Bool:
		return reflect.TypeOf(false), true
	case reflect.Slice:
		return reflect.SliceOf(t.Elem()), true
	case reflect.Map:
		return reflect.MapOf(t.Key(), t.Elem()), true
	case reflect.Struct:
		return reflect.StructOf(pwFields(t)), true
	default:
		return nil, false
	}
}

// pwRulesMustContribute is the CANONICAL list of the rules this sweep covers,
// written down here rather than derived from whatever the generator happened to
// hand back.
//
// THAT DISTINCTION IS THE WHOLE POINT. The per-rule accounting below used to
// build its list of rules FROM the pairs (`for _, p := range pairs { rules[p.rule]
// = true }`), so it could only ever audit a rule that had already produced at
// least one pair. A rule that generated NOTHING never entered the map and was
// reported by no one: neutering the `member-omitempty-toggled` loop with a single
// `continue` removed 108 armed subtests — the rule this file's header calls
// "exactly the rev-3 and rev-4 leak", and the only rule that toggles `omitempty`
// on a populated member — and `go test ./pkg/call/` still printed `ok`. A
// generator loop whose filter widens, a fixture list that loses an entry, a
// refactor of structFixtures: any of them silently deletes a rule, and the sweep
// that exists to stop a false-fire family from shipping quietly stops covering
// one.
//
// Registering a rule here is therefore part of adding one. The accounting reports
// both directions — a listed rule that generated nothing, and a generated rule
// that is not listed — so neither the list nor the generator can drift alone.
var pwRulesMustContribute = []string{
	"array-vs-slice",
	"embed-ptr-vs-embed",
	"embed-ptr-vs-inlined",
	"embed-vs-inlined",
	"member-T-vs-ptrT",
	"member-array-vs-slice",
	"member-named-vs-underlying",
	"member-omitempty-toggled",
	"member-omitzero-toggled",
	"member-slice-elem-T-vs-ptrT",
	"named-vs-underlying",
	"result-T-vs-ptrT",
	"slice-elem-T-vs-ptrT",
}

// pwGeneratePairs builds every (typeA, typeB) the rules can produce. Each pair is
// a claim: "these two serialize identically". Step 1 of the test verifies that
// claim before anything is asserted on it.
//
// EVERY RULE IT IMPLEMENTS MUST BE LISTED IN pwRulesMustContribute.
func pwGeneratePairs() []pwPair {
	var pairs []pwPair
	add := func(rule, name string, a, b reflect.Type, sliceLen int) {
		pairs = append(pairs, pwPair{rule: rule, name: name, a: a, b: b, sliceLen: sliceLen})
	}

	structFixtures := []reflect.Type{
		reflect.TypeOf(pwLeaf{}),
		reflect.TypeOf(pwOmitLeaf{}),
		reflect.TypeOf(pwMid{}),
	}
	allFixtures := append([]reflect.Type{}, structFixtures...)
	allFixtures = append(allFixtures,
		reflect.TypeOf(PwName("")),
		reflect.TypeOf(PwCount(0)),
		reflect.TypeOf(PwList(nil)),
		reflect.TypeOf(PwDict(nil)),
	)

	// RULE: the RESULT TYPE itself, T <-> *T. encoding/json dereferences, so a
	// handler that starts returning *T instead of T cannot move a byte.
	for _, ft := range allFixtures {
		add("result-T-vs-ptrT", ft.String(), ft, reflect.PointerTo(ft), 1)
	}

	// RULE: a MEMBER, T <-> *T.
	for _, ft := range structFixtures {
		fields := pwFields(ft)
		for i, f := range fields {
			if f.Type.Kind() == reflect.Pointer || f.Anonymous {
				continue
			}
			mod := append([]reflect.StructField(nil), fields...)
			mod[i].Type = reflect.PointerTo(f.Type)
			add("member-T-vs-ptrT",
				fmt.Sprintf("%s.%s", ft.Name(), f.Name),
				ft, reflect.StructOf(mod), 1)
		}
	}

	// RULE: []T <-> []*T, as the result type and as a member.
	for _, ft := range structFixtures {
		add("slice-elem-T-vs-ptrT", "[]"+ft.Name(),
			reflect.SliceOf(ft), reflect.SliceOf(reflect.PointerTo(ft)), 1)
	}
	for _, ft := range structFixtures {
		fields := pwFields(ft)
		for i, f := range fields {
			if f.Type.Kind() != reflect.Slice || f.Type.Elem().Kind() == reflect.Pointer {
				continue
			}
			mod := append([]reflect.StructField(nil), fields...)
			mod[i].Type = reflect.SliceOf(reflect.PointerTo(f.Type.Elem()))
			add("member-slice-elem-T-vs-ptrT",
				fmt.Sprintf("%s.%s", ft.Name(), f.Name),
				ft, reflect.StructOf(mod), 1)
		}
	}

	// RULE: `omitempty` toggled on a POPULATED member. A populated value is never
	// omitted, so both sides emit the member and the wire is identical. This is
	// exactly the rev-3 and rev-4 leak: the option changes nothing on the wire but
	// changes what a SUBSTITUTED value does, and the substitution is what the
	// shape reads.
	for _, ft := range structFixtures {
		fields := pwFields(ft)
		for i, f := range fields {
			if f.Anonymous {
				continue
			}
			name, opts := pwJSONName(f)
			var withOpt, withoutOpt string
			if pwHasOpt(opts, "omitempty") {
				withOpt = fmt.Sprintf("json:%q", name+",omitempty")
				withoutOpt = fmt.Sprintf("json:%q", name)
			} else {
				withOpt = fmt.Sprintf("json:%q", name+",omitempty")
				withoutOpt = fmt.Sprintf("json:%q", name)
			}
			a := reflect.StructOf(pwRetag(fields, i, withoutOpt))
			b := reflect.StructOf(pwRetag(fields, i, withOpt))
			add("member-omitempty-toggled",
				fmt.Sprintf("%s.%s", ft.Name(), f.Name), a, b, 1)
		}
	}

	// RULE: `omitzero` toggled on a member. Go 1.24 added the option, and it is
	// NOT a second spelling of `omitempty`: it drops a member whose value is the
	// ZERO of its type, or whose `IsZero()` reports true — which `omitempty`, that
	// only ever looks at emptiness, never does for a struct.
	//
	// That difference is the whole reason this rule exists separately.
	// `member-omitempty-toggled` above is INERT on a struct member, so no amount
	// of fixtures could have reached the hazard through it; here a fully populated
	// value is never dropped by either side — so the wire is identical, exactly as
	// in the omitempty rule — while the PROBE's value may be, which is what moves
	// the shape.
	//
	// PwOpaque is included precisely because build leaves its members at their
	// zero. On the ordinary fixtures the rule is armed on both sides and asserts
	// equal shapes; on PwOpaque the omitzero side must record NO shape, which
	// replay skips.
	for _, ft := range append(append([]reflect.Type{}, structFixtures...), reflect.TypeOf(PwOpaque{})) {
		fields := pwFields(ft)
		for i, f := range fields {
			if f.Anonymous {
				continue
			}
			name, _ := pwJSONName(f)
			a := reflect.StructOf(pwRetag(fields, i, fmt.Sprintf("json:%q", name)))
			b := reflect.StructOf(pwRetag(fields, i, fmt.Sprintf("json:%q", name+",omitzero")))
			add("member-omitzero-toggled",
				fmt.Sprintf("%s.%s", ft.Name(), f.Name), a, b, 1)
		}
	}

	// RULE: members grouped into an UNTAGGED EMBED <-> the same members inlined.
	// json promotes the embed's members into the parent object, so the two are
	// byte-identical; "extract these fields into a shared struct" is one of the
	// most common refactors there is.
	group := reflect.TypeOf(PwGroup{})
	extra := reflect.StructField{Name: "Extra", Type: reflect.TypeOf(0), Tag: `json:"extra"`}
	embedded := reflect.StructOf([]reflect.StructField{
		{Name: group.Name(), Type: group, Anonymous: true},
		extra,
	})
	inlined := reflect.StructOf(append(pwFields(group), extra))
	add("embed-vs-inlined", "PwGroup", embedded, inlined, 1)

	// The same with a POINTER embed: json promotes through a non-nil embedded
	// pointer too.
	embeddedPtr := reflect.StructOf([]reflect.StructField{
		{Name: group.Name(), Type: reflect.PointerTo(group), Anonymous: true},
		extra,
	})
	add("embed-ptr-vs-inlined", "PwGroup", embeddedPtr, inlined, 1)
	add("embed-ptr-vs-embed", "PwGroup", embeddedPtr, embedded, 1)

	// RULE: a NAMED type <-> its underlying type. The fingerprint is documented as
	// STRUCTURAL, never nominal, so naming or un-naming a type must not move it.
	for _, ft := range allFixtures {
		u, ok := pwUnderlying(ft)
		if !ok || u == ft {
			continue
		}
		add("named-vs-underlying", ft.Name(), ft, u, 1)
		// and as a member, where the name sits one level in.
		holderA := reflect.StructOf([]reflect.StructField{{Name: "X", Type: ft, Tag: `json:"x"`}})
		holderB := reflect.StructOf([]reflect.StructField{{Name: "X", Type: u, Tag: `json:"x"`}})
		add("member-named-vs-underlying", ft.Name(), holderA, holderB, 1)
	}

	// RULE: a fixed-size array [N]T <-> []T holding N elements. Both emit a
	// JSON array of N elements; widening a fixed array to a slice is the single
	// most common evolution of such a field.
	const arrN = 2
	for _, ft := range append([]reflect.Type{reflect.TypeOf(float64(0)), reflect.TypeOf("")}, structFixtures...) {
		add("array-vs-slice", "["+fmt.Sprint(arrN)+"]"+ft.Name(),
			reflect.ArrayOf(arrN, ft), reflect.SliceOf(ft), arrN)
		holderA := reflect.StructOf([]reflect.StructField{{Name: "X", Type: reflect.ArrayOf(arrN, ft), Tag: `json:"x"`}})
		holderB := reflect.StructOf([]reflect.StructField{{Name: "X", Type: reflect.SliceOf(ft), Tag: `json:"x"`}})
		add("member-array-vs-slice", "["+fmt.Sprint(arrN)+"]"+ft.Name(), holderA, holderB, arrN)
	}

	return pairs
}

// ---- placements -------------------------------------------------------------
//
// EVERY defect of the last four revisions failed at a BOUNDARY, never at the top
// level, so the same pair is re-asserted at eight positions. The depth ones
// straddle maxShapeDepth deliberately: level 6 is where a member of the placed
// type is truncated, which is the boundary rev 4 false-fired at.

type pwPlacement struct {
	name string
	wrap func(reflect.Type) reflect.Type
}

func pwNestN(t reflect.Type, n int) reflect.Type {
	for i := 0; i < n; i++ {
		t = reflect.StructOf([]reflect.StructField{{Name: "F", Type: t, Tag: `json:"f"`}})
	}
	return t
}

func pwPlacements() []pwPlacement {
	return []pwPlacement{
		{"top-level", func(t reflect.Type) reflect.Type { return t }},
		{"nested-1", func(t reflect.Type) reflect.Type { return pwNestN(t, 1) }},
		{"nested-2", func(t reflect.Type) reflect.Type { return pwNestN(t, 2) }},
		{"slice-element", func(t reflect.Type) reflect.Type {
			return reflect.StructOf([]reflect.StructField{
				{Name: "Items", Type: reflect.SliceOf(t), Tag: `json:"items"`}})
		}},
		{"map-value", func(t reflect.Type) reflect.Type {
			return reflect.StructOf([]reflect.StructField{
				{Name: "M", Type: reflect.MapOf(reflect.TypeOf(""), t), Tag: `json:"m"`}})
		}},
		// The cap placements are computed from maxShapeDepth rather than
		// hardcoded, so raising the constant cannot silently move the sweep off
		// the only region where anything has ever failed. Written as literals
		// they were 5/6/7 against a cap of 6; the cap is now 32 and those numbers
		// would test nothing at all.
		//
		// A fixture placed at cap-3 is still fully explored; at cap-1 and beyond
		// its members are past the budget and the WHOLE type records no shape.
		// Both sides of every pair go empty together, which is the fail-open
		// symmetry that makes the boundary unable to false-fire — the sweep
		// asserts it here and the per-rule accounting below reports how many
		// pairs are green for that reason, so it can never be mistaken for
		// coverage.
		{"depth-cap-minus-3", func(t reflect.Type) reflect.Type { return pwNestN(t, maxShapeDepth-3) }},
		{"depth-cap-minus-1", func(t reflect.Type) reflect.Type { return pwNestN(t, maxShapeDepth-1) }},
		{"depth-at-cap", func(t reflect.Type) reflect.Type { return pwNestN(t, maxShapeDepth) }},
		{"depth-past-cap", func(t reflect.Type) reflect.Type { return pwNestN(t, maxShapeDepth+1) }},
	}
}

// pwRuleNames collects the DISTINCT rule names a generator actually emitted, in
// no particular order. Used only as one half of the audit — never as the list of
// rules to audit, which is the bug this pair of helpers exists to close.
func pwRuleNames[P interface{ ruleName() string }](pairs []P) map[string]bool {
	out := map[string]bool{}
	for _, p := range pairs {
		out[p.ruleName()] = true
	}
	return out
}

func (p pwPair) ruleName() string      { return p.rule }
func (p pwIfacePair) ruleName() string { return p.rule }

// pwAuditedRules reconciles the canonical rule list against what the generator
// emitted and returns the sorted union to report on, plus the set that really
// produced pairs.
//
// It reports BOTH failures, because each one is a different way for the sweep to
// go quietly vacuous:
//
//   - a CANONICAL rule that generated nothing — the generator lost it, and every
//     subtest it should have armed is gone with no other signal;
//   - a GENERATED rule that is not canonical — a rule was added without being
//     registered, so nothing would notice if IT later stopped generating.
func pwAuditedRules(t *testing.T, canonical []string, generated map[string]bool) ([]string, map[string]bool) {
	t.Helper()
	listed := map[string]bool{}
	names := make([]string, 0, len(canonical)+len(generated))
	for _, r := range canonical {
		if listed[r] {
			continue // a duplicated entry is cosmetic; report it once
		}
		listed[r] = true
		names = append(names, r)
		if !generated[r] {
			t.Errorf("rule %q generated NO pairs at all: the generator emits nothing for it and "+
				"every subtest it should have armed is gone. Accounting derived from the pairs "+
				"cannot see this — the rule name simply never appears — which is why the list is "+
				"written down separately. Either restore the generator or delete the rule from "+
				"the canonical list deliberately", r)
		}
	}
	for r := range generated {
		if listed[r] {
			continue
		}
		names = append(names, r)
		t.Errorf("rule %q produced pairs but is not on the canonical rule list; add it there, or "+
			"nothing will notice when it stops producing any", r)
	}
	sort.Strings(names)
	return names, generated
}

// ---- the sweep --------------------------------------------------------------

type pwOutcome int

const (
	pwEqual pwOutcome = iota
	pwDiverged
	pwNotWireIdentical
	pwUnmarshalable
	// Exactly ONE side records a shape. Replay compares only when BOTH the
	// persisted shape and the replaying type's shape are non-empty (call.go), so
	// neither direction of the deploy can be refused and this is NOT a false
	// fire. It is not coverage either — half the pair is unguarded — so it is
	// counted separately and cannot satisfy the armed-pair requirement below.
	pwOneSideRecordsNoShape
)

// pwClassify turns two fingerprints into an outcome. It is a function of its own,
// rather than three lines inside pwCheckPair, so the ONE relaxation in this
// harness can be tested over every combination directly: a fixture can only ever
// exhibit the combinations the current code happens to produce, and the failure
// mode to guard against is the branch widening to swallow a real divergence.
func pwClassify(fpA, fpB string) pwOutcome {
	if (fpA == "") != (fpB == "") {
		return pwOneSideRecordsNoShape
	}
	if fpA != fpB {
		return pwDiverged
	}
	return pwEqual
}

// pwCheckPair runs the three steps in order and returns what happened.
func pwCheckPair(a, b reflect.Type, sliceLen int) (out pwOutcome, wire string, shapeA, shapeB string, detail string) {
	va, oka := populatePairwise(a, sliceLen, 0)
	vb, okb := populatePairwise(b, sliceLen, 0)
	if !oka || !okb {
		return pwUnmarshalable, "", "", "", "the harness cannot populate one side"
	}
	ba, erra := json.Marshal(va.Interface())
	bb, errb := json.Marshal(vb.Interface())
	if erra != nil || errb != nil {
		return pwUnmarshalable, "", "", "", fmt.Sprintf("marshal a=%v b=%v", erra, errb)
	}
	shapeA = ResultShapeStringForTest(a)
	shapeB = ResultShapeStringForTest(b)
	if !bytes.Equal(ba, bb) {
		return pwNotWireIdentical, "", shapeA, shapeB, fmt.Sprintf("a=%s\n  b=%s", ba, bb)
	}
	wire = string(ba)
	return pwClassify(ResultFingerprintForTest(a), ResultFingerprintForTest(b)),
		wire, shapeA, shapeB, ""
}

func TestResultShape_PairwiseWireIdenticalTypesFingerprintIdentically(t *testing.T) {
	pairs := pwGeneratePairs()
	if len(pairs) == 0 {
		t.Fatal("the generator produced no pairs; the sweep would be vacuous")
	}
	placements := pwPlacements()

	// Per-rule accounting. A rule that never produces a VALID (wire-identical)
	// pair asserts nothing, and a sweep that quietly degrades to nothing is how
	// this repo has shipped shape tests before.
	valid := map[string]int{}
	invalid := map[string]int{}
	bothEmpty := map[string]int{}
	oneEmpty := map[string]int{}

	for _, p := range pairs {
		for _, pl := range placements {
			a := pl.wrap(p.a)
			b := pl.wrap(p.b)
			name := fmt.Sprintf("%s/%s/%s", p.rule, p.name, pl.name)
			t.Run(name, func(t *testing.T) {
				out, wire, shapeA, shapeB, detail := pwCheckPair(a, b, p.sliceLen)
				switch out {
				case pwUnmarshalable, pwNotWireIdentical:
					invalid[p.rule]++
					// NOT an assertion. Two types that serialize differently are
					// ALLOWED to fingerprint differently, so this pair proves
					// nothing and must not be reported as a finding.
					t.Skipf("INVALID PAIR (skipped, not a finding): the two sides are not "+
						"wire-identical, so nothing about the fingerprint follows.\n  %s", detail)
				case pwDiverged:
					valid[p.rule]++
					t.Errorf("FALSE FIRE: wire-identical types fingerprint DIFFERENTLY, so a "+
						"deploy swapping one for the other is refused on replay although it "+
						"cannot move a byte.\n"+
						"  rule:   %s\n"+
						"  pair:   %s  <->  %s\n"+
						"  wire:   %s\n"+
						"  shapeA: %s\n"+
						"  shapeB: %s",
						p.rule, a, b, wire, shapeA, shapeB)
				case pwOneSideRecordsNoShape:
					// NOT an assertion either, and NOT a finding: with one shape
					// empty the guard is skipped in BOTH directions of the deploy,
					// so nothing can be refused. Recorded so the accounting can
					// refuse to count it as coverage.
					valid[p.rule]++
					oneEmpty[p.rule]++
					t.Logf("HALF-ARMED (not a finding): one side records no shape, so replay "+
						"skips the check in both directions.\n  wire:   %s\n  shapeA: %q\n  shapeB: %q",
						wire, shapeA, shapeB)
				case pwEqual:
					valid[p.rule]++
					if shapeA == "" && shapeB == "" {
						bothEmpty[p.rule]++
					}
				}
			})
		}
	}

	t.Run("every-rule-contributed-a-valid-pair", func(t *testing.T) {
		names, generated := pwAuditedRules(t, pwRulesMustContribute, pwRuleNames(pairs))
		for _, r := range names {
			t.Logf("rule %-32s valid=%d skipped-not-wire-identical=%d both-sides-record-no-shape=%d one-side-records-no-shape=%d",
				r, valid[r], invalid[r], bothEmpty[r], oneEmpty[r])
			if !generated[r] {
				continue // already reported by pwAuditedRules; the counts below are all zero
			}
			if valid[r] == 0 {
				t.Errorf("rule %q produced no valid (wire-identical) pair at any placement, so it "+
					"asserts nothing; a silently vacuous rule is worse than no rule", r)
			}
			// A PAIR WHERE NEITHER SIDE RECORDS A SHAPE IS GREEN FOR FREE. Past
			// the depth cap both sides fail open to "", so their fingerprints
			// agree trivially — a true statement about the fail-open symmetry,
			// but not evidence that the rule's edit is wire-neutral where the
			// guard is actually ARMED. Every rule must therefore land at least
			// one pair with a real shape on both sides. Without this, raising
			// maxShapeDepth far enough that every placement fell past it would
			// turn the whole sweep green while testing nothing.
			// A HALF-ARMED PAIR IS FREE IN THE SAME WAY. One empty shape makes
			// replay skip in both directions, so the pair cannot fail whatever the
			// other side says. Counting it as coverage would let a rule whose every
			// pair disarmed one side look fully exercised.
			if valid[r] == bothEmpty[r]+oneEmpty[r] {
				t.Errorf("every valid pair for rule %q is green only because at least one side "+
					"records NO shape (%d both-empty, %d one-empty of %d); the rule never "+
					"exercises an armed guard and asserts nothing",
					r, bothEmpty[r], oneEmpty[r], valid[r])
			}
		}
	})
}

// ---- the harness checking itself --------------------------------------------
//
// The sweep above skipped nothing: every generated pair really was wire-identical.
// That is the intended outcome, but it also means step 1 never fired, and a gate
// that never fires is indistinguishable from a gate that CANNOT fire. If step 1
// were broken open, the sweep would start reporting pairs that serialize
// differently as false fires — garbage findings, which is exactly how a previous
// round wasted itself. So the gate is exercised directly here.
//
// The populator is pinned for the same reason. `omitempty` only drops an EMPTY
// value, so if populatePairwise ever produced a zero anywhere, the omitempty rule
// would silently degrade to comparing two types that both omit the member — 96
// green subtests asserting nothing at all.

type pwRenamed struct {
	S2 string  `json:"s2"`
	N  int     `json:"n"`
	B  bool    `json:"b"`
	F  float64 `json:"f"`
}

func TestResultShape_PairwiseHarnessSelfCheck(t *testing.T) {
	t.Run("step 1 rejects a pair that is not wire-identical", func(t *testing.T) {
		out, _, _, _, detail := pwCheckPair(reflect.TypeOf(pwLeaf{}), reflect.TypeOf(pwRenamed{}), 1)
		if out != pwNotWireIdentical {
			t.Fatalf("renaming a json member changes the wire, so the pair must be rejected as "+
				"invalid before any fingerprint claim is made; got outcome %d (%s)", out, detail)
		}
	})

	t.Run("step 1 accepts a pair that is wire-identical", func(t *testing.T) {
		out, wire, _, _, detail := pwCheckPair(
			reflect.TypeOf(pwLeaf{}), reflect.PointerTo(reflect.TypeOf(pwLeaf{})), 1)
		if out == pwNotWireIdentical || out == pwUnmarshalable {
			t.Fatalf("T and *T serialize identically, so the pair must be accepted: %s", detail)
		}
		if wire == "" {
			t.Fatal("an accepted pair must report the shared wire bytes")
		}
	})

	// THE ASYMMETRIC FAIL-OPEN IS A RELAXATION OF STEP 2, so it is pinned from
	// both sides here. Reporting "one side records no shape" instead of a
	// divergence is correct only because call.go compares nothing unless BOTH
	// shapes are non-empty — which
	// TestResultShape_OmitzeroDeployReplaysTheCheckpointProductionWrote and
	// TestResultShape_PastTheDepthCapIsFailOpenInProduction drive through the real
	// Call in both directions. If the relaxation ever widened to cover two
	// non-empty shapes, the sweep would stop detecting the family it exists for.
	t.Run("a pair with ONE empty shape is reported as half-armed, not as a divergence", func(t *testing.T) {
		// PwOpaque.Created carries a member the probe leaves at its zero, so the
		// omitzero side records no shape while the plain side records one.
		fields := pwFields(reflect.TypeOf(PwOpaque{}))
		plain := reflect.StructOf(pwRetag(fields, 0, `json:"created"`))
		omit := reflect.StructOf(pwRetag(fields, 0, `json:"created,omitzero"`))
		if s := ResultShapeStringForTest(plain); s == "" {
			t.Fatalf("FIXTURE BROKEN: the plain side must record a shape, got %q", s)
		}
		if s := ResultShapeStringForTest(omit); s != "" {
			t.Fatalf("FIXTURE BROKEN: the omitzero side must record no shape, got %q", s)
		}
		out, _, _, _, detail := pwCheckPair(plain, omit, 1)
		if out != pwOneSideRecordsNoShape {
			t.Fatalf("with one shape empty replay skips in both directions, so this is not a "+
				"divergence; got outcome %d (%s)", out, detail)
		}
	})

	t.Run("the relaxation covers exactly the empty cases and nothing else", func(t *testing.T) {
		// The branch itself, over every combination it can see. A widening of it —
		// "either empty OR merely different" — is what would silently switch the
		// detector off, and no fixture could show that, because a fixture can only
		// exhibit the combinations the current code produces.
		for _, c := range []struct {
			fpA, fpB string
			want     pwOutcome
		}{
			{"aa", "aa", pwEqual},
			{"", "", pwEqual},
			{"aa", "bb", pwDiverged},
			{"aa", "", pwOneSideRecordsNoShape},
			{"", "aa", pwOneSideRecordsNoShape},
		} {
			if got := pwClassify(c.fpA, c.fpB); got != c.want {
				t.Errorf("pwClassify(%q, %q) = %d, want %d", c.fpA, c.fpB, got, c.want)
			}
		}
	})

	t.Run("the populator leaves nothing empty, so omitempty is never vacuously satisfied", func(t *testing.T) {
		// Every member of pwOmitLeaf carries omitempty, and every one is of a kind
		// json will drop when empty. If any key is missing here, the whole
		// omitempty rule is comparing two omissions.
		v, ok := populatePairwise(reflect.TypeOf(pwOmitLeaf{}), 1, 0)
		if !ok {
			t.Fatal("the populator cannot build pwOmitLeaf")
		}
		b, err := json.Marshal(v.Interface())
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		for _, k := range []string{"s", "n", "sl", "p"} {
			if _, present := m[k]; !present {
				t.Errorf("populated pwOmitLeaf omits %q (%s); the omitempty rule would be "+
					"comparing two omissions and asserting nothing", k, b)
			}
		}
	})

	t.Run("the populator fills interface members, so the interface rules are not vacuous", func(t *testing.T) {
		// A nil interface is EMPTY to encoding/json. If the populator left one
		// zero, every interface `omitempty` toggle would drop the member on both
		// sides and the whole interface sweep would assert nothing — which is
		// exactly how the interface false fire survived five revisions.
		v, ok := populatePairwise(reflect.TypeOf(pwIfaceOmitProbe{}), 1, 0)
		if !ok {
			t.Fatal("the populator cannot build pwIfaceOmitProbe")
		}
		b, err := json.Marshal(v.Interface())
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal %s: %v", b, err)
		}
		if _, present := m["meta"]; !present {
			t.Fatalf("a populated `any` member carrying omitempty is missing from %s; the "+
				"interface rules would be comparing two omissions", b)
		}
	})

	t.Run("the placements really straddle the depth cap", func(t *testing.T) {
		// The rule the cap placements depend on. A slice member is used because
		// its explored form ([number]) is unmistakable and depends on neither
		// omitempty nor pointer boxing — the two mechanisms under investigation.
		//
		// pwCapSlice puts its element two JSON levels below its own position, so
		// placed at nesting cap-2 the element still fits and at cap-1 it does
		// not. If maxShapeDepth or any nesting site moves, this fails and the
		// placements above are re-derived rather than quietly testing nothing.
		inside := ResultShapeStringForTest(pwNestN(reflect.TypeOf(pwCapSlice{}), maxShapeDepth-2))
		if !strings.Contains(inside, "x:[number]") {
			t.Errorf("at nesting %d the member should still be explored, got %s", maxShapeDepth-2, inside)
		}
		// AND ONE PAST IT THERE IS NO SHAPE AT ALL — not a shorter shape, not a
		// truncated one. This is the property that retires the false-fire family:
		// nothing is substituted at the boundary, so no Go representation can
		// leak through encoding/json into the fingerprint.
		at := ResultShapeStringForTest(pwNestN(reflect.TypeOf(pwCapSlice{}), maxShapeDepth-1))
		if at != "" {
			t.Errorf("at nesting %d the type must record NO shape, got %s.\n"+
				"A partial shape at the cap means a value was substituted there, which is how "+
				"each of the last four revisions shipped a false fire", maxShapeDepth-1, at)
		}
		// And the deepest placements really are past the boundary even for the
		// shallowest fixture in the sweep — pwLeaf holds only scalars, so it
		// survives one level deeper than pwCapSlice does. Asserting BOTH halves
		// keeps "depth-cap-minus-1" honest as a placement that still carries real
		// content, and "depth-at-cap"/"depth-past-cap" honest as placements that
		// genuinely sit past the budget.
		if s := ResultShapeStringForTest(pwNestN(reflect.TypeOf(pwLeaf{}), maxShapeDepth-1)); s == "" {
			t.Errorf("a scalar-only fixture at nesting %d should still be explored; if it is not, "+
				"the depth-cap-minus-1 placement carries no content and asserts nothing", maxShapeDepth-1)
		}
		for _, n := range []int{maxShapeDepth, maxShapeDepth + 1} {
			if s := ResultShapeStringForTest(pwNestN(reflect.TypeOf(pwLeaf{}), n)); s != "" {
				t.Errorf("placement at nesting %d still records a shape (%s); the deep "+
					"placements are meant to sit past the budget", n, s)
			}
		}
	})
}

// ---- misses: reported, not fixed --------------------------------------------
//
// The other direction. Two types whose wire output DIFFERS but whose fingerprint
// is the SAME is a MISS: replay lets a real type change through and behaves
// exactly as it did before this guard existed. That is the acceptable direction —
// a miss leaves prior behaviour in place, a false fire wedges a live workflow —
// so this test REPORTS and never fails. The maintainer wants the list for the
// docs.

type pwOrderAB struct {
	A string `json:"a"`
	B string `json:"b"`
}
type pwOrderBA struct {
	B string `json:"b"`
	A string `json:"a"`
}
type pwNumInt struct {
	N int `json:"n"`
}
type pwNumFloat struct {
	N float64 `json:"n"`
}

type pwMissCandidate struct {
	name string
	typ  reflect.Type
	len  int
}

func TestResultShape_PairwiseMissesAreReportedNotAsserted(t *testing.T) {
	cands := []pwMissCandidate{
		{"pwLeaf", reflect.TypeOf(pwLeaf{}), 1},
		{"pwOmitLeaf", reflect.TypeOf(pwOmitLeaf{}), 1},
		{"pwMid", reflect.TypeOf(pwMid{}), 1},
		{"[2]float64", reflect.TypeOf([2]float64{}), 2},
		{"[3]float64", reflect.TypeOf([3]float64{}), 3},
		{"[]float64", reflect.TypeOf([]float64{}), 1},
		{"map[string]int", reflect.TypeOf(map[string]int{}), 1},
		{"map[int]int", reflect.TypeOf(map[int]int{}), 1},
		{"PwList", reflect.TypeOf(PwList{}), 1},
		// Member ORDER is part of the wire form (encoding/json emits fields in
		// declaration order) but describe() sorts member names, so a reorder is
		// invisible to the fingerprint.
		{"pwOrderAB", reflect.TypeOf(pwOrderAB{}), 1},
		{"pwOrderBA", reflect.TypeOf(pwOrderBA{}), 1},
		// int -> float64 changes the bytes (1 vs 1.5) but both are JSON "number".
		{"pwNumInt", reflect.TypeOf(pwNumInt{}), 1},
		{"pwNumFloat", reflect.TypeOf(pwNumFloat{}), 1},
		// A type containing a VALIDATING marshaler records no shape at all, so it
		// collides with every other such type. That is the documented accepted
		// miss; it is counted separately rather than listed, since the collision
		// is with "no guard" rather than with a specific other type.
		{"r27LeaseIP (validating marshaler)", reflect.TypeOf(r27LeaseIP{}), 1},
		{"r27LeaseIPChanged (validating marshaler)", reflect.TypeOf(r27LeaseIPChanged{}), 1},
	}
	// Wrap each candidate at a couple of depths too, since a miss can be created
	// by truncation as easily as by the type itself.
	var all []pwMissCandidate
	for _, c := range cands {
		all = append(all, c)
		all = append(all, pwMissCandidate{c.name + "@depth-6", pwNestN(c.typ, 6), c.len})
	}

	type entry struct {
		name  string
		wire  string
		fp    string
		shape string
	}
	var entries []entry
	for _, c := range all {
		v, ok := populatePairwise(c.typ, c.len, 0)
		if !ok {
			continue
		}
		b, err := json.Marshal(v.Interface())
		if err != nil {
			continue
		}
		entries = append(entries, entry{c.name, string(b), ResultFingerprintForTest(c.typ), ResultShapeStringForTest(c.typ)})
	}

	misses := 0
	emptyShape := 0
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			x, y := entries[i], entries[j]
			if x.fp != y.fp || x.wire == y.wire {
				continue
			}
			if x.fp == "" {
				emptyShape++
				continue
			}
			misses++
			t.Logf("MISS (acceptable, reported for the docs): different wire, same fingerprint\n"+
				"  %s: %s\n  %s: %s\n  shared shape: %s", x.name, x.wire, y.name, y.wire, x.shape)
		}
	}
	t.Logf("pairwise miss sweep: %d miss pairs, %d pairs colliding only because neither records a shape", misses, emptyShape)
}

// ---- the same property, driven through PRODUCTION ---------------------------
//
// The sweep above compares ResultFingerprintForTest, which is one call away from
// the real write path. This repo has repeatedly shipped shape tests that
// hand-seeded the very field they claimed to protect, so the headline pairs are
// re-run through writeThenReplay: Call[W] runs for real, PRODUCTION computes and
// persists the shape, and Call[R] replays that persisted checkpoint. A false
// fire shows up here as the determinism violation a real deploy would hit.

// pwCapPlain / pwCapPtr differ only in whether the member is boxed in a pointer.
// Populated, they are byte-identical. Wrapped six levels deep they were the pair
// that false-fired in revision 4, when the member landed exactly on a
// maxShapeDepth of 6 and `truncate` substituted a value there. Both the cap and
// the substitution are gone, so these now pin the DEEP-BUT-GUARDED region: six
// levels down is comfortably inside a cap of 32, the guard is armed, and the two
// wire-identical types must agree. The boundary itself is driven through
// production by TestResultShape_PastTheDepthCapIsFailOpenInProduction below,
// which needs no 32-level fixture because a self-referential type reaches the cap
// on its own.
type pwCapPlain struct {
	X int `json:"x,omitempty"`
}
type pwCapPtr struct {
	X *int `json:"x,omitempty"`
}
type pwCapNoOmit struct {
	X int `json:"x"`
}

// pwCapSlice is only used by the self-check that proves the cap placements
// really straddle maxShapeDepth.
type pwCapSlice struct {
	X []int `json:"x"`
}

type pwPlainL1 struct {
	F pwCapPlain `json:"f"`
}
type pwPlainL2 struct {
	F pwPlainL1 `json:"f"`
}
type pwPlainL3 struct {
	F pwPlainL2 `json:"f"`
}
type pwPlainL4 struct {
	F pwPlainL3 `json:"f"`
}
type pwPlainL5 struct {
	F pwPlainL4 `json:"f"`
}
type pwPlainL6 struct {
	F pwPlainL5 `json:"f"`
}

type pwPtrL1 struct {
	F pwCapPtr `json:"f"`
}
type pwPtrL2 struct {
	F pwPtrL1 `json:"f"`
}
type pwPtrL3 struct {
	F pwPtrL2 `json:"f"`
}
type pwPtrL4 struct {
	F pwPtrL3 `json:"f"`
}
type pwPtrL5 struct {
	F pwPtrL4 `json:"f"`
}
type pwPtrL6 struct {
	F pwPtrL5 `json:"f"`
}

type pwNoOmitL1 struct {
	F pwCapNoOmit `json:"f"`
}
type pwNoOmitL2 struct {
	F pwNoOmitL1 `json:"f"`
}
type pwNoOmitL3 struct {
	F pwNoOmitL2 `json:"f"`
}
type pwNoOmitL4 struct {
	F pwNoOmitL3 `json:"f"`
}
type pwNoOmitL5 struct {
	F pwNoOmitL4 `json:"f"`
}
type pwNoOmitL6 struct {
	F pwNoOmitL5 `json:"f"`
}

// pwAssertWireIdentical is step 1 for the production-driven pairs: if the two
// values do not marshal to the same bytes there is no false fire to look for and
// the test would be asserting nonsense.
func pwAssertWireIdentical(t *testing.T, a, b any) {
	t.Helper()
	ba, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal %T: %v", a, err)
	}
	bb, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal %T: %v", b, err)
	}
	if !bytes.Equal(ba, bb) {
		t.Fatalf("the pair is not wire-identical, so it proves nothing about the fingerprint:\n  %T: %s\n  %T: %s",
			a, ba, b, bb)
	}
}

func pwHandlerReturning[R any](t *testing.T, v R) any {
	t.Helper()
	h, err := handler.NewHandler(func(_ context.Context, _ string) (R, error) { return v, nil })
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

// THE HEADLINE PRODUCTION-DRIVEN PAIR. `X int` and `X *int`, both with
// omitempty, six levels down. The wire is identical, so replay must accept it.
// Under revision 4 it did not: the member landed on the cap, truncate left the
// int zero (omitempty DROPPED it) and allocated through the pointer (omitempty
// KEPT it), the recorded shapes differed and replay refused a byte-identical
// deploy.
func TestResultShape_PairwiseProduction_PointerBoxingDeepButGuarded(t *testing.T) {
	plain := pwPlainL6{pwPlainL5{pwPlainL4{pwPlainL3{pwPlainL2{pwPlainL1{pwCapPlain{X: 1}}}}}}}
	one := 1
	ptr := pwPtrL6{pwPtrL5{pwPtrL4{pwPtrL3{pwPtrL2{pwPtrL1{pwCapPtr{X: &one}}}}}}}
	pwAssertWireIdentical(t, plain, ptr)

	h := pwHandlerReturning(t, plain)
	saved, _, err := writeThenReplay[pwPlainL6, pwPtrL6](t, h, "cap-ptr-box")
	if err != nil {
		t.Errorf("FALSE FIRE six levels down: boxing a member in a pointer cannot move a "+
			"byte, yet replay refused the checkpoint production wrote.\n"+
			"  persisted shape: %s\n"+
			"  shape(pwPlainL6): %s\n"+
			"  shape(pwPtrL6):   %s\n"+
			"  replay error: %v",
			saved.ResultShape,
			ResultShapeStringForTest(reflectTypeOf[pwPlainL6]()),
			ResultShapeStringForTest(reflectTypeOf[pwPtrL6]()),
			err)
	}
}

// The same depth reached by the OTHER wire-neutral edit: adding or removing
// `omitempty` on a member that is always populated.
func TestResultShape_PairwiseProduction_OmitemptyToggleDeepButGuarded(t *testing.T) {
	omit := pwPlainL6{pwPlainL5{pwPlainL4{pwPlainL3{pwPlainL2{pwPlainL1{pwCapPlain{X: 1}}}}}}}
	plain := pwNoOmitL6{pwNoOmitL5{pwNoOmitL4{pwNoOmitL3{pwNoOmitL2{pwNoOmitL1{pwCapNoOmit{X: 1}}}}}}}
	pwAssertWireIdentical(t, omit, plain)

	h := pwHandlerReturning(t, omit)
	saved, _, err := writeThenReplay[pwPlainL6, pwNoOmitL6](t, h, "cap-omitempty")
	if err != nil {
		t.Errorf("FALSE FIRE six levels down: adding or removing `omitempty` on a member "+
			"that is always populated cannot move a byte, yet replay refused the checkpoint "+
			"production wrote.\n"+
			"  persisted shape: %s\n"+
			"  shape(omitempty): %s\n"+
			"  shape(plain):     %s\n"+
			"  replay error: %v",
			saved.ResultShape,
			ResultShapeStringForTest(reflectTypeOf[pwPlainL6]()),
			ResultShapeStringForTest(reflectTypeOf[pwNoOmitL6]()),
			err)
	}
}

// A CONTROL. The same edit at the TOP level is already handled, and this pair
// passing is what shows the two tests above are detecting a boundary defect
// rather than a broken harness.
func TestResultShape_PairwiseProduction_PointerBoxingAtTopLevelIsFine(t *testing.T) {
	one := 1
	pwAssertWireIdentical(t, pwCapPlain{X: 1}, pwCapPtr{X: &one})
	h := pwHandlerReturning(t, pwCapPlain{X: 1})
	if _, _, err := writeThenReplay[pwCapPlain, pwCapPtr](t, h, "top-ptr-box"); err != nil {
		t.Errorf("boxing a top-level member in a pointer is wire-neutral and must replay: %v", err)
	}
}

// ---- the BOUNDARY itself, driven through production --------------------------
//
// The pair above sits deep but inside the budget. This one sits PAST it, and it
// needs no 32-level fixture to get there: a self-referential type nests one JSON
// level per hop and so reaches any cap on its own, which is the case the cap
// exists for in the first place.
//
// The property is that failing open is SYMMETRIC and total. Production writes an
// empty shape, replay into a type whose members are genuinely DIFFERENT is
// accepted, and no error is produced in either direction. That is an accepted
// MISS — the type is not guarded — and it is stated as such rather than sold as
// coverage. A miss leaves prior behaviour in place; a false fire wedges a live
// workflow.

type pwDeepNode struct {
	Label string      `json:"label"`
	Next  *pwDeepNode `json:"next,omitempty"`
}

type pwDeepNodeChanged struct {
	Ref  int                `json:"ref"`
	Next *pwDeepNodeChanged `json:"next,omitempty"`
}

func TestResultShape_PastTheDepthCapIsFailOpenInProduction(t *testing.T) {
	if s := ResultShapeStringForTest(reflect.TypeOf(pwDeepNode{})); s != "" {
		t.Fatalf("a self-referential type nests past maxShapeDepth and must record NO shape, got %s", s)
	}

	h := pwHandlerReturning(t, pwDeepNode{Label: "root"})
	saved, _, err := writeThenReplay[pwDeepNode, pwDeepNodeChanged](t, h, "past-cap")
	if err != nil {
		t.Errorf("a type past the depth cap records no shape, so replay must SKIP the check "+
			"and never refuse: %v", err)
	}
	if saved.ResultShape != "" {
		t.Errorf("production persisted %q for a type past the cap; it must persist nothing, "+
			"or a later deploy could be compared against it", saved.ResultShape)
	}

	// The other direction of the same deploy, so the skip is symmetric rather
	// than an accident of which side happens to be empty.
	h2 := pwHandlerReturning(t, pwDeepNodeChanged{Ref: 1})
	if _, _, err := writeThenReplay[pwDeepNodeChanged, pwDeepNode](t, h2, "past-cap-rev"); err != nil {
		t.Errorf("the reverse deploy must be skipped too: %v", err)
	}
}

// ---- INTERFACE VARIANTS -----------------------------------------------------
//
// The sweep above generates wire-neutral edits of fixtures made entirely of
// concrete types, and that is precisely why it could not see the oldest defect in
// this file: none of pwLeaf/pwOmitLeaf/pwMid declares an interface member, so the
// `member-omitempty-toggled` rule never toggled `omitempty` on one.
//
// It was a live false fire at nesting depth 0. `build`'s `case reflect.Interface`
// substituted a nil interface and handed it to encoding/json; isEmptyValue calls a
// nil interface EMPTY, so `omitempty` DROPPED the member while its absence
// recorded `null`. Adding `,omitempty` to a `Meta any` that production always
// populates cannot move a byte, and it moved the fingerprint.
//
// An interface member now disarms the WHOLE type: no shape at all, replay skips.
// So these rules assert something STRONGER than the sweep above — not "the two
// fingerprints agree" but "BOTH sides record no shape" — because agreeing at some
// non-empty value would mean a value was substituted again, which is the whole
// defect. Equal fingerprints follow from it, and a pair that ends up equal for a
// DIFFERENT reason (both past the depth cap) cannot be mistaken for coverage.
//
// Each rule carries a CONTROL: the same type with the interface swapped for a
// concrete `map[string]string`. If the control records no shape either, the
// placement is past the depth cap and proves nothing about interfaces, so the
// pair is not counted as armed. The four placements the maintainer named — top
// level, at depth, inside a slice element, inside a map value — are REQUIRED to
// be armed, or this sweep fails as vacuous.

// PwAny is `any` under a different name: a NAMED empty interface. The fingerprint
// is documented as structural and never nominal, so the two must behave alike.
type PwAny interface{}

var (
	pwAnyType       = reflect.TypeOf((*any)(nil)).Elem()
	pwNamedAnyType  = reflect.TypeOf((*PwAny)(nil)).Elem()
	pwConcreteMeta  = reflect.TypeOf(map[string]string(nil))
	pwStringType    = reflect.TypeOf("")
	pwIntType       = reflect.TypeOf(0)
	pwStringPtrType = reflect.PointerTo(reflect.TypeOf(""))
)

// pwIfaceOmitProbe is the self-check's fixture: an `any` member carrying
// omitempty, which a nil interface would make vanish.
type pwIfaceOmitProbe struct {
	Meta any    `json:"meta,omitempty"`
	Ref  string `json:"ref"`
}

// pwMetaStruct builds `struct{Meta <metaType>; Ref <refType>; N int}` with the
// given tags — the realistic shape of a result type carrying free-form metadata
// alongside ordinary members.
func pwMetaStruct(metaType reflect.Type, metaTag string, refType reflect.Type, nTag string) reflect.Type {
	return reflect.StructOf([]reflect.StructField{
		{Name: "Meta", Type: metaType, Tag: reflect.StructTag(metaTag)},
		{Name: "Ref", Type: refType, Tag: `json:"ref"`},
		{Name: "N", Type: pwIntType, Tag: reflect.StructTag(nTag)},
	})
}

type pwIfacePair struct {
	rule string
	name string
	a, b reflect.Type
	// control is a, with every interface replaced by a concrete type. It records
	// a shape wherever the placement is inside the depth budget, which is how a
	// placement proves it is ARMED rather than merely past the cap.
	control  reflect.Type
	sliceLen int
}

// pwIfaceRulesMustContribute is the canonical list for the interface sweep, kept
// for exactly the reason pwRulesMustContribute is: this accounting derived its
// rules from the pairs too, so a rule that generated nothing was invisible here
// as well. EVERY RULE pwGenerateInterfacePairs IMPLEMENTS MUST BE LISTED.
var pwIfaceRulesMustContribute = []string{
	"iface-any-vs-named-interface",
	"iface-behind-embed-omitempty-toggled",
	"iface-behind-pointer-omitempty-toggled",
	"iface-map-value",
	"iface-member-omitempty-toggled",
	"iface-named-member-omitempty-toggled",
	"iface-result-T-vs-ptrT",
	"iface-result-any-vs-named",
	"iface-sibling-T-vs-ptrT",
	"iface-sibling-omitempty-toggled",
	"iface-slice-element",
}

func pwGenerateInterfacePairs() []pwIfacePair {
	var out []pwIfacePair
	add := func(rule, name string, a, b, control reflect.Type) {
		out = append(out, pwIfacePair{rule: rule, name: name, a: a, b: b, control: control, sliceLen: 1})
	}

	const plainMeta = `json:"meta"`
	const omitMeta = `json:"meta,omitempty"`
	const plainN = `json:"n"`
	const omitN = `json:"n,omitempty"`

	base := pwMetaStruct(pwAnyType, plainMeta, pwStringType, plainN)
	control := pwMetaStruct(pwConcreteMeta, plainMeta, pwStringType, plainN)

	// THE HEADLINE. `Meta any `json:"meta"`` -> `Meta any `json:"meta,omitempty"``.
	// Byte-identical on a populated value; this is the edit that wedged replay.
	add("iface-member-omitempty-toggled", "Meta",
		base, pwMetaStruct(pwAnyType, omitMeta, pwStringType, plainN), control)

	// The same edit on a NAMED empty interface, since the fingerprint is
	// structural and never nominal.
	add("iface-named-member-omitempty-toggled", "Meta PwAny",
		pwMetaStruct(pwNamedAnyType, plainMeta, pwStringType, plainN),
		pwMetaStruct(pwNamedAnyType, omitMeta, pwStringType, plainN), control)

	// `any` <-> a named empty interface: renaming a type cannot move a byte.
	add("iface-any-vs-named-interface", "Meta",
		base, pwMetaStruct(pwNamedAnyType, plainMeta, pwStringType, plainN), control)

	// ONE interface member disarms the whole type, so wire-neutral edits to its
	// SIBLINGS are unguarded too. That is the coverage this policy costs, and it
	// is asserted rather than described.
	add("iface-sibling-T-vs-ptrT", "Ref",
		base, pwMetaStruct(pwAnyType, plainMeta, pwStringPtrType, plainN), control)
	add("iface-sibling-omitempty-toggled", "N",
		base, pwMetaStruct(pwAnyType, plainMeta, pwStringType, omitN), control)

	// THE RESULT TYPE ITSELF is an interface: Call[any].
	add("iface-result-any-vs-named", "any <-> PwAny", pwAnyType, pwNamedAnyType, pwConcreteMeta)
	add("iface-result-T-vs-ptrT", "any <-> *any",
		pwAnyType, reflect.PointerTo(pwAnyType), pwConcreteMeta)

	// INSIDE A SLICE ELEMENT and INSIDE A MAP VALUE, directly rather than through
	// a placement, so the interface is the element/value type itself.
	add("iface-slice-element", "[]any",
		reflect.SliceOf(pwAnyType), reflect.SliceOf(pwNamedAnyType), reflect.SliceOf(pwConcreteMeta))
	add("iface-map-value", "map[string]any",
		reflect.MapOf(pwStringType, pwAnyType), reflect.MapOf(pwStringType, pwNamedAnyType),
		reflect.MapOf(pwStringType, pwConcreteMeta))

	// Reached through a POINTER, and through an untagged EMBED that promotes into
	// the parent — the two hops that spend no depth budget, so neither can be
	// confused with the cap.
	embedded := reflect.StructOf([]reflect.StructField{
		{Name: "PwGroup", Type: reflect.TypeOf(PwGroup{}), Anonymous: true},
		{Name: "Meta", Type: pwAnyType, Tag: reflect.StructTag(plainMeta)},
	})
	embeddedOmit := reflect.StructOf([]reflect.StructField{
		{Name: "PwGroup", Type: reflect.TypeOf(PwGroup{}), Anonymous: true},
		{Name: "Meta", Type: pwAnyType, Tag: reflect.StructTag(omitMeta)},
	})
	embeddedControl := reflect.StructOf([]reflect.StructField{
		{Name: "PwGroup", Type: reflect.TypeOf(PwGroup{}), Anonymous: true},
		{Name: "Meta", Type: pwConcreteMeta, Tag: reflect.StructTag(plainMeta)},
	})
	add("iface-behind-embed-omitempty-toggled", "PwGroup+Meta", embedded, embeddedOmit, embeddedControl)
	add("iface-behind-pointer-omitempty-toggled", "*struct{Meta any}",
		reflect.PointerTo(base),
		reflect.PointerTo(pwMetaStruct(pwAnyType, omitMeta, pwStringType, plainN)),
		reflect.PointerTo(control))

	return out
}

// pwPlacementsMustBeArmed are the positions the policy is claimed to hold at.
// If the control at one of them records no shape, the sweep is testing the depth
// cap rather than the interface rule and says so instead of passing.
var pwPlacementsMustBeArmed = map[string]bool{
	"top-level":     true,
	"nested-2":      true,
	"slice-element": true,
	"map-value":     true,
}

func TestResultShape_PairwiseInterfaceVariantsRecordNoShape(t *testing.T) {
	pairs := pwGenerateInterfacePairs()
	if len(pairs) == 0 {
		t.Fatal("the generator produced no interface pairs; the sweep would be vacuous")
	}
	placements := pwPlacements()

	armed := map[string]int{}
	invalid := map[string]int{}
	armedPlacement := map[string]int{}

	for _, p := range pairs {
		for _, pl := range placements {
			a := pl.wrap(p.a)
			b := pl.wrap(p.b)
			ctrl := pl.wrap(p.control)
			name := fmt.Sprintf("%s/%s/%s", p.rule, p.name, pl.name)
			t.Run(name, func(t *testing.T) {
				// STEP 1, unchanged and still first: the pair must be
				// wire-identical on POPULATED values, or nothing about the
				// fingerprint follows from it.
				va, oka := populatePairwise(a, p.sliceLen, 0)
				vb, okb := populatePairwise(b, p.sliceLen, 0)
				if !oka || !okb {
					invalid[p.rule]++
					t.Skip("INVALID PAIR (skipped, not a finding): the harness cannot populate one side")
				}
				ba, erra := json.Marshal(va.Interface())
				bb, errb := json.Marshal(vb.Interface())
				if erra != nil || errb != nil {
					invalid[p.rule]++
					t.Skipf("INVALID PAIR (skipped, not a finding): marshal a=%v b=%v", erra, errb)
				}
				if !bytes.Equal(ba, bb) {
					invalid[p.rule]++
					t.Skipf("INVALID PAIR (skipped, not a finding): the two sides are not "+
						"wire-identical, so nothing about the fingerprint follows.\n  a=%s\n  b=%s", ba, bb)
				}

				// STEP 2: both sides must record NO SHAPE. Stronger than equality
				// on purpose — two equal NON-empty shapes would mean a value was
				// substituted for the interface again, which is the defect.
				shapeA := ResultShapeStringForTest(a)
				shapeB := ResultShapeStringForTest(b)
				if shapeA != "" || shapeB != "" {
					t.Fatalf("a type containing an interface must record NO shape, so replay "+
						"skips it; a shape here means a value was substituted for the interface "+
						"and its fate handed to encoding/json — the mechanism of every false fire "+
						"this file has shipped.\n  wire:   %s\n  shapeA: %q\n  shapeB: %q", ba, shapeA, shapeB)
				}
				if fa, fb := ResultFingerprintForTest(a), ResultFingerprintForTest(b); fa != "" || fb != "" {
					t.Fatalf("no shape must fingerprint empty, got %q and %q", fa, fb)
				}

				// STEP 3: is this placement ARMED, or is it merely past the depth
				// cap? The control answers it, and the answer is accounted for so
				// a cap change cannot turn this sweep green-but-empty.
				if ResultShapeStringForTest(ctrl) != "" {
					armed[p.rule]++
					armedPlacement[pl.name]++
				}
			})
		}
	}

	t.Run("every rule is armed somewhere, and at every placement the policy names", func(t *testing.T) {
		rules, generated := pwAuditedRules(t, pwIfaceRulesMustContribute, pwRuleNames(pairs))
		for _, r := range rules {
			t.Logf("rule %-42s armed=%d skipped-not-wire-identical=%d", r, armed[r], invalid[r])
			if !generated[r] {
				continue // already reported by pwAuditedRules; the counts below are all zero
			}
			if armed[r] == 0 {
				t.Errorf("rule %q is never armed: at every placement the CONTROL also records no "+
					"shape, so the rule is observing the depth cap rather than the interface "+
					"policy and asserts nothing about it", r)
			}
		}
		for pl := range pwPlacementsMustBeArmed {
			t.Logf("placement %-16s armed pairs=%d", pl, armedPlacement[pl])
			if armedPlacement[pl] == 0 {
				t.Errorf("placement %q is never armed; the policy is claimed to hold at top level, "+
					"at depth, inside a slice element and inside a map value, and this one is "+
					"testing nothing", pl)
			}
		}
	})
}

// THE CONCRETE PAYLOAD NEVER REACHES THE FINGERPRINT. The shape is a function of
// the static TYPE, so `any` holding a map and `any` holding a struct that
// marshals to the same bytes must behave identically — and, under the policy,
// both record nothing. This is the half of "an interface has no concrete value to
// inspect" that a type-level pair cannot express.
type pwIfaceHolder struct {
	Meta any    `json:"meta"`
	Ref  string `json:"ref"`
}

type pwIfacePayload struct {
	K string `json:"k"`
}

func TestResultShape_InterfaceConcretePayloadDoesNotReachTheFingerprint(t *testing.T) {
	withMap := pwIfaceHolder{Meta: map[string]any{"k": "v"}, Ref: "r"}
	withStruct := pwIfaceHolder{Meta: pwIfacePayload{K: "v"}, Ref: "r"}
	withPtr := pwIfaceHolder{Meta: &pwIfacePayload{K: "v"}, Ref: "r"}

	var wire string
	for i, v := range []pwIfaceHolder{withMap, withStruct, withPtr} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		if i == 0 {
			wire = string(b)
			continue
		}
		if string(b) != wire {
			t.Fatalf("payload %d is not wire-identical (%s vs %s), so the pair proves nothing",
				i, b, wire)
		}
	}

	// One static type, so one fingerprint whatever it carries — and under the
	// policy that fingerprint is empty.
	if s := ResultShapeStringForTest(reflect.TypeOf(pwIfaceHolder{})); s != "" {
		t.Fatalf("a struct with an `any` member must record no shape, got %q", s)
	}

	// Driven through production in both directions: the value written carries one
	// concrete type, the replay decodes into a different one, and replay must not
	// refuse — it is unguarded, which is the accepted miss.
	h := pwHandlerReturning(t, withStruct)
	saved, _, err := writeThenReplay[pwIfaceHolder, pwIfaceHolder](t, h, "iface-payload")
	if err != nil {
		t.Fatalf("an unguarded type must replay without complaint: %v", err)
	}
	if saved.ResultShape != "" {
		t.Fatalf("production persisted %q for a type with an interface member; it must persist "+
			"nothing, or a later deploy could be compared against it", saved.ResultShape)
	}
}
