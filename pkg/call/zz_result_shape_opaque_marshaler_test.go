package call

import (
	"context"
	"encoding/json"
	"math/big"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---------------------------------------------------------------------------
// A MARSHALER THAT READS ONLY UNEXPORTED STATE FINGERPRINTS ITS ZERO
//
// build's struct case cannot set a plain unexported field, so the probe hands
// the encoder a value whose unexported state is the ZERO. A marshaler that reads
// only that state therefore describes the zero, and — unlike a VALIDATING
// marshaler, which rejects the probe and records no shape — this one SUCCEEDS.
// A shape is recorded, and it is the zero's structure presented as the type's.
//
// For the standard Option/Maybe idiom (unexported `present bool` + unexported
// value; samber/mo, moznion/go-optional and every hand-rolled equivalent) the
// probe is ABSENT, so the member shapes as `null`. The ordinary simplification
// `Option[Inner]` -> `*Inner` is byte-identical for every value — present emits
// the object, absent emits null — yet it fingerprints `{a:number,b:string}` and
// the replay is REFUSED. That is a false fire on a change that cannot move a
// byte, and it wedges a live workflow.
//
// Same mechanism, second wire form: a set backed by an unexported map that
// marshals as a sorted array probes as an EMPTY array, so its element shape is
// lost and the byte-identical `Set` -> `[]string` splits apart too.

// ---- the Option/Maybe idiom -------------------------------------------------

type omInner struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// omOption is the textbook Maybe: both fields unexported, a value-receiver
// MarshalJSON emitting null when absent and the payload when present.
type omOption[T any] struct {
	present bool
	value   T
}

func omSome[T any](v T) omOption[T] { return omOption[T]{present: true, value: v} }

func (o omOption[T]) MarshalJSON() ([]byte, error) {
	if !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

func (o *omOption[T]) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*o = omOption[T]{}
		return nil
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*o = omOption[T]{present: true, value: v}
	return nil
}

type omResultV1 struct {
	Opt omOption[omInner] `json:"opt"`
	ID  string            `json:"id"`
}

// The ordinary simplification. Byte-identical to omResultV1 for EVERY value.
type omResultV2 struct {
	Opt *omInner `json:"opt"`
	ID  string   `json:"id"`
}

// ---- a set backed by an unexported map --------------------------------------

type omSet struct {
	m map[string]struct{}
}

func omNewSet(items ...string) omSet {
	s := omSet{m: map[string]struct{}{}}
	for _, it := range items {
		s.m[it] = struct{}{}
	}
	return s
}

func (s omSet) MarshalJSON() ([]byte, error) {
	out := make([]string, 0, len(s.m))
	for k := range s.m {
		out = append(out, k)
	}
	sort.Strings(out)
	return json.Marshal(out)
}

func (s *omSet) UnmarshalJSON(b []byte) error {
	var items []string
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}
	*s = omNewSet(items...)
	return nil
}

type omTagsSet struct {
	Tags omSet  `json:"tags"`
	ID   string `json:"id"`
}
type omTagsSlice struct {
	Tags []string `json:"tags"`
	ID   string   `json:"id"`
}

// ---- THE FALSE FIRE, end to end through the real write and replay paths ------

func TestResultShape_OptionOverUnexportedStateDoesNotWedgeReplay(t *testing.T) {
	// The premise: these two really are wire-identical, in BOTH the present and
	// the absent case. If this stops holding the test below proves nothing.
	for _, c := range []struct {
		name string
		a, b any
	}{
		{"present", omResultV1{Opt: omSome(omInner{A: 5, B: "x"}), ID: "o1"},
			omResultV2{Opt: &omInner{A: 5, B: "x"}, ID: "o1"}},
		{"absent", omResultV1{ID: "o1"}, omResultV2{ID: "o1"}},
	} {
		ba, err := json.Marshal(c.a)
		if err != nil {
			t.Fatalf("%s: marshal v1: %v", c.name, err)
		}
		bb, err := json.Marshal(c.b)
		if err != nil {
			t.Fatalf("%s: marshal v2: %v", c.name, err)
		}
		if string(ba) != string(bb) {
			t.Fatalf("FIXTURE BROKEN: %s is not wire-identical:\n  v1=%s\n  v2=%s", c.name, ba, bb)
		}
	}

	h, herr := handler.NewHandler(func(_ context.Context, _ string) (omResultV1, error) {
		return omResultV1{Opt: omSome(omInner{A: 5, B: "x"}), ID: "o1"}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[omResultV1, omResultV2](t, h, "settle")
	if err != nil {
		t.Fatalf("a wire-identical omOption[omInner] -> *omInner refactor wedged the replay: %v\n"+
			"  persisted result %s shape %q\n  shape(v1)=%q\n  shape(v2)=%q",
			err, saved.Result, saved.ResultShape,
			ResultShapeStringForTest(reflectTypeOf[omResultV1]()),
			ResultShapeStringForTest(reflectTypeOf[omResultV2]()))
	}
	if got.Opt == nil || got.Opt.A != 5 || got.Opt.B != "x" || got.ID != "o1" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// The reverse direction, which the recorded-side skip alone does not cover: the
// checkpoint was written from the *omInner form (which records a real shape) and
// is replayed into the Option form.
func TestResultShape_PointerToOptionOverUnexportedStateDoesNotWedgeReplay(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (omResultV2, error) {
		return omResultV2{Opt: &omInner{A: 5, B: "x"}, ID: "o1"}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[omResultV2, omResultV1](t, h, "settle-rev")
	if err != nil {
		t.Fatalf("a wire-identical *omInner -> omOption[omInner] refactor wedged the replay: %v", err)
	}
	if !got.Opt.present || got.Opt.value.A != 5 || got.ID != "o1" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// The second wire form: an unexported map behind a marshaler probes as an EMPTY
// array, so the element shape is lost and `omSet` -> `[]string` splits apart.
func TestResultShape_SetOverUnexportedMapDoesNotWedgeReplay(t *testing.T) {
	a, err := json.Marshal(omTagsSet{Tags: omNewSet("a", "b"), ID: "i"})
	if err != nil {
		t.Fatalf("marshal set form: %v", err)
	}
	b, err := json.Marshal(omTagsSlice{Tags: []string{"a", "b"}, ID: "i"})
	if err != nil {
		t.Fatalf("marshal slice form: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("FIXTURE BROKEN: not wire-identical:\n  set=%s\n  slice=%s", a, b)
	}
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (omTagsSet, error) {
		return omTagsSet{Tags: omNewSet("a", "b"), ID: "i"}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[omTagsSet, omTagsSlice](t, h, "tags")
	if err != nil {
		t.Fatalf("a wire-identical omSet -> []string refactor wedged the replay: %v\n"+
			"  shape(set)=%q shape(slice)=%q", err,
			ResultShapeStringForTest(reflectTypeOf[omTagsSet]()),
			ResultShapeStringForTest(reflectTypeOf[omTagsSlice]()))
	}
	if len(got.Tags) != 2 || got.Tags[0] != "a" {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// The whole TYPE records no shape, not merely the offending member — the same
// fail-open every other boundary in result_fingerprint.go takes. Pinned so a
// later revision cannot quietly go back to substituting a value for the member.
func TestResultShape_OpaqueMarshalerRecordsNoShapeForTheWholeType(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
	}{
		{"Option member (zero marshals to null)", reflectTypeOf[omResultV1]()},
		{"Option behind a pointer", reflectTypeOf[struct {
			Opt *omOption[omInner] `json:"opt"`
		}]()},
		{"Option as the whole result", reflectTypeOf[omOption[omInner]]()},
		{"set member (zero marshals to [])", reflectTypeOf[omTagsSet]()},
	} {
		if s := ResultShapeStringForTest(tc.typ); s != "" {
			t.Errorf("%s: expected NO shape, got %q — the probe cannot populate the "+
				"unexported state this marshaler reads, so any shape it records is the "+
				"ZERO's structure presented as the type's", tc.name, s)
		}
	}
}

// ---- a POINTER-receiver marshaler over unexported state ---------------------
//
// encoding/json reaches a pointer-receiver marshaler only where it can take an
// address — a slice element, anything behind a pointer — so this one is invisible
// to a probe that only ever looks at the value form. It is the same hazard: at a
// slice position the marshaler runs over unexported state the probe could not
// set, and the element shapes as `null`.

type omPtrOption struct {
	present bool
	value   omInner
}

func (o *omPtrOption) MarshalJSON() ([]byte, error) {
	if o == nil || !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

func (o *omPtrOption) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*o = omPtrOption{}
		return nil
	}
	var v omInner
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*o = omPtrOption{present: true, value: v}
	return nil
}

type omPtrHolder struct {
	Opts []omPtrOption `json:"opts"`
	ID   string        `json:"id"`
}
type omPtrHolderSlice struct {
	Opts []*omInner `json:"opts"`
	ID   string     `json:"id"`
}

func TestResultShape_PointerReceiverOptionDoesNotWedgeReplay(t *testing.T) {
	a, err := json.Marshal(omPtrHolder{Opts: []omPtrOption{{present: true, value: omInner{A: 5, B: "x"}}}, ID: "p1"})
	if err != nil {
		t.Fatalf("marshal ptr-option form: %v", err)
	}
	b, err := json.Marshal(omPtrHolderSlice{Opts: []*omInner{{A: 5, B: "x"}}, ID: "p1"})
	if err != nil {
		t.Fatalf("marshal pointer form: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("FIXTURE BROKEN: not wire-identical:\n  opt=%s\n  ptr=%s", a, b)
	}
	if s := ResultShapeStringForTest(reflectTypeOf[omPtrHolder]()); s != "" {
		t.Errorf("a pointer-receiver marshaler over unexported state must record NO shape "+
			"(it is reachable at a slice element, where it describes the zero); got %q", s)
	}
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (omPtrHolder, error) {
		return omPtrHolder{Opts: []omPtrOption{{present: true, value: omInner{A: 5, B: "x"}}}, ID: "p1"}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[omPtrHolder, omPtrHolderSlice](t, h, "ptr-opt")
	if err != nil {
		t.Fatalf("a wire-identical []omPtrOption -> []*omInner refactor wedged the replay: %v", err)
	}
	if len(got.Opts) != 1 || got.Opts[0].A != 5 {
		t.Fatalf("the stored payload did not reconstruct: %+v", got)
	}
}

// ---- the third structured wire form: an OBJECT ------------------------------
//
// A tagged union whose ZERO emits `{"kind":"none"}` and whose populated values
// emit `{"kind":...,"value":{...}}`. The member set recorded from the probe is
// the absent case's, not the type's.

type omTagged struct {
	kind  string
	inner omInner
}

func (u omTagged) MarshalJSON() ([]byte, error) {
	if u.kind == "" {
		return []byte(`{"kind":"none"}`), nil
	}
	return json.Marshal(map[string]any{"kind": u.kind, "value": u.inner})
}

type omTaggedHolder struct {
	U  omTagged `json:"u"`
	ID string   `json:"id"`
}

func TestResultShape_ObjectFromAnOpaqueMarshalerRecordsNoShape(t *testing.T) {
	if s := ResultShapeStringForTest(reflectTypeOf[omTaggedHolder]()); s != "" {
		t.Fatalf("an object assembled from unexported state the probe could not populate "+
			"describes the ZERO's member set, not the type's; expected NO shape, got %q", s)
	}
}

// ---- an unexported EMBEDDED field IS populated, so it is not opaque ---------
//
// build reaches an unexported embedded field through its addressable storage
// (encoding/json promotes its members, so it must be populated like any other).
// A marshaler reading THAT state therefore describes a populated value and its
// shape is sound — which is why the opacity test skips anonymous fields.

type omEmbedBase struct {
	X int    `json:"x"`
	Y string `json:"y"`
}

type omEmbedded struct {
	omEmbedBase
}

func (e omEmbedded) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"x": e.X, "y": e.Y})
}

// ---- THE LINE: what must STILL be guarded -----------------------------------
//
// "Any type implementing json.Marshaler records no shape" would disarm every
// result carrying a time.Time, which guts the feature for ordinary code. The
// rule is narrower: a marshaler over unpopulatable unexported state is trusted
// when its wire form is a JSON SCALAR, because a scalar has no internal
// structure the probe could have got wrong — its shape IS its kind. An object,
// an array or null carries structure that depends on the state the probe could
// not set.

type omStamped struct {
	At   time.Time `json:"at"`
	Note string    `json:"note"`
}
type omStampedRenamed struct {
	At   time.Time `json:"at"`
	Memo string    `json:"memo"`
}
type omAddrHolder struct {
	IP   netip.Addr `json:"ip"`
	Note string     `json:"note"`
}

// A struct with unexported fields and NO marshaler: the unexported state cannot
// reach the wire at all, so the probe not populating it is irrelevant.
type omPlainUnexported struct {
	A      int `json:"a"`
	hidden string
}
type omPlainHolder struct {
	P    omPlainUnexported `json:"p"`
	Note string            `json:"note"`
}

// A marshaler over EXPORTED state: the probe populates it, so the wire form it
// produces really is the type's.
type omExportedMarshaler struct {
	V string `json:"v"`
}

func (e omExportedMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"v": e.V})
}

type omExportedHolder struct {
	E    omExportedMarshaler `json:"e"`
	Note string              `json:"note"`
}

// The two non-string scalars, so the rule is pinned across every scalar kind and
// not only the one time.Time happens to use.
type omCounter struct{ n int }

func (c omCounter) MarshalJSON() ([]byte, error) { return json.Marshal(c.n) }

type omFlag struct{ set bool }

func (f omFlag) MarshalJSON() ([]byte, error) { return json.Marshal(f.set) }

// A nullable string: the wire KIND flips with the exported state the probe DOES
// populate. It is the fixture that pins the check reading the value the probe
// built rather than a fresh zero of the same type — with a zero V it would
// marshal to null and be refused.
type omNullableString struct {
	V      string `json:"-"`
	hidden int
}

func (n omNullableString) MarshalJSON() ([]byte, error) {
	if n.V == "" {
		return []byte("null"), nil
	}
	return json.Marshal(n.V)
}

type omScalarHolder struct {
	K    omNullableString `json:"k"`
	C    omCounter        `json:"c"`
	F    omFlag           `json:"f"`
	Amt  *big.Int         `json:"amt"`
	Note string           `json:"note"`
}

type omEmbeddedHolder struct {
	E    omEmbedded `json:"e"`
	Note string     `json:"note"`
}

func TestResultShape_ScalarMarshalersStayGuarded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   reflect.Type
		shape string
	}{
		{"time.Time (zero marshals to a non-empty string)", reflectTypeOf[omStamped](),
			"{at:string,note:string}"},
		{"netip.Addr (zero marshals to the empty string, still a string)",
			reflectTypeOf[omAddrHolder](), "{ip:string,note:string}"},
		{"unexported state with no marshaler cannot reach the wire",
			reflectTypeOf[omPlainHolder](), "{note:string,p:{a:number}}"},
		{"a marshaler over exported state is fully probeable",
			reflectTypeOf[omExportedHolder](), "{e:{v:string},note:string}"},
		{"number and boolean scalars, and *big.Int (a pointer-receiver marshaler " +
			"whose zero is a number)", reflectTypeOf[omScalarHolder](),
			"{amt:number,c:number,f:bool,k:string,note:string}"},
		{"an unexported EMBEDDED field is populated, so a marshaler reading it is not opaque",
			reflectTypeOf[omEmbeddedHolder](), "{e:{x:number,y:string},note:string}"},
	} {
		if got := ResultShapeStringForTest(tc.typ); got != tc.shape {
			t.Errorf("%s: shape %q, want %q", tc.name, got, tc.shape)
		}
	}

	// The unexported members are read here rather than left to look like dead
	// weight: being present-but-unpopulatable IS what these two fixtures
	// contribute, and a `unused` finding would invite someone to delete them and
	// silently retire two of the rows above.
	if (omPlainUnexported{}).hidden != "" || (omNullableString{}).hidden != 0 {
		t.Fatal("FIXTURE BROKEN: the probe-unpopulatable members must be zero-valued")
	}

	// And the guard still FIRES for those types: a real member-set change to a
	// time.Time-carrying result is still refused.
	if a, b := fingerprintOf(omStamped{}), fingerprintOf(omStampedRenamed{}); a == "" || a == b {
		t.Fatalf("a renamed member of a time.Time-carrying result must still be caught; got %q / %q", a, b)
	}
}

// THE COST IS AN ACCEPTED MISS, and it is real: a result type carrying an
// Option-style member is now UNGUARDED ENTIRELY — its other members used to be
// compared and no longer are. Pinned so the trade is visible rather than
// discovered.
func TestResultShape_OpaqueMarshalerIsADeliberateAcceptedMiss(t *testing.T) {
	type before struct {
		Opt omOption[omInner] `json:"opt"`
		ID  string            `json:"id"`
	}
	type after struct {
		Opt   omOption[omInner] `json:"opt"`
		Total int               `json:"total"`
	}
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (before, error) {
		return before{ID: "o1"}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, _, err := writeThenReplay[before, after](t, h, "miss")
	if err != nil {
		t.Fatalf("DO NOT 'FIX' THIS by re-recording a shape for an opaque marshaler: it is "+
			"the accepted miss that buys the false fire back. Got %v", err)
	}
}

// The mechanism is UNEXPORTEDNESS, not the marshaler: the identical Option whose
// two fields are EXPORTED (and json:\"-\") is fully probeable and keeps a shape
// that matches its pointer partner.
type omOpenOption[T any] struct {
	Present bool `json:"-"`
	Value   T    `json:"-"`
}

func (o omOpenOption[T]) MarshalJSON() ([]byte, error) {
	if !o.Present {
		return []byte("null"), nil
	}
	return json.Marshal(o.Value)
}

func TestResultShape_ExportedOptionStateIsStillProbed(t *testing.T) {
	type openHolder struct {
		Opt omOpenOption[omInner] `json:"opt"`
		ID  string                `json:"id"`
	}
	got := ResultShapeStringForTest(reflectTypeOf[openHolder]())
	want := ResultShapeStringForTest(reflectTypeOf[omResultV2]())
	if got == "" || got != want {
		t.Fatalf("an Option whose state the probe CAN populate must keep the pointer form's "+
			"shape: got %q, *omInner form %q", got, want)
	}
	if !strings.Contains(got, "opt:{a:number,b:string}") {
		t.Fatalf("expected the populated member to show through, got %q", got)
	}
}
