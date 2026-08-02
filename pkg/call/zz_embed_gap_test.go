package call

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

type egInner struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// egState holds the Option's payload in PLAIN UNEXPORTED fields.
type egState struct {
	present bool
	value   egInner
}

// egOption embeds it ANONYMOUSLY and unexported — the route the first fix missed.
type egOption struct{ egState }

func (o egOption) MarshalJSON() ([]byte, error) {
	if !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

type egV1 struct {
	Opt egOption `json:"opt"`
	ID  string   `json:"id"`
}
type egV2 struct {
	Opt *egInner `json:"opt"`
	ID  string   `json:"id"`
}

// Mutually-embedded pointers are legal Go; hasUnpopulatedState must terminate.
// These are reached only through reflect, which `unused` cannot see — they are
// the fixture, not dead code.
//
//nolint:unused // walked via reflect.TypeOf below; the cycle IS the test
type egCycA struct{ *egCycB }

//nolint:unused // the other half of the cycle egCycA closes
type egCycB struct{ *egCycA }

//nolint:unused // walked via reflect.TypeOf below
type egCycHolder struct{ egCycA }

func TestZZ_EmbeddedUnexportedStateIsOpaqueToo(t *testing.T) {
	// Precondition: the two types are byte-identical on the wire.
	a, _ := json.Marshal(egV1{Opt: egOption{egState{present: true, value: egInner{A: 5, B: "x"}}}, ID: "o1"})
	inner := egInner{A: 5, B: "x"}
	b, _ := json.Marshal(egV2{Opt: &inner, ID: "o1"})
	if string(a) != string(b) {
		t.Fatalf("FIXTURE BROKEN: %s vs %s", a, b)
	}
	t.Logf("wire (identical): %s", a)

	s1 := ResultShapeStringForTest(reflect.TypeOf(egV1{}))
	s2 := ResultShapeStringForTest(reflect.TypeOf(egV2{}))
	t.Logf("shape(v1 embedded-unexported Option) = %q", s1)
	t.Logf("shape(v2 *egInner)                   = %q", s2)
	if s1 != "" {
		t.Errorf("a marshaler reading state inside an unexported EMBEDDED field is still opaque: "+
			"the probe cannot populate it, so the type must record NO shape, got %q", s1)
	}
}

// ---- the same state one indirection further out ------------------------------
//
// egOption above embeds its state BY VALUE, which the walk reaches without ever
// dereferencing anything. The Option/Maybe idiom is at least as often written
// with the state behind an unexported EMBEDDED POINTER — that is what makes the
// absent case a nil check rather than a flag — and reaching it needs
// hasUnpopulatedStateSeen's pointer deref.
//
// THAT DEREF WAS UNPINNED: deleting it left the entire pkg/call suite green,
// including the pairwise sweep and the parity table, because not one of their
// generated fixtures declares an unexported field, an embedded pointer or a
// json.Marshaler. With it deleted the walk stops at *egPtrState (a pointer is
// not a struct), the probe's `null` is recorded as the TYPE's shape, and the
// byte-identical simplification `egPtrOption -> *egInner` wedges a live replay.

type egPtrState struct {
	present bool
	value   egInner
}

// egPtrOption embeds it ANONYMOUSLY, unexported, and BEHIND A POINTER.
type egPtrOption struct{ *egPtrState }

func (o egPtrOption) MarshalJSON() ([]byte, error) {
	if o.egPtrState == nil || !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

type egPtrV1 struct {
	Opt egPtrOption `json:"opt"`
	ID  string      `json:"id"`
}
type egPtrV2 struct {
	Opt *egInner `json:"opt"`
	ID  string   `json:"id"`
}

func TestZZ_UnexportedEmbeddedPointerStateIsOpaqueToo(t *testing.T) {
	// Precondition, asserted rather than assumed, in BOTH the present and the
	// absent case: the deploy cannot move a byte.
	present := egPtrV1{Opt: egPtrOption{&egPtrState{present: true, value: egInner{A: 5, B: "x"}}}, ID: "o1"}
	inner := egInner{A: 5, B: "x"}
	a, _ := json.Marshal(present)
	b, _ := json.Marshal(egPtrV2{Opt: &inner, ID: "o1"})
	if string(a) != string(b) {
		t.Fatalf("FIXTURE BROKEN (present): %s vs %s", a, b)
	}
	absentA, _ := json.Marshal(egPtrV1{Opt: egPtrOption{&egPtrState{}}, ID: "o1"})
	absentB, _ := json.Marshal(egPtrV2{ID: "o1"})
	if string(absentA) != string(absentB) {
		t.Fatalf("FIXTURE BROKEN (absent): %s vs %s", absentA, absentB)
	}
	t.Logf("wire present (identical): %s", a)
	t.Logf("wire absent  (identical): %s", absentA)

	s1 := ResultShapeStringForTest(reflect.TypeOf(egPtrV1{}))
	s2 := ResultShapeStringForTest(reflect.TypeOf(egPtrV2{}))
	t.Logf("shape(v1 embedded-POINTER Option) = %q", s1)
	t.Logf("shape(v2 *egInner)                = %q", s2)
	if s1 != "" {
		t.Errorf("a marshaler reading state behind an unexported EMBEDDED POINTER is opaque too: "+
			"the probe cannot populate it, so the type must record NO shape, got %q", s1)
	}

	// And through the real Call, because a shape that is only read by a test is
	// not what wedges a workflow: production writes the checkpoint for the Option
	// form and the deploy replays it as *egInner.
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (egPtrV1, error) {
		return present, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[egPtrV1, egPtrV2](t, h, "opt-embed-ptr")
	if err != nil {
		t.Fatalf("FALSE FIRE: the deploy `egPtrOption -> *egInner` cannot move a byte (%s), yet "+
			"replay refused the checkpoint production wrote.\n  persisted shape: %q\n  error: %v",
			a, saved.ResultShape, err)
	}
	if got.Opt == nil || *got.Opt != inner || got.ID != "o1" {
		t.Fatalf("the replayed value must be the checkpointed one, got %+v", got)
	}
}

func TestZZ_MutuallyEmbeddedPointersTerminate(t *testing.T) {
	done := make(chan bool, 1)
	go func() {
		_ = hasUnpopulatedState(reflect.TypeOf(egCycHolder{}))
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("hasUnpopulatedState did not terminate on mutually-embedded pointer types")
	}
}

// R33NullTime is the textbook nullable wrapper: an EXPORTED embed of a type made
// entirely of unexported fields, with a marshaler that reads through it.
type R33NullTime struct{ time.Time }

func (n R33NullTime) MarshalJSON() ([]byte, error) {
	if n.IsZero() {
		return []byte("null"), nil
	}
	return json.Marshal(n.Time)
}

type r33V1 struct {
	At R33NullTime `json:"at"`
	ID string      `json:"id"`
}
type r33V2 struct {
	At *time.Time `json:"at"`
	ID string     `json:"id"`
}

// R33ExportedState is egState with its state type EXPORTED — one capital letter
// away from the fixture two tests above already pin.
type R33ExportedState struct {
	present bool
	value   egInner
}
type R33Option struct{ R33ExportedState }

func (o R33Option) MarshalJSON() ([]byte, error) {
	if !o.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.value)
}

type r33OptV1 struct {
	Opt R33Option `json:"opt"`
	ID  string    `json:"id"`
}

// r33NamedOpt reaches the same state through a NAMED exported member rather than
// an embed — the form the walker's own godoc used to name as a residual.
type r33NamedOpt struct {
	Opt R33ExportedState `json:"opt"`
}

func (o r33NamedOpt) MarshalJSON() ([]byte, error) {
	if !o.Opt.present {
		return []byte("null"), nil
	}
	return json.Marshal(o.Opt.value)
}

type r33NamedV1 struct {
	Opt r33NamedOpt `json:"opt"`
	ID  string      `json:"id"`
}

// TestZZ_ExportedEmbedIsOpaqueToo pins the three shapes an EXPORTED field can
// take while still hiding state the probe cannot populate.
//
// The walker skipped every exported field with "exported: build sets it
// outright" — true of the FIELD, false of what is inside it, which is the exact
// reasoning its own godoc rejects for the unexported case. All three record the
// shape of their ZERO, and each one's byte-identical simplification then
// hard-fails replay.
func TestZZ_ExportedEmbedIsOpaqueToo(t *testing.T) {
	at := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	a, _ := json.Marshal(r33V1{At: R33NullTime{at}, ID: "o1"})
	b, _ := json.Marshal(r33V2{At: &at, ID: "o1"})
	if string(a) != string(b) {
		t.Fatalf("FIXTURE BROKEN: %s vs %s", a, b)
	}
	t.Logf("wire (identical): %s", a)

	for name, typ := range map[string]reflect.Type{
		"exported embed of an unexported-state type": reflect.TypeOf(r33V1{}),
		"exported embedded state type":               reflect.TypeOf(r33OptV1{}),
		"named exported member":                      reflect.TypeOf(r33NamedV1{}),
	} {
		t.Run(name, func(t *testing.T) {
			if got := ResultShapeStringForTest(typ); got != "" {
				t.Errorf("a marshaler reading state the probe cannot populate must record NO shape "+
					"whether it reaches that state through an unexported field, an EXPORTED embed, or "+
					"an exported named member — got %q", got)
			}
		})
	}
}

// TestZZ_ExportedEmbedGuardDoesNotOverDisarm is the other half, and it is why the
// broad recursion is safe: probeSpeaksForType still trusts a probe whose wire form
// is a SCALAR, so an ordinary embed of time.Time keeps its shape.
func TestZZ_ExportedEmbedGuardDoesNotOverDisarm(t *testing.T) {
	if got := ResultShapeStringForTest(reflect.TypeOf(struct {
		time.Time
		X int `json:"x"`
	}{})); got == "" {
		t.Error("an embed whose promoted marshaler renders a SCALAR is describable and must stay " +
			"guarded; disarming it would trade a wedge for a needless blind spot")
	}
}

// r34FirstSeen hides time.Time's unexported state one ELEMENT hop out: an
// exported SLICE of a struct made entirely of unexported fields.
type r34FirstSeen struct {
	Samples []time.Time `json:"-"`
}

func (f r34FirstSeen) MarshalJSON() ([]byte, error) {
	for _, s := range f.Samples {
		if !s.IsZero() {
			return json.Marshal(s)
		}
	}
	return []byte("null"), nil
}

type r34V1 struct {
	At r34FirstSeen `json:"at"`
	ID string       `json:"id"`
}

// The same state reached through a slice element, a map value and an array
// element, using this package's OWN pinned Option fixture.
type r34SliceOpt struct{ S []R33ExportedState }
type r34MapOpt struct{ M map[string]R33ExportedState }
type r34ArrOpt struct{ A [1]R33ExportedState }

func (o r34SliceOpt) MarshalJSON() ([]byte, error) { return r34optJSON(o.S[0]) }
func (o r34MapOpt) MarshalJSON() ([]byte, error)   { return r34optJSON(o.M["k"]) }
func (o r34ArrOpt) MarshalJSON() ([]byte, error)   { return r34optJSON(o.A[0]) }

func r34optJSON(s R33ExportedState) ([]byte, error) {
	if !s.present {
		return []byte("null"), nil
	}
	return json.Marshal(s.value)
}

type r34SliceHolder struct {
	Opt r34SliceOpt `json:"opt"`
	ID  string      `json:"id"`
}
type r34MapHolder struct {
	Opt r34MapOpt `json:"opt"`
	ID  string    `json:"id"`
}
type r34ArrHolder struct {
	Opt r34ArrOpt `json:"opt"`
	ID  string    `json:"id"`
}

// TestZZ_ElementHopIsOpaqueToo pins the indirection round 33 did not follow.
//
// The walker followed a POINTER but stopped at a slice, array or map. build fills
// a container with ONE element, so if that element is a struct of unexported
// fields the state is exactly as unreachable as it would be inline — and a
// marshaler reading it recorded the shape of its ZERO. `FirstSeen -> *time.Time`
// then false-fired a replay it should have accepted.
//
// Four shapes, because the hole was one `Elem()` wide in each of them.
func TestZZ_ElementHopIsOpaqueToo(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"slice of unexported-state structs": reflect.TypeOf(r34V1{}),
		"slice element":                     reflect.TypeOf(r34SliceHolder{}),
		"map value":                         reflect.TypeOf(r34MapHolder{}),
		"array element":                     reflect.TypeOf(r34ArrHolder{}),
	} {
		t.Run(name, func(t *testing.T) {
			if got := ResultShapeStringForTest(typ); got != "" {
				t.Errorf("state the probe cannot populate is just as unreachable one element hop "+
					"out; this must record NO shape, got %q", got)
			}
		})
	}
}

// TestZZ_ElementHopDoesNotOverDisarm is the other half. Following the element type
// must not disarm a container whose element IS describable.
func TestZZ_ElementHopDoesNotOverDisarm(t *testing.T) {
	type plain struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	for name, typ := range map[string]reflect.Type{
		"slice of plain structs": reflect.TypeOf(struct {
			S []plain `json:"s"`
		}{}),
		"map of plain structs": reflect.TypeOf(struct {
			M map[string]plain `json:"m"`
		}{}),
	} {
		t.Run(name, func(t *testing.T) {
			if ResultShapeStringForTest(typ) == "" {
				t.Error("a container whose element is fully describable must stay guarded")
			}
		})
	}
}

// r35SelfPtr is `type T *T` — legal Go, and the shape that hangs an unwrap loop
// which has no bound of its own.
type r35SelfPtr *r35SelfPtr

// r35MutA / r35MutB are the mutual form, reachable without any struct.
type r35MutA *r35MutB
type r35MutB *r35MutA

// r35SelfSlice and r35SelfMap close the remaining container cycles.
type r35SelfSlice []r35SelfSlice
type r35SelfMap map[string]r35SelfMap

// TestZZ_UnwrapTerminatesOnIndirectionOnlyCycles pins termination for cycles made
// ENTIRELY of indirections.
//
// The previous termination test routed every self-reference through a STRUCT,
// which hasUnpopulatedStateSeen's visited set already handled — so it could not
// reach the unwrap loop's own cycle at all. These four types do, and an unbounded
// loop hangs on each: not a wrong answer but a HANG, inside Call, on the write and
// the replay path, where recover() cannot help.
func TestZZ_UnwrapTerminatesOnIndirectionOnlyCycles(t *testing.T) {
	types := map[string]reflect.Type{
		"self pointer":  reflect.TypeOf((*r35SelfPtr)(nil)).Elem(),
		"mutual A":      reflect.TypeOf((*r35MutA)(nil)).Elem(),
		"mutual B":      reflect.TypeOf((*r35MutB)(nil)).Elem(),
		"self slice":    reflect.TypeOf((*r35SelfSlice)(nil)).Elem(),
		"self map":      reflect.TypeOf((*r35SelfMap)(nil)).Elem(),
		"ptr to struct": reflect.TypeOf(&egInner{}),
	}
	for name, typ := range types {
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = hasUnpopulatedState(typ)
				_ = ResultShapeStringForTest(typ)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("hung: a cycle made only of indirections must terminate — this blocks Call " +
					"on both the write and the replay path, and recover() cannot catch it")
			}
		})
	}
}
