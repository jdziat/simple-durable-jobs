package call

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---- an INTEGER-kind map key that declares MarshalText -----------------------
//
// The last member of the substitution family, and the one with the widest reach:
// `ByStatus map[Status]int`, the enum-keyed count map.
//
// encoding/json's resolveKeyName short-circuits on reflect.String FIRST and only
// then looks for an encoding.TextMarshaler, so a STRING-kind key is taken
// verbatim and never consults its marshaler, while an INTEGER-kind or UINT-kind
// one ALWAYS does — before the strconv branch is ever reached. synthesizeMapKey
// fabricates K(1); the user's marshaler renders it; and the resulting NAME landed
// in the shape verbatim. `{by_status:{active:number}}` — "active" only because
// that is what Status(1) happened to spell on the day the fingerprint was
// computed.
//
// A shape that is a function of a FABRICATED VALUE is not a shape. Inserting one
// constant at the FRONT of the same iota block cannot move a byte — every
// persisted key is written by NAME and every name stays attached to its state —
// and yet it moved the fingerprint and hard-failed replay with a statement that
// was provably untrue. Round 29 closed the sibling case by KIND; this one is
// invisible to a kind test, because int IS one of the kinds the switch renders.
//
// Same settled rule as every other boundary in result_fingerprint.go: such a type
// records NO SHAPE.
//
// THE COST IS AN ACCEPTED MISS, and a real one: a result type carrying such a map
// ANYWHERE in it is now unguarded ENTIRELY, not merely at the map — its other
// members used to be compared and no longer are, so a genuine change to them
// replays exactly as it did before this feature existed.
// TestResultShape_IntKindMapKeyMissIsDeliberate asserts that cost directly rather
// than leaving it implied, and UPGRADE.md lists it. It is still the cheap
// direction: a miss leaves prior behaviour in place, a false fire wedges a live
// workflow.

type r30Status int

// r30Table models the CONSTANT BLOCK behind the enum: code -> name. It is a
// variable rather than a const block only so a test can model a REDEPLOY that
// inserted a new state at the front of the iota run. Production code would spell
// this as `const ( StatusActive Status = iota + 1; ... )` plus a switch.
var r30Table = map[r30Status]string{1: "active", 2: "done"}

// r30TableShifted is the SAME enum after one constant is inserted at the front.
// Every name is still attached to its own state; only the numbers moved.
var r30TableShifted = map[r30Status]string{1: "pending", 2: "active", 3: "done"}

// A TOTAL marshaler: it accepts every value, including the probe's K(1). That is
// what separates this from round 27's VALIDATING map key (r27Zone), which
// REJECTS the probe and so already took the no-shape path via the marshal error.
func (s r30Status) MarshalText() ([]byte, error) { return []byte(r30Table[s]), nil }

func (s *r30Status) UnmarshalText(b []byte) error {
	for code, name := range r30Table {
		if name == string(b) {
			*s = code
			return nil
		}
	}
	return nil
}

// r30Code resolves a NAME to whatever code the CURRENTLY DEPLOYED table assigns
// it. Building the fixture's data this way is what makes the wire bytes identical
// across the deploy by construction rather than by luck.
func r30Code(t *testing.T, name string) r30Status {
	t.Helper()
	for code, n := range r30Table {
		if n == name {
			return code
		}
	}
	t.Fatalf("no status named %q in the deployed table %v", name, r30Table)
	return 0
}

type r30Report struct {
	ByStatus map[r30Status]int `json:"by_status"`
	Total    int               `json:"total"`
}

// r30SwapTable installs the after-deploy enum table and drops the memoized
// fingerprints, which is what a restarted process would see. It restores both on
// cleanup so the mutation cannot leak into another test.
func r30SwapTable(t *testing.T, table map[r30Status]string) {
	t.Helper()
	prev := r30Table
	r30Table = table
	r30ForgetShapes()
	t.Cleanup(func() {
		r30Table = prev
		r30ForgetShapes()
	})
}

func r30ForgetShapes() {
	for _, rt := range []reflect.Type{
		reflect.TypeOf(r30Report{}),
		reflect.TypeOf(map[r30Status]int(nil)),
	} {
		fingerprintCache.Delete(rt)
	}
}

// ---- the shape-level statement ----------------------------------------------

func TestResultShape_IntKindMapKeyWithMarshalTextRecordsNoShape(t *testing.T) {
	mapType := reflect.TypeOf(map[r30Status]int(nil))

	// STEP 1, THE PREMISE: the two enum tables really are wire-identical for the
	// same logical data. Without this the rest proves nothing.
	before, err := json.Marshal(r30Report{
		ByStatus: map[r30Status]int{r30Code(t, "active"): 7, r30Code(t, "done"): 2},
		Total:    9,
	})
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}
	shapeBefore := ResultShapeStringForTest(reflect.TypeOf(r30Report{}))
	mapShapeBefore := ResultShapeStringForTest(mapType)

	r30SwapTable(t, r30TableShifted)

	after, err := json.Marshal(r30Report{
		ByStatus: map[r30Status]int{r30Code(t, "active"): 7, r30Code(t, "done"): 2},
		Total:    9,
	})
	if err != nil {
		t.Fatalf("marshal after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("the enum edit is not wire-neutral (%s vs %s), so it proves nothing", before, after)
	}

	// STEP 2: neither side may record a shape. Equal-but-non-empty would mean a
	// fabricated key value is still being rendered into the shape — it would
	// merely happen to render the same on both sides today.
	shapeAfter := ResultShapeStringForTest(reflect.TypeOf(r30Report{}))
	mapShapeAfter := ResultShapeStringForTest(mapType)
	for name, got := range map[string]string{
		"r30Report before":         shapeBefore,
		"r30Report after":          shapeAfter,
		"map[r30Status]int before": mapShapeBefore,
		"map[r30Status]int after":  mapShapeAfter,
	} {
		if got != "" {
			t.Errorf("%s: an integer-kind map key that declares MarshalText must record NO shape, got %q.\n"+
				"encoding/json renders such a key through the marshaler, so the key NAME in the shape "+
				"is whatever MarshalText makes of the fabricated K(1) — a function of the enum's "+
				"numbering, not of the wire form. The wire is %s on both sides of the edit.",
				name, got, before)
		}
	}
}

// THE MISS, asserted rather than implied. A result type carrying the map is
// unguarded end to end: a handler whose result type really did change replays
// silently instead of raising a determinism violation. Stating it here is what
// keeps the trade honest — and what makes it visible if someone later narrows the
// rule and this turns red without anyone having decided to re-arm it.
type r30ReportRenamed struct {
	ByState map[r30Status]int `json:"by_state"`
	Sum     int               `json:"sum"`
}

func TestResultShape_IntKindMapKeyMissIsDeliberate(t *testing.T) {
	h, err := handler.NewHandler(func(_ context.Context, _ string) (r30Report, error) {
		return r30Report{ByStatus: map[r30Status]int{r30Code(t, "active"): 7}, Total: 7}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	saved := r30FirstRun(t, h, "report")
	if saved.ResultShape != "" {
		t.Fatalf("production recorded shape %q for a type whose map key is rendered by a "+
			"marshaler; it must record none", saved.ResultShape)
	}

	// A DIFFERENT result type entirely — different member names — and replay does
	// NOT complain, because neither side records a shape.
	got, err := r30Replay[r30ReportRenamed](t, h, "report", *saved)
	if err != nil {
		t.Fatalf("the miss must be silent, not an error: %v", err)
	}
	if got.Sum != 0 || len(got.ByState) != 0 {
		t.Fatalf("expected the changed type to decode to its zero (the miss), got %+v", got)
	}

	// THE CONTROL, so this is not green because the guard stopped working: the
	// same deploy on a type with no marshaler-rendered key is still CAUGHT.
	h2, err := handler.NewHandler(func(_ context.Context, _ string) (e2eV1, error) {
		return e2eV1{OrderID: "ord_1", Amount: 500}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	if _, _, err := writeThenReplay[e2eV1, e2eV2](t, h2, "settle"); err == nil {
		t.Fatal("CONTROL BROKEN: a changed result type with no marshaler-rendered map key must " +
			"still be refused on replay")
	}
}

// ---- the same statement driven through PRODUCTION ----------------------------
//
// The shape assertions above would still pass if the guard were computed but
// never consulted. This drives a real first-run Call, captures the checkpoint
// PRODUCTION wrote, redeploys the shifted enum table, and replays that captured
// checkpoint — the whole failure the finding describes, end to end.

func TestResultShape_EnumTableShiftIsNotRefusedOnReplay(t *testing.T) {
	newHandler := func() any {
		h, err := handler.NewHandler(func(_ context.Context, _ string) (r30Report, error) {
			return r30Report{
				ByStatus: map[r30Status]int{r30Code(t, "active"): 7, r30Code(t, "done"): 2},
				Total:    9,
			}, nil
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		return h
	}

	// FIRST RUN, on the deployed-today enum table.
	saved := r30FirstRun(t, newHandler(), "report")
	if saved.Result == nil {
		t.Fatal("production saved no result payload; the fixture proves nothing")
	}

	// THE DEPLOY: one constant inserted at the front of the iota block. The result
	// TYPE DECLARATION IS UNCHANGED.
	r30SwapTable(t, r30TableShifted)

	// PREMISE: the redeployed handler emits byte-identical JSON.
	afterRun := r30FirstRun(t, newHandler(), "report")
	if !bytes.Equal(saved.Result, afterRun.Result) {
		t.Fatalf("the deploy is not wire-neutral (%s vs %s), so it proves nothing",
			saved.Result, afterRun.Result)
	}

	// REPLAY of the pre-deploy checkpoint under the post-deploy binary.
	got, err := r30Replay[r30Report](t, newHandler(), "report", *saved)
	if err != nil {
		t.Fatalf("FALSE FIRE: replaying a checkpoint whose wire bytes (%s) are byte-identical to "+
			"what the redeployed handler emits, for an UNCHANGED result type, was refused: %v\n"+
			"persisted shape %q, shape now %q",
			saved.Result, err, saved.ResultShape, ResultFingerprintForTest(reflect.TypeOf(r30Report{})))
	}
	if got.Total != 9 || got.ByStatus[r30Code(t, "active")] != 7 {
		t.Fatalf("replay returned %+v, which is not the checkpointed result", got)
	}
}

func r30FirstRun(t *testing.T, h any, name string) *core.Checkpoint {
	t.Helper()
	var saved *core.Checkpoint
	jobCtx := &intctx.JobContext{
		Job:           &core.Job{ID: "job-r30"},
		HandlerLookup: func(string) (any, bool) { return h, true },
		SaveCheckpoint: func(_ context.Context, cp *core.Checkpoint) error {
			c := *cp
			saved = &c
			return nil
		},
	}
	ctx := intctx.WithCallState(intctx.WithJobContext(context.Background(), jobCtx), []core.Checkpoint{})
	if _, err := Call[r30Report](ctx, name, "arg"); err != nil {
		t.Fatalf("first-run Call error: %v", err)
	}
	if saved == nil {
		t.Fatal("expected a success checkpoint to be saved")
	}
	return saved
}

func r30Replay[R any](t *testing.T, h any, name string, cp core.Checkpoint) (R, error) {
	t.Helper()
	jobCtx := &intctx.JobContext{
		Job:            &core.Job{ID: "job-r30"},
		HandlerLookup:  func(string) (any, bool) { return h, true },
		SaveCheckpoint: func(context.Context, *core.Checkpoint) error { return nil },
	}
	ctx := intctx.WithCallState(intctx.WithJobContext(context.Background(), jobCtx), []core.Checkpoint{cp})
	return Call[R](ctx, name, "arg")
}

// ---- THE BOUNDARY, which is exactly as load-bearing as the rule ---------------
//
// A STRING-kind key is UNAFFECTED, because resolveKeyName returns it verbatim
// before the TextMarshaler branch is reached. Disarming string keys too would be
// a strictly larger accepted miss for no false fire, and nothing else in the
// package would notice: `map[NamedString]V` is one of the commonest result
// members there is.

type r30Currency string

// A total marshaler that renders something OTHER than the key's own text. If
// encoding/json ever consulted it for a string-kind key, the shape below would
// read {usd:number} instead of {1:number} and this test would say so.
func (c r30Currency) MarshalText() ([]byte, error) { return []byte("usd"), nil }

func TestResultShape_StringKindMapKeyIgnoresItsMarshalText(t *testing.T) {
	// PREMISE: encoding/json really does take the string key verbatim.
	b, err := json.Marshal(map[r30Currency]int{"1": 5})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `{"1":5}` {
		t.Fatalf("premise broken: encoding/json rendered a string-kind key as %s, not {\"1\":5}; "+
			"it now consults MarshalText for string keys and the rule above needs revisiting", b)
	}

	got := ResultShapeStringForTest(reflect.TypeOf(map[r30Currency]int(nil)))
	want := ResultShapeStringForTest(reflect.TypeOf(map[string]int(nil)))
	if want != "{1:number}" {
		t.Fatalf("control broken: map[string]int must shape {1:number}, got %q", want)
	}
	if got != want {
		t.Errorf("a STRING-kind map key must be unaffected by its MarshalText: map[r30Currency]int "+
			"shaped %q, map[string]int shaped %q. encoding/json emits %s for the first, so the two "+
			"are wire-identical and disarming the string case is a miss bought for nothing",
			got, want, b)
	}
}

// The other half of the same control: the key kinds synthesizeMapKey renders
// itself still record a real, identical shape, so the rule above is not green
// because maps stopped working.
func TestResultShape_PlainMapKeyKindsStillRecordAShape(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(map[string]int(nil)),
		reflect.TypeOf(map[int]int(nil)),
		reflect.TypeOf(map[int32]int(nil)),
		reflect.TypeOf(map[uint8]int(nil)),
		reflect.TypeOf(map[uint64]int(nil)),
	} {
		if s := ResultShapeStringForTest(typ); s != "{1:number}" {
			t.Errorf("%s must still record {1:number}, got %q", typ, s)
		}
	}
}
