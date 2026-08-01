package call

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---------------------------------------------------------------------------
// TERMINATION IS ITS OWN BOUND, AND IT IS TESTED IN ITS OWN PROCESS
//
// Raising maxShapeDepth from 6 to 32 makes the depth cap effectively unreachable
// for real result types. It also makes it useless as an accidental backstop
// against a type that recurses while adding ZERO JSON nesting levels:
//
//	type T *T                         // legal Go
//	type A *B; type B *A              // legal Go
//	type S1 struct{ *S2 }; S2{ *S1 }  // legal Go, promotes at every hop
//
// A pointer is dereferenced by encoding/json and an untagged embedded struct is
// promoted into its parent, so neither may spend depth budget — which means a
// cycle made only of those hops never reaches ANY depth bound, however large.
// The visited-type set in synthesize is the only thing that stops it.
//
// WHY A SUBPROCESS, and why this file exists at all. Unbounded recursion here
// does not panic; it dies on `fatal error: stack overflow`, which recover()
// cannot catch and which takes the whole process down. In-process, a broken
// visited-type set therefore does not FAIL a test — it kills the test binary
// mid-run, so the signal arrives as an unattributed crash of whatever test
// happened to be executing, and under -race or a parallel package run it can be
// mistaken for infrastructure noise. Worse, in PRODUCTION the same crash lands
// on the write AND the replay path of every Call using such a type: a guard that
// kills the worker is strictly worse than no guard.
//
// So the probe runs in a child copy of this test binary. A stack overflow there
// is an exit status, which the parent reports as an ordinary, attributed test
// failure naming the mechanism that broke.

const cycleProbeEnv = "SDJ_CALL_CYCLE_PROBE"

// ---- fixtures: every legal way to cycle without adding a JSON level ---------

type r28CycSelf *r28CycSelf

type r28CycPtrA *r28CycPtrB
type r28CycPtrB *r28CycPtrA

// Mutually recursive through untagged EMBEDDED pointers: promotes at every hop.
type r28CycEmbA struct {
	*r28CycEmbB
	Name string `json:"name"`
}
type r28CycEmbB struct {
	*r28CycEmbA
	Kind string `json:"kind"`
}

// A struct embedding a pointer to ITSELF — the shortest embed cycle there is.
type r28CycSelfEmb struct {
	*r28CycSelfEmb
	V string `json:"v"`
}

// A longer cycle, mixing a value embed into the ring, so a mechanism that only
// notices a one- or two-hop repeat is not enough.
type r28CycLongA struct {
	*r28CycLongB
	A string `json:"a"`
}
type r28CycLongB struct {
	r28CycLongC
	B string `json:"b"`
}
type r28CycLongC struct {
	*r28CycLongA
	C string `json:"c"`
}

// A NAMED pointer type cannot sit on a zero-nesting ring through a struct — an
// embedded named pointer does not promote, so json nests it and the walk spends
// budget — but it is on the ring in `type T *T` and `type A *B` above, which is
// where build's Pointer case has to convert reflect.New's UNNAMED pointer back
// to the named type. Those two rows cover it.

func r28CycleFixtures() []struct {
	name string
	typ  reflect.Type
} {
	return []struct {
		name string
		typ  reflect.Type
	}{
		{"type T *T", reflect.TypeOf((*r28CycSelf)(nil)).Elem()},
		{"type A *B; type B *A", reflect.TypeOf((*r28CycPtrA)(nil)).Elem()},
		{"mutually recursive embedded pointers", reflect.TypeOf(r28CycEmbA{})},
		{"a struct embedding a pointer to itself", reflect.TypeOf(r28CycSelfEmb{})},
		{"a three-hop embed ring through a value embed", reflect.TypeOf(r28CycLongA{})},
		{"pointer to a cyclic type", reflect.TypeOf(&r28CycEmbA{})},
		{"slice of a cyclic type", reflect.TypeOf([]r28CycEmbA{})},
	}
}

// TestResultShape_CycleTerminationProbe is the CHILD. It is skipped unless the
// parent below re-executes this binary with cycleProbeEnv set, so an ordinary
// `go test` run never takes the crash in-process.
func TestResultShape_CycleTerminationProbe(t *testing.T) {
	if os.Getenv(cycleProbeEnv) == "" {
		t.Skip("child of TestResultShape_ZeroNestingCyclesTerminateInTheirOwnProcess")
	}
	for _, tc := range r28CycleFixtures() {
		// Reaching the next statement at all is the assertion. Unbounded
		// recursion never gets here: it dies on a fatal stack overflow that
		// takes this process with it, which is what the parent detects.
		_ = ResultFingerprintForTest(tc.typ)
		t.Logf("terminated: %s (%s)", tc.name, tc.typ)
	}

	// THE REPLAY PATH TOO. resultFingerprint runs on both sides of a Call, and
	// on replay nothing else in the process would touch the result type — so a
	// non-terminating walk is a crash a worker takes while REPLAYING work it
	// already did successfully. writeThenReplay drives the real Call on both
	// sides.
	h, err := handler.NewHandler(func(_ context.Context, _ string) (r28CycEmbA, error) {
		return r28CycEmbA{Name: "n"}, nil
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	if _, _, err := writeThenReplay[r28CycEmbA, r28CycEmbA](t, h, "cycle"); err != nil {
		t.Fatalf("replaying a cyclic result type must not error: %v", err)
	}
}

// TestResultShape_ZeroNestingCyclesTerminateInTheirOwnProcess is the PARENT.
func TestResultShape_ZeroNestingCyclesTerminateInTheirOwnProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child test binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestResultShape_CycleTerminationProbe$",
		"-test.v",
		"-test.timeout=90s",
		"-test.count=1")
	cmd.Env = append(os.Environ(), cycleProbeEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		return
	}
	// A stack overflow is the specific failure this guards, so say so rather
	// than dumping an exit status and leaving the reader to work it out.
	text := string(out)
	hint := ""
	if strings.Contains(text, "stack overflow") {
		hint = "\n\nTHE VISITED-TYPE SET IN synthesize IS BROKEN. A type that reaches itself " +
			"through pointers and untagged embeds adds no JSON nesting, so NO depth bound can " +
			"stop it — and the resulting stack overflow is a fatal error that resultShape's " +
			"recover() cannot catch. In production this kills the worker on the write AND the " +
			"replay path of every Call using such a type."
	}
	if ctx.Err() != nil {
		hint = "\n\nThe probe did not finish. The walk is looping without terminating."
	}
	t.Fatalf("the cycle probe subprocess failed (%v)%s\n\n--- child output ---\n%s", err, hint, text)
}

// THE VISITED-TYPE SET IS PINNED BY WHAT IT PRODUCES, not only by the absence of
// a crash. maxShapeNodes bounds the total work of a probe, so a broken
// visited-type set no longer overflows the stack — it burns the node budget and
// the type fails open to no shape. That is a much better failure than a fatal
// error, and it is exactly why "it did not crash" is no longer sufficient
// evidence that the mechanism works.
//
// So every cyclic fixture must still record a REAL shape. A cycle terminated by
// the visited-type set costs a handful of nodes and keeps every ordinary member
// around the ring; a cycle "terminated" by exhausting the budget costs 100000 and
// keeps nothing. The two are indistinguishable by liveness and trivially
// distinguishable here.
//
// A reviewer's mutation of the Pointer case's `append(free, t)` — one line —
// leaves every embed-cycle fixture terminating correctly, because the Struct
// case's own append catches those. `type T *T` has no struct in its ring, so it
// is the row that fails, and it is the reason the fixture list is not just the
// interesting-looking struct ones.
func TestResultShape_ACyclicTypeStillRecordsARealShape(t *testing.T) {
	for _, tc := range r28CycleFixtures() {
		t.Run(tc.name, func(t *testing.T) {
			shape, ok := resultShape(tc.typ)
			if !ok || shape == "" {
				t.Fatalf("%s records no shape. Terminating a cycle must not mean abandoning "+
					"the type: this is what a broken visited-type set looks like once "+
					"maxShapeNodes catches the runaway walk instead of the stack doing it.",
					tc.typ)
			}
		})
	}

	// The fixtures must be types encoding/json can really marshal, or the probe
	// would fail open for a reason that has nothing to do with the cycle and the
	// rows above would pass without testing the mechanism.
	if _, err := json.Marshal(r28CycSelfEmb{r28CycSelfEmb: &r28CycSelfEmb{V: "inner"}, V: "outer"}); err != nil {
		t.Fatalf("the self-embedding fixture must be marshalable: %v", err)
	}

	// And the ordinary members around the ring survive, so a cyclic result type
	// is still genuinely guarded rather than nominally so.
	shape := ResultShapeStringForTest(reflect.TypeOf(r28CycEmbA{}))
	if !strings.Contains(shape, "name:string") {
		t.Errorf("a cyclic type must still describe its ordinary members, got %q", shape)
	}
}

// ---- the UPGRADE.md claim, EXECUTED -----------------------------------------
//
// UPGRADE.md states that a result type containing a validating marshaler "records
// no shape anywhere in it". A reviewer proved that false and demonstrated the
// false fire it denied: the skip only held while the validating member sat INSIDE
// the probe depth. At or below the cap the member was TRUNCATED rather than
// probed, the marshaler was never offended, and a shape WAS recorded — so the two
// forms of one wire-identical refactor could be compared against each other and
// one of them refused.
//
// Deleting the truncation closes it, and this test is what makes the sentence in
// the docs a statement someone has run rather than one someone believed. Inside
// the cap the probe is rejected and there is no shape; past the cap the budget is
// exhausted and there is no shape. There is no depth at which a shape appears.
func TestResultShape_ValidatingMarshalerRecordsNoShapeAtANYDepth(t *testing.T) {
	// r27LeaseIP holds a net.IP, whose MarshalText refuses any length other than
	// 0, 4 or 16 — so it rejects the fabricated one-byte probe.
	for n := 0; n <= maxShapeDepth+2; n++ {
		typ := pwNestN(reflect.TypeOf(r27LeaseIP{}), n)
		if s := ResultShapeStringForTest(typ); s != "" {
			t.Errorf("at nesting %d a type containing a validating marshaler recorded the shape "+
				"%s. UPGRADE.md claims it records none ANYWHERE in it; at this depth the member "+
				"is reached by a mechanism that does not offend the marshaler, so the guard arms "+
				"itself against a type it cannot describe.", n, s)
		}
	}

	// The claim has to hold for the CONTROL too, or the sentence is true only by
	// accident of one fixture: the wire-identical modernization of the same type.
	for n := 0; n <= maxShapeDepth+2; n++ {
		if s := ResultShapeStringForTest(pwNestN(reflect.TypeOf(r27LeaseIPChanged{}), n)); s != "" {
			t.Errorf("at nesting %d the changed form recorded %s; both directions of the refactor "+
				"must be unguarded or one can refuse the other", n, s)
		}
	}

	// And a nesting depth at which an EQUIVALENT type WITHOUT the marshaler is
	// still guarded, so the rows above are not passing merely because everything
	// at that depth is unguarded.
	type plainLease struct {
		IP    string    `json:"ip"`
		Items []r27Item `json:"items"`
	}
	if s := ResultShapeStringForTest(reflect.TypeOf(plainLease{})); s == "" {
		t.Error("the same type without the validating marshaler must still be guarded, or this " +
			"test proves nothing about the marshaler")
	}
}
