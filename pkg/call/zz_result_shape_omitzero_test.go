package call

import (
	"context"
	"encoding/json"
	"math/big"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// ---------------------------------------------------------------------------
// `,omitzero` ON A MEMBER THE PROBE CANNOT POPULATE
//
// Go 1.24 added the `omitzero` json tag option. It drops a member whose value is
// the zero of its type — or, when the type carries an `IsZero() bool`, whose
// IsZero reports true — and that decision is made about the VALUE the encoder is
// handed, not about the type.
//
// build cannot set unexported fields, so a member type made only of them —
// time.Time, netip.Addr, big.Int, any decimal-as-struct — is probed at its ZERO.
// probeSpeaksForType then trusts it, correctly, because its wire form IN
// ISOLATION is a scalar. That is a statement about the member's own bytes and
// says nothing about whether the PARENT emits the member at all: with `omitzero`
// the parent DROPS it, and the recorded shape loses a member production always
// writes.
//
// So `Created time.Time `json:"created"`` -> `json:"created,omitzero"` is
// byte-identical for every non-zero timestamp — the only kind a real handler
// produces — and moves the fingerprint from {created:string,n:number} to
// {n:number}. BOTH are non-empty, so call.go's fail-open guard does not skip and
// replay hard-fails on a refactor that cannot move a byte.
//
// The tests below assert wire-identity FIRST in every case, so a failure here is
// always a false fire and never two types that legitimately serialize
// differently.

type ozTime struct {
	Created time.Time `json:"created"`
	N       int       `json:"n"`
}
type ozTimeOmit struct {
	Created time.Time `json:"created,omitzero"`
	N       int       `json:"n"`
}

type ozTimePtr struct {
	Created *time.Time `json:"created"`
	N       int        `json:"n"`
}
type ozTimePtrOmit struct {
	Created *time.Time `json:"created,omitzero"`
	N       int        `json:"n"`
}

type ozAddr struct {
	Peer netip.Addr `json:"peer"`
	N    int        `json:"n"`
}
type ozAddrOmit struct {
	Peer netip.Addr `json:"peer,omitzero"`
	N    int        `json:"n"`
}

type ozBig struct {
	Amount big.Int `json:"amount"`
	N      int     `json:"n"`
}
type ozBigOmit struct {
	Amount big.Int `json:"amount,omitzero"`
	N      int     `json:"n"`
}

// ozTicket's IsZero has a POINTER receiver and reads state the probe cannot set.
// encoding/json boxes an unaddressable value to call such a method, so the member
// is dropped even though reflect reports the probe's value as NON-zero (Label is
// exported and gets populated). This is the fixture that separates the boxed
// check from the plain reflect one: without the box, nothing here is zero.
type ozTicket struct {
	Label string `json:"label"`
	seq   int
}

func (tk *ozTicket) IsZero() bool { return tk.seq == 0 }

type ozTicketHolder struct {
	T ozTicket `json:"t"`
	N int      `json:"n"`
}
type ozTicketHolderOmit struct {
	T ozTicket `json:"t,omitzero"`
	N int      `json:"n"`
}

// The control pair. *big.Int carries no IsZero, so `omitzero` falls back to
// reflect's zero test, which a non-nil pointer never satisfies: the member is
// kept on both sides and the fingerprints must stay EQUAL AND NON-EMPTY. It
// pins the fix's boundary from the other side — a rule that disarmed every
// `omitzero` member outright would pass every other test in this file and turn
// this one's shapes empty.
type ozBigPtr struct {
	Amount *big.Int `json:"amount"`
	N      int      `json:"n"`
}
type ozBigPtrOmit struct {
	Amount *big.Int `json:"amount,omitzero"`
	N      int      `json:"n"`
}

// ozWireIdentical marshals both populated values and fails unless the bytes are
// identical. Nothing about a fingerprint follows from a pair that serializes
// differently, so this runs before any fingerprint claim.
func ozWireIdentical(t *testing.T, a, b any) string {
	t.Helper()
	ba, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("FIXTURE BROKEN: marshal %T: %v", a, err)
	}
	bb, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("FIXTURE BROKEN: marshal %T: %v", b, err)
	}
	if string(ba) != string(bb) {
		t.Fatalf("FIXTURE BROKEN: the pair is not wire-identical, so no false fire follows:\n  %T: %s\n  %T: %s",
			a, ba, b, bb)
	}
	return string(ba)
}

func ozCases(t *testing.T) []struct {
	name string
	a, b any
} {
	t.Helper()
	ts := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	addr := netip.MustParseAddr("10.0.0.1")
	amt := big.NewInt(42)
	return []struct {
		name string
		a, b any
	}{
		{"time.Time member", ozTime{Created: ts, N: 3}, ozTimeOmit{Created: ts, N: 3}},
		{"*time.Time member", ozTimePtr{Created: &ts, N: 3}, ozTimePtrOmit{Created: &ts, N: 3}},
		{"netip.Addr member", ozAddr{Peer: addr, N: 3}, ozAddrOmit{Peer: addr, N: 3}},
		{"big.Int member", ozBig{Amount: *amt, N: 3}, ozBigOmit{Amount: *amt, N: 3}},
		{"pointer-receiver IsZero member",
			ozTicketHolder{T: ozTicket{Label: "x", seq: 7}, N: 3},
			ozTicketHolderOmit{T: ozTicket{Label: "x", seq: 7}, N: 3}},
	}
}

func TestResultShape_OmitzeroOnAProbeZeroMemberIsNotAFalseFire(t *testing.T) {
	for _, c := range ozCases(t) {
		t.Run(c.name, func(t *testing.T) {
			wire := ozWireIdentical(t, c.a, c.b)
			ta := reflect.TypeOf(c.a)
			tb := reflect.TypeOf(c.b)
			shapeA := ResultShapeStringForTest(ta)
			shapeB := ResultShapeStringForTest(tb)
			fpA := ResultFingerprintForTest(ta)
			fpB := ResultFingerprintForTest(tb)
			t.Logf("wire=%s shapeA=%q shapeB=%q", wire, shapeA, shapeB)
			// The PLAIN side carries no `omitzero`, so nothing about it changed and
			// it must still record a real shape. Without this the whole file would
			// pass just as well if the fix disarmed every member of these types —
			// two empty shapes agree trivially.
			if shapeA == "" {
				t.Errorf("the plain form carries no `omitzero` and nothing drops its member, so it " +
					"must still record a shape; an empty one means the guard was disarmed far " +
					"wider than the option that needed it")
			}
			// Replay refuses only when BOTH shapes are non-empty and differ; a
			// type that records no shape is skipped on either side of the
			// comparison (call.go), so an empty shape is fail-open, not a fire.
			if fpA != "" && fpB != "" && fpA != fpB {
				t.Errorf("FALSE FIRE: adding `,omitzero` to a member the probe leaves at its zero "+
					"cannot move a byte, yet the fingerprint moved and replay would refuse the "+
					"checkpoint.\n  wire:   %s\n  shapeA: %s\n  shapeB: %s", wire, shapeA, shapeB)
			}
		})
	}
}

// The control: no IsZero anywhere, so `omitzero` cannot drop a non-nil pointer
// and BOTH sides must keep a real, EQUAL shape. A fix that simply refused every
// type carrying an `omitzero` member would empty these and lose the guard for
// them.
func TestResultShape_OmitzeroOnAPopulatedMemberKeepsItsShape(t *testing.T) {
	amt := big.NewInt(42)
	wire := ozWireIdentical(t, ozBigPtr{Amount: amt, N: 3}, ozBigPtrOmit{Amount: amt, N: 3})
	shapeA := ResultShapeStringForTest(reflect.TypeOf(ozBigPtr{}))
	shapeB := ResultShapeStringForTest(reflect.TypeOf(ozBigPtrOmit{}))
	t.Logf("wire=%s shapeA=%q shapeB=%q", wire, shapeA, shapeB)
	if shapeA == "" || shapeB == "" {
		t.Fatalf("a member `omitzero` cannot drop must keep its shape on both sides; got %q and %q "+
			"— the guard is disarmed for every type carrying an omitzero member", shapeA, shapeB)
	}
	if shapeA != shapeB {
		t.Fatalf("wire-identical, so the shapes must match: %q vs %q", shapeA, shapeB)
	}
}

// An option encoding/json does not recognise is IGNORED by the encoder — the
// member is emitted like any other — so it must not disarm anything either. This
// is the pair that separates comparing a whole option from merely looking for the
// substring: `omitzeroish` contains `omitzero` and means nothing at all.
type ozLookalike struct {
	//nolint:staticcheck // SA5008: the UNKNOWN option is the fixture — encoding/json
	// ignores it, and so must the probe.
	Created time.Time `json:"created,omitzeroish"`
	N       int       `json:"n"`
}

func TestResultShape_AnUnrecognisedTagOptionDoesNotDisarmTheGuard(t *testing.T) {
	ts := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	wire := ozWireIdentical(t, ozTime{Created: ts, N: 3}, ozLookalike{Created: ts, N: 3})
	plain := ResultShapeStringForTest(reflect.TypeOf(ozTime{}))
	look := ResultShapeStringForTest(reflect.TypeOf(ozLookalike{}))
	t.Logf("wire=%s shape(plain)=%q shape(lookalike)=%q", wire, plain, look)
	if look == "" {
		t.Fatalf("encoding/json ignores an option it does not recognise, so the member is emitted "+
			"and the type must keep its shape; got %q — an option merely CONTAINING `omitzero` "+
			"disarmed the guard", look)
	}
	if look != plain {
		t.Fatalf("wire-identical, so the shapes must match: %q vs %q", plain, look)
	}
}

// And the guard stays ARMED for a type whose `omitzero` member the probe really
// does populate: dropping a member from it must still move the fingerprint.
type ozArmed struct {
	S string `json:"s,omitzero"`
	N int    `json:"n"`
}
type ozArmedShrunk struct {
	N int `json:"n"`
}

func TestResultShape_OmitzeroMemberTheProbePopulatesStaysGuarded(t *testing.T) {
	a := ResultFingerprintForTest(reflect.TypeOf(ozArmed{}))
	b := ResultFingerprintForTest(reflect.TypeOf(ozArmedShrunk{}))
	if a == "" || b == "" {
		t.Fatalf("both types are ordinary and must record a shape; got %q and %q", a, b)
	}
	if a == b {
		t.Fatal("deleting a member from a result type must move the fingerprint; the guard is inert")
	}
}

// ---- the production write+replay path ---------------------------------------

// The unit assertions above read the fingerprint directly. This drives the same
// edit through the real Call: production writes the checkpoint for the plain
// type, a deploy adds `,omitzero`, and the replay must accept the row it wrote.
func TestResultShape_OmitzeroDeployReplaysTheCheckpointProductionWrote(t *testing.T) {
	ts := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	// The premise, asserted rather than assumed: the deploy cannot move a byte.
	ozWireIdentical(t, ozTime{Created: ts, N: 3}, ozTimeOmit{Created: ts, N: 3})

	h, herr := handler.NewHandler(func(_ context.Context, _ string) (ozTime, error) {
		return ozTime{Created: ts, N: 3}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[ozTime, ozTimeOmit](t, h, "oz")
	if err != nil {
		t.Fatalf("FALSE FIRE IN PRODUCTION: adding `,omitzero` to an always-set timestamp cannot "+
			"move a byte, yet replay refused the checkpoint production wrote.\n"+
			"  persisted shape: %q\n  shape(ozTime):     %q\n  shape(ozTimeOmit): %q\n  error: %v",
			saved.ResultShape,
			ResultShapeStringForTest(reflect.TypeOf(ozTime{})),
			ResultShapeStringForTest(reflect.TypeOf(ozTimeOmit{})),
			err)
	}
	if !got.Created.Equal(ts) || got.N != 3 {
		t.Fatalf("the replayed value must be the checkpointed one, got %+v", got)
	}
	if strings.Contains(saved.ResultShape, " ") {
		t.Fatalf("unexpected persisted shape %q", saved.ResultShape)
	}

	// AND THE REVERSE DEPLOY, so the skip is symmetric rather than an accident of
	// which side happens to be empty. This direction is the one that persists the
	// empty shape: production runs the omitzero type, writes nothing for it, and a
	// rollback to the plain type must still replay. The pairwise sweep's
	// half-armed outcome rests on exactly this being true in both directions.
	h2, herr := handler.NewHandler(func(_ context.Context, _ string) (ozTimeOmit, error) {
		return ozTimeOmit{Created: ts, N: 3}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	savedRev, gotRev, err := writeThenReplay[ozTimeOmit, ozTime](t, h2, "oz-rev")
	if err != nil {
		t.Fatalf("FALSE FIRE (reverse): rolling the `,omitzero` back off was refused: %v", err)
	}
	if savedRev.ResultShape != "" {
		t.Fatalf("a type whose shape cannot be trusted must persist NOTHING, got %q — a later "+
			"deploy would be compared against it", savedRev.ResultShape)
	}
	if !gotRev.Created.Equal(ts) || gotRev.N != 3 {
		t.Fatalf("the replayed value must be the checkpointed one, got %+v", gotRev)
	}
}
