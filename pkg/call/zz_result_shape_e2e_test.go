package call

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// This file exists because the original result-shape tests hand-seeded
// checkpoint.ResultShape in their fixture. Nothing observed whether PRODUCTION
// ever wrote it, so deleting the write site left the whole guard inert with the
// full suite green — the feature could be removed and no test noticed.
//
// Everything here therefore captures the checkpoint from a REAL first-run Call
// and feeds that captured value into the replay. The shape under test is the one
// production computed, never one a test supplied.

// writeThenReplay runs Call[W] for real, captures the checkpoint production wrote,
// then replays it as Call[R] — the deploy-changed-the-result-type scenario. It
// returns the captured checkpoint and the replay's error so a test can assert on
// both the persisted shape and the replay outcome.
func writeThenReplay[W any, R any](t *testing.T, h any, name string) (*core.Checkpoint, R, error) {
	t.Helper()
	var saved *core.Checkpoint
	jobCtx := &intctx.JobContext{
		Job:           &core.Job{ID: "job-1"},
		HandlerLookup: func(string) (any, bool) { return h, true },
		SaveCheckpoint: func(_ context.Context, cp *core.Checkpoint) error {
			c := *cp
			saved = &c
			return nil
		},
	}
	ctx := intctx.WithCallState(intctx.WithJobContext(context.Background(), jobCtx), []core.Checkpoint{})
	if _, err := Call[W](ctx, name, "arg"); err != nil {
		t.Fatalf("first-run Call error: %v", err)
	}
	if saved == nil {
		t.Fatal("expected a success checkpoint to be saved")
	}
	replayCtx := intctx.WithCallState(intctx.WithJobContext(context.Background(), jobCtx), []core.Checkpoint{*saved})
	got, err := Call[R](replayCtx, name, "arg")
	return saved, got, err
}

type e2eV1 struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}
type e2eV2 struct {
	Reference string `json:"reference"`
	Total     int    `json:"total"`
}

// THE WRITE SIDE. If production stops recording the fingerprint, the persisted
// shape is empty, replay reads the row as pre-upgrade, skips the check, and the
// silent-zero bug is back. Asserting the captured checkpoint is non-empty is what
// makes deleting the write site fail.
func TestResultShape_ProductionWritesTheFingerprint(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (e2eV1, error) {
		return e2eV1{OrderID: "ord_1", Amount: 500}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, _, err := writeThenReplay[e2eV1, e2eV1](t, h, "settle")
	if err != nil {
		t.Fatalf("same-type replay must succeed, got %v", err)
	}
	if saved.ResultShape == "" {
		t.Fatal("production did not record a result shape on the checkpoint; " +
			"replay will treat every new row as a pre-upgrade row and the guard is inert")
	}
	if want := ResultFingerprintForTest(reflectTypeOf[e2eV1]()); saved.ResultShape != want {
		t.Fatalf("persisted shape %q is not the result type's fingerprint %q", saved.ResultShape, want)
	}
}

// End-to-end version of the headline case, with the shape produced by production
// rather than by the test.
func TestResultShape_ChangedTypeIsCaughtEndToEnd(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (e2eV1, error) {
		return e2eV1{OrderID: "ord_1", Amount: 500}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	_, got, err := writeThenReplay[e2eV1, e2eV2](t, h, "settle")
	if err == nil {
		t.Fatalf("replaying a changed result type must fail loudly; got value %+v with a nil error", got)
	}
	if !strings.Contains(err.Error(), "determinism violation") {
		t.Fatalf("expected a determinism violation, got %v", err)
	}
}

// ---- embedded structs -------------------------------------------------------
//
// encoding/json PROMOTES an untagged embedded struct's fields into the parent
// object. A fingerprint that reads the Go declaration literally instead is nominal
// in disguise: renaming the embedded type changes the shape while the bytes stay
// byte-identical, which wedges a healthy workflow on a pure refactor.

type auditFieldsA struct {
	CreatedBy string `json:"created_by"`
}
type auditFieldsRenamed struct {
	CreatedBy string `json:"created_by"`
}

type embeddedBefore struct {
	auditFieldsA
	Amount int `json:"amount"`
}
type embeddedAfterRename struct {
	auditFieldsRenamed
	Amount int `json:"amount"`
}
type embeddedAfterFlatten struct {
	CreatedBy string `json:"created_by"`
	Amount    int    `json:"amount"`
}

func TestResultShape_EmbeddedRefactorsDoNotFalseFire(t *testing.T) {
	base := ResultFingerprintForTest(reflectTypeOf[embeddedBefore]())
	for _, tc := range []struct {
		name string
		got  string
	}{
		{"embedded type renamed", ResultFingerprintForTest(reflectTypeOf[embeddedAfterRename]())},
		{"embedded fields flattened into the parent", ResultFingerprintForTest(reflectTypeOf[embeddedAfterFlatten]())},
	} {
		if tc.got != base {
			t.Errorf("%s: wire-identical refactor changed the fingerprint (%s -> %s); "+
				"in-flight replays would hard-fail on a pure refactor", tc.name, base, tc.got)
		}
	}
}

// The same defect's other face: an UNEXPORTED embedded type still has its exported
// fields promoted and serialized, so skipping it makes those fields invisible and a
// completely different field set fingerprints identically.
type hiddenV1 struct {
	CreatedBy string `json:"created_by"`
	Amount    int    `json:"amount"`
}
type hiddenV2 struct {
	Approver string `json:"approver"`
	Total    int    `json:"total"`
}
type payoutV1 struct {
	hiddenV1
	Ref string `json:"ref"`
}
type payoutV2 struct {
	hiddenV2
	Ref string `json:"ref"`
}

func TestResultShape_PromotedFieldsOfUnexportedEmbedAreSeen(t *testing.T) {
	a := ResultFingerprintForTest(reflectTypeOf[payoutV1]())
	b := ResultFingerprintForTest(reflectTypeOf[payoutV2]())
	if a == b {
		t.Fatalf("payoutV1{created_by,amount,ref} and payoutV2{approver,total,ref} share fingerprint %s; "+
			"the promoted fields of an unexported embedded struct are invisible to the shape, "+
			"so a real result-type change replays as a wrong value with a nil error", a)
	}
}

// ---- interface results ------------------------------------------------------

// Call[any] is documented and supported. Its fingerprint must not collide with the
// empty sentinel that means "written before this column existed", or tightening
// Call[any] to a concrete type is silently exempted from the check.
func TestResultShape_InterfaceResultIsNotTheLegacySentinel(t *testing.T) {
	if s := staticResultShape[any](); s == "" {
		t.Fatal("Call[any] records the empty shape, which replay reads as a pre-upgrade " +
			"checkpoint and skips; tightening Call[any] to a concrete type would replay as zero")
	}
}

func TestResultShape_AnyThenConcreteIsCaughtEndToEnd(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (e2eV1, error) {
		return e2eV1{OrderID: "ord_1", Amount: 500}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, got, err := writeThenReplay[any, e2eV2](t, h, "lookup")
	if saved.ResultShape == "" {
		t.Fatal("Call[any] persisted the legacy empty sentinel")
	}
	if err == nil {
		t.Fatalf("tightening Call[any] to a concrete type must be caught; got %+v with a nil error", got)
	}
}

// ---- numeric widening -------------------------------------------------------

type widenBefore struct {
	Amount int `json:"amount"`
}
type widenAfter struct {
	Amount int64 `json:"amount"`
}

// JSON has one number type, so int -> int64 leaves the wire format and the decode
// identical and must not read as a type change.
func TestResultShape_NumericWideningDoesNotFalseFire(t *testing.T) {
	a := ResultFingerprintForTest(reflectTypeOf[widenBefore]())
	b := ResultFingerprintForTest(reflectTypeOf[widenAfter]())
	if a != b {
		t.Fatalf("int -> int64 widening changed the fingerprint (%s -> %s) despite identical JSON", a, b)
	}
}

// ...but a number becoming a string is a real wire change and must still fire.
type widenToString struct {
	Amount string `json:"amount"`
}

func TestResultShape_NumberToStringStillFires(t *testing.T) {
	if ResultFingerprintForTest(reflectTypeOf[widenBefore]()) == ResultFingerprintForTest(reflectTypeOf[widenToString]()) {
		t.Fatal("number -> string is a real wire change and must change the fingerprint")
	}
}

// reflectTypeOf gives T's STATIC type, so an interface T yields the interface type
// rather than nil (which reflect.TypeOf of a nil interface value would return).
func reflectTypeOf[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

// ---- promotion conflicts ----------------------------------------------------
//
// Go's rule, which the shape has to mirror or it describes JSON that is never
// emitted: for one JSON name the SHALLOWEST embedding wins, and if two fields tie
// at that shallowest depth encoding/json emits neither.

type embA struct {
	ID   string `json:"id"`
	Only string `json:"only_a"`
}
type embB struct {
	ID string `json:"id"`
}

// Both embeds supply "id" at the same depth, so json emits no "id" at all.
// nolint:govet // the repeated "id" tag IS the fixture: this is the ambiguous
// promotion whose field encoding/json declines to emit.
type ambiguous struct {
	embA
	embB
}

// Same two embeds, but the outer struct declares "id" itself: depth 0 beats depth
// 1, so json emits the outer one.
// nolint:govet // the repeated "id" tag IS the fixture: the outer field must win
// the promotion conflict against both embedded ones.
type shadowed struct {
	embA
	embB
	ID int `json:"id"`
}

func TestResultShape_AmbiguousPromotionDropsTheField(t *testing.T) {
	// Equivalent to a struct with only "only_a": "id" is ambiguous and not emitted.
	type onlyA struct {
		Only string `json:"only_a"`
	}
	got := ResultFingerprintForTest(reflectTypeOf[ambiguous]())
	want := ResultFingerprintForTest(reflectTypeOf[onlyA]())
	if got != want {
		t.Fatalf("an ambiguous promoted name must not appear in the shape: %s != %s", got, want)
	}
}

func TestResultShape_ShallowerFieldWinsOverPromoted(t *testing.T) {
	// The outer "id" (a number) wins over both promoted string "id"s.
	type flat struct {
		ID   int    `json:"id"`
		Only string `json:"only_a"`
	}
	got := ResultFingerprintForTest(reflectTypeOf[shadowed]())
	want := ResultFingerprintForTest(reflectTypeOf[flat]())
	if got != want {
		t.Fatalf("the shallowest field must win a promotion conflict: %s != %s", got, want)
	}
}
