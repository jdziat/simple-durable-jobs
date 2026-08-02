package call

import (
	"context"
	"encoding/json"
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

// ---- interface results: A DELIBERATE ACCEPTED MISS ---------------------------
//
// A result type containing an interface ANYWHERE records no shape and is not
// guarded at all. THESE TESTS ASSERT THE MISS, and they are the inverse of what
// they asserted before: the old pair required Call[any] to record a real shape and
// required tightening it to a concrete type to be CAUGHT.
//
// WHY IT WAS INVERTED, so nobody re-inverts it. A shape for an interface member
// could only come from substituting a value — `reflect.Zero`, a nil interface —
// and encoding/json then decides its fate by the Go REPRESENTATION, not the wire
// form. isEmptyValue calls a nil interface EMPTY, so with `omitempty` the member
// was DROPPED and without it recorded as null. Adding `,omitempty` to a `Meta any`
// production always populates is byte-identical on the wire and was refused on
// replay: a live false fire at nesting depth 0. The same substitution also hid the
// member's outright DELETION, because a dropped member and an absent one look the
// same. There is no third value to try, so the type records nothing — the same
// fail-open as an unprobeable marshaler and an over-cap type.
//
// A MISS LEAVES PRIOR BEHAVIOUR IN PLACE. A FALSE FIRE WEDGES A LIVE WORKFLOW.
// The cost is real and is stated in UPGRADE.md: Call[any], and any struct with an
// `any` member, lose the guard on their OTHER members too.

// rsStringerLike is an interface with a NON-EMPTY method set. It exists because
// the policy above was, for one revision, pinned only for EMPTY interfaces: a
// mutation narrowing the rule to `if t.NumMethod() > 0 { substitute }` survived
// the entire suite. That is not a far-fetched mutation — it is the plausible
// future "optimization" (`any` is opaque, but surely a method-set interface tells
// us something). It does not: the METHOD set says nothing about the JSON the
// concrete value will produce, so the substitution and its false fire come back
// wholesale.
type rsStringerLike interface {
	String() string
}

type rsMethodIfaceHolder struct {
	V  rsStringerLike `json:"v"`
	ID string         `json:"id"`
}

func TestResultShape_InterfaceMemberIsADeliberateAcceptedMiss(t *testing.T) {
	t.Run("Call[any] records no shape", func(t *testing.T) {
		if s := staticResultShape[any](); s != "" {
			t.Fatalf("an interface result must record NO shape, got %q; a shape here can only "+
				"come from substituting a nil interface, which `omitempty` drops and its "+
				"absence records as null — the false fire this inversion removed", s)
		}
	})

	// The rule is on Kind, NOT on NumMethod. Without this leg, narrowing it to
	// empty interfaces only is undetectable.
	t.Run("a NON-EMPTY interface member is treated the same", func(t *testing.T) {
		if fp := fingerprintOf(rsMethodIfaceHolder{}); fp != "" {
			t.Fatalf("an interface with a method set must record NO shape either, got %q — a "+
				"method set constrains the Go type, not the JSON the concrete value emits, "+
				"so substituting for it reintroduces the omitempty false fire", fp)
		}
	})

	// ONE interface member disarms the WHOLE type, including its ordinary members.
	// This is the coverage the inversion costs, asserted rather than implied.
	t.Run("one any member disarms the whole type", func(t *testing.T) {
		if s := ResultShapeStringForTest(reflectTypeOf[ifaceMetaV1]()); s != "" {
			t.Fatalf("a struct with an `any` member must record NO shape, got %q", s)
		}
		a := ResultFingerprintForTest(reflectTypeOf[ifaceMetaV1]())
		b := ResultFingerprintForTest(reflectTypeOf[ifaceMetaRenamed]())
		if a != "" || b != "" {
			t.Fatalf("both must fingerprint empty, got %q and %q", a, b)
		}
	})

	// The member at depth, inside a slice element, and inside a map value — the
	// rule is "anywhere in the type", not "at the top".
	t.Run("anywhere in the type, not just at the top", func(t *testing.T) {
		for name, typ := range map[string]reflect.Type{
			"nested":        reflectTypeOf[ifaceNested](),
			"slice element": reflect.TypeOf([]ifaceMetaV1(nil)),
			"map value":     reflect.TypeOf(map[string]ifaceMetaV1(nil)),
			"pointer":       reflect.TypeOf((*ifaceMetaV1)(nil)),
		} {
			if s := ResultShapeStringForTest(typ); s != "" {
				t.Errorf("%s: an interface reached through it must disarm the whole type, got %q", name, s)
			}
		}
	})
}

// The wire-neutral edit that used to WEDGE, driven end to end through production.
// Adding `,omitempty` to a populated interface member cannot move a byte, and
// replay must accept it.
func TestResultShape_InterfaceOmitemptyToggleIsNotRefused(t *testing.T) {
	v1 := ifaceMetaV1{Meta: map[string]any{"k": "v"}, Ref: "r"}
	v2 := ifaceMetaOmit{Meta: map[string]any{"k": "v"}, Ref: "r"}
	ba, err := json.Marshal(v1)
	if err != nil {
		t.Fatalf("marshal v1: %v", err)
	}
	bb, err := json.Marshal(v2)
	if err != nil {
		t.Fatalf("marshal v2: %v", err)
	}
	if string(ba) != string(bb) {
		t.Fatalf("the pair is not wire-identical, so it proves nothing: %s vs %s", ba, bb)
	}

	h, herr := handler.NewHandler(func(_ context.Context, _ string) (ifaceMetaV1, error) { return v1, nil })
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, _, err := writeThenReplay[ifaceMetaV1, ifaceMetaOmit](t, h, "meta-omitempty")
	if err != nil {
		t.Fatalf("FALSE FIRE: adding `,omitempty` to a populated interface member is "+
			"byte-identical (%s) and decodes losslessly, yet replay refused it: %v", ba, err)
	}
	if saved.ResultShape != "" {
		t.Fatalf("a type with an interface member must persist no shape, got %q", saved.ResultShape)
	}

	// And the reverse deploy, so the skip is symmetric.
	h2, herr := handler.NewHandler(func(_ context.Context, _ string) (ifaceMetaOmit, error) { return v2, nil })
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	if _, _, err := writeThenReplay[ifaceMetaOmit, ifaceMetaV1](t, h2, "meta-omitempty-rev"); err != nil {
		t.Fatalf("FALSE FIRE (reverse): removing `,omitempty` was refused: %v", err)
	}
}

// THE COST, asserted rather than described. Tightening Call[any] to a concrete
// type is NO LONGER caught. This test fails if that ever starts being caught
// again, which is the signal that someone re-introduced a substituted value.
func TestResultShape_AnyThenConcreteIsNoLongerCaught(t *testing.T) {
	h, herr := handler.NewHandler(func(_ context.Context, _ string) (e2eV1, error) {
		return e2eV1{OrderID: "ord_1", Amount: 500}, nil
	})
	if herr != nil {
		t.Fatalf("NewHandler: %v", herr)
	}
	saved, _, err := writeThenReplay[any, e2eV2](t, h, "lookup")
	if saved.ResultShape != "" {
		t.Fatalf("Call[any] must persist no shape, got %q", saved.ResultShape)
	}
	if err != nil {
		t.Fatalf("an interface result is unguarded by design, so replay must not refuse it: %v", err)
	}
}

type ifaceMetaV1 struct {
	Meta any    `json:"meta"`
	Ref  string `json:"ref"`
}

type ifaceMetaOmit struct {
	Meta any    `json:"meta,omitempty"`
	Ref  string `json:"ref"`
}

// Same wire-visible members as ifaceMetaV1 but a renamed one: a REAL change that
// the guard would have caught before, and no longer does. That is the coverage
// this inversion costs, named here so it cannot be forgotten.
type ifaceMetaRenamed struct {
	Meta any    `json:"meta"`
	Code string `json:"code"`
}

type ifaceNested struct {
	Inner struct {
		Deep struct {
			M any `json:"m"`
		} `json:"deep"`
	} `json:"inner"`
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

// EmbA and EmbB each carry an `id` — the SAME json name at the SAME promotion
// depth, which is the whole fixture. Neither declaration repeats a tag on its own,
// so neither is a diagnostic; the repetition only exists once both are embedded in
// one struct.
type EmbA struct {
	ID   string `json:"id"`
	Only string `json:"only_a"`
}
type EmbB struct {
	ID string `json:"id"`
}

// THE TWO CONFLICT TYPES ARE BUILT, NOT DECLARED, and that is not cosmetic.
//
// The repeated `json:"id"` IS the fixture — it is the promotion conflict — so it
// must not be weakened to quiet a linter. Written as literal declarations
// (`type ambiguous struct { embA; embB }`) they made `go vet ./...` exit 1 with
// two structtag diagnostics for this package, on a branch whose CONTRIBUTING.md
// requires `go vet ./...` to pass. The `// nolint:govet` they carried is a
// golangci-lint directive; `go vet` has no such mechanism and ignored it, so the
// suppression only ever silenced ONE of the two gates and the other was quietly
// red. Nothing in CI runs `go vet`, and `go test` does not run the structtag
// analyzer, so the suite stayed green over it.
//
// reflect.StructOf produces a type encoding/json cannot distinguish from the
// declared one — the same two promoted `id` members at the same depth, resolved
// by the same dominantField rule — and leaves no literal duplicate tag in source
// for the analyzer to find. zz_result_shape_pairwise_test.go already builds its
// variants this way. Both tests below assert the conflict against the REAL
// encoder first, so a StructOf that failed to reproduce it could not pass.
var (
	// Both embeds supply "id" at the same depth, so json emits no "id" at all.
	ambiguousType = reflect.StructOf([]reflect.StructField{
		{Name: "EmbA", Type: reflect.TypeOf(EmbA{}), Anonymous: true},
		{Name: "EmbB", Type: reflect.TypeOf(EmbB{}), Anonymous: true},
	})

	// Same two embeds, but the outer struct declares "id" itself: depth 0 beats
	// depth 1, so json emits the outer one.
	shadowedType = reflect.StructOf([]reflect.StructField{
		{Name: "EmbA", Type: reflect.TypeOf(EmbA{}), Anonymous: true},
		{Name: "EmbB", Type: reflect.TypeOf(EmbB{}), Anonymous: true},
		{Name: "ID", Type: reflect.TypeOf(0), Tag: `json:"id"`},
	})
)

// e2eZeroOf gives a value of a built type, for the fixtures that are consumed as
// values rather than as reflect.Types.
func e2eZeroOf(t reflect.Type) any { return reflect.New(t).Elem().Interface() }

// e2eJSONKeys is the PREMISE check both promotion tests run first: what
// encoding/json really emits for the built type. Without it a StructOf that
// silently stopped promoting would leave both tests comparing two empty shapes
// and asserting nothing.
func e2eJSONKeys(t *testing.T, v any) map[string]json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return m
}

func TestResultShape_AmbiguousPromotionDropsTheField(t *testing.T) {
	// PREMISE: the built type really does exhibit the conflict — "id" is dropped
	// by the real encoder, "only_a" survives.
	keys := e2eJSONKeys(t, e2eZeroOf(ambiguousType))
	if _, present := keys["id"]; present {
		t.Fatalf("FIXTURE BROKEN: encoding/json emitted \"id\" for %s, so the two embeds are not "+
			"in conflict and the test below proves nothing", ambiguousType)
	}
	if _, present := keys["only_a"]; !present {
		t.Fatalf("FIXTURE BROKEN: %s promotes nothing at all (%v); the conflict is not being "+
			"exercised", ambiguousType, keys)
	}

	// Equivalent to a struct with only "only_a": "id" is ambiguous and not emitted.
	type onlyA struct {
		Only string `json:"only_a"`
	}
	got := ResultFingerprintForTest(ambiguousType)
	want := ResultFingerprintForTest(reflectTypeOf[onlyA]())
	if got != want {
		t.Fatalf("an ambiguous promoted name must not appear in the shape: %s != %s", got, want)
	}
}

func TestResultShape_ShallowerFieldWinsOverPromoted(t *testing.T) {
	// PREMISE: the outer "id" really is what the encoder emits, and it is a
	// NUMBER — the promoted ones are strings, so the kind is what distinguishes
	// "the outer field won" from "some id was emitted".
	keys := e2eJSONKeys(t, e2eZeroOf(shadowedType))
	if string(keys["id"]) != "0" {
		t.Fatalf("FIXTURE BROKEN: encoding/json emitted id=%s for %s; the outer numeric field "+
			"must win the promotion conflict or the test below proves nothing",
			keys["id"], shadowedType)
	}

	// The outer "id" (a number) wins over both promoted string "id"s.
	type flat struct {
		ID   int    `json:"id"`
		Only string `json:"only_a"`
	}
	got := ResultFingerprintForTest(shadowedType)
	want := ResultFingerprintForTest(reflectTypeOf[flat]())
	if got != want {
		t.Fatalf("the shallowest field must win a promotion conflict: %s != %s", got, want)
	}
}
