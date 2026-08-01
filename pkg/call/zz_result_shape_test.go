package call_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/call"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/stretchr/testify/require"
)

type shapeV1 struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}
type shapeV2 struct {
	Reference string `json:"reference"`
	Total     int    `json:"total"`
}

// strictShape validates that every key is present — a required-fields decoder, an
// ordinary shape for a schema-checked payload. THIS TYPE IS THE POINT: three
// earlier attempts to detect a result-type change by inspecting the stored BYTES
// hard-failed on exactly this, because its stored payload can legitimately be
// all-zero and any strict re-decode of those bytes fails.
type strictShape struct {
	A string `json:"a"`
	B int    `json:"b"`
}

func (v *strictShape) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	for _, k := range []string{"a", "b"} {
		if _, ok := raw[k]; !ok {
			return errors.New("missing " + k)
		}
	}
	type alias strictShape
	var out alias
	if err := json.Unmarshal(b, &out); err != nil {
		return err
	}
	*v = strictShape(out)
	return nil
}

func replayCtx(t *testing.T, name string, stored []byte, shape string, bestEffort bool) context.Context {
	t.Helper()
	cs := &intctx.CallState{
		Checkpoints: map[intctx.CheckpointKey]*core.Checkpoint{
			{Index: 0, Type: name}: {
				CallIndex: 0, CallType: name, Result: stored, SpanEnd: 1, ResultShape: shape,
			},
		},
	}
	jc := &intctx.JobContext{Job: &core.Job{ID: core.NewID(), Type: "wf"}, BestEffortReplay: bestEffort}
	ctx := intctx.WithJobContext(context.Background(), jc)
	return context.WithValue(ctx, intctx.CallStateKey{}, cs)
}

func shapeOf(v any) string {
	return call.ResultFingerprintForTest(reflect.TypeOf(v))
}

// A result type whose FIELDS changed decodes cleanly into the new type — unknown
// keys ignored, absent ones left zero — so the caller used to receive an empty
// result with a nil error and the workflow completed carrying it. Changing the call
// NAME was always caught loudly; this was the same hole for the result type.
func TestCallReplay_ChangedResultTypeFailsLoud(t *testing.T) {
	stored, err := json.Marshal(shapeV1{OrderID: "ord_1", Amount: 500})
	require.NoError(t, err)

	ctx := replayCtx(t, "settle", stored, shapeOf(shapeV1{}), false)
	_, err = call.Call[shapeV2](ctx, "settle", func(context.Context) (shapeV2, error) {
		t.Fatal("the handler must not run on replay")
		return shapeV2{}, nil
	})
	require.Error(t, err, "a changed result type must fail loudly, not return a zero value with a nil error")
	require.Contains(t, err.Error(), "determinism violation")
	require.Contains(t, err.Error(), "written from a different result type")
}

// THE CASE THAT KILLED THREE EARLIER ATTEMPTS. The type is UNCHANGED, its stored
// result is legitimately all-zero, and it has a required-fields UnmarshalJSON. Any
// approach that re-inspects the stored bytes hard-fails here — a working replay
// broken mid-flight, by default, with a confident and false "your type changed".
// Comparing a fingerprint computed from the TYPE on both sides cannot: identical
// type, identical fingerprint, no payload involved.
func TestCallReplay_UnchangedStrictTypeWithAllZeroResultReplaysCleanly(t *testing.T) {
	stored := []byte(`{"a":"","b":0}`)
	ctx := replayCtx(t, "validated", stored, shapeOf(strictShape{}), false)

	got, err := call.Call[strictShape](ctx, "validated", func(context.Context) (strictShape, error) {
		t.Fatal("the handler must not run on replay")
		return strictShape{}, nil
	})
	require.NoError(t, err, "the SAME type on both runs must replay cleanly; this is the false fire that sank the byte-inspection approaches")
	require.Equal(t, strictShape{}, got)
}

// A checkpoint written before the column existed carries an empty shape and must
// replay untouched, so work already in flight is unaffected by the upgrade.
func TestCallReplay_LegacyCheckpointWithoutAShapeIsNotChecked(t *testing.T) {
	stored, err := json.Marshal(shapeV1{OrderID: "ord_1", Amount: 500})
	require.NoError(t, err)

	ctx := replayCtx(t, "settle", stored, "", false) // no recorded shape
	got, err := call.Call[shapeV2](ctx, "settle", func(context.Context) (shapeV2, error) {
		t.Fatal("the handler must not run on replay")
		return shapeV2{}, nil
	})
	require.NoError(t, err, "a checkpoint with no recorded shape must skip the check, exactly as SpanEnd==0 degrades to the historical behaviour")
	require.Equal(t, shapeV2{}, got, "it still decodes to zero — that is the pre-existing behaviour this deliberately does not change for legacy rows")
}

// BestEffortReplay warns instead of failing, matching how the NAME mismatch behaves.
func TestCallReplay_BestEffortWarnsInsteadOfFailing(t *testing.T) {
	stored, err := json.Marshal(shapeV1{OrderID: "ord_1", Amount: 500})
	require.NoError(t, err)

	ctx := replayCtx(t, "settle", stored, shapeOf(shapeV1{}), true)
	_, err = call.Call[shapeV2](ctx, "settle", func(context.Context) (shapeV2, error) {
		t.Fatal("the handler must not run on replay")
		return shapeV2{}, nil
	})
	require.NoError(t, err, "best-effort replay must degrade to a warning, as it does for a name mismatch")
}

// The fingerprint is STRUCTURAL, so shapes that are semantically the same must not
// trip it. Each of these would be a false fire, which is the failure mode that
// matters most here — a guard that breaks working replays is worse than the bug.
func TestResultFingerprint_DoesNotFireOnEquivalentShapes(t *testing.T) {
	type reordered struct {
		Amount  int    `json:"amount"`
		OrderID string `json:"order_id"`
	}
	type renamedTypeSameShape struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
	}
	type withUnexported struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
		hidden  string //nolint:unused // unexported fields are never serialized
	}
	type withSkipped struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
		Skip    string `json:"-"`
	}

	base := shapeOf(shapeV1{})
	for name, v := range map[string]any{
		"field order changed": reordered{},
		"type renamed":        renamedTypeSameShape{},
		"unexported field":    withUnexported{},
		"json:\"-\" field":    withSkipped{},
		"pointer to the type": &shapeV1{},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, base, shapeOf(v),
				"%s is the same JSON shape and must not be treated as a type change; a guard that breaks working replays is worse than the defect it closes", name)
		})
	}
}

// ...and it MUST fire on a real shape change, or the equivalence test above could
// be satisfied by a fingerprint that returns a constant.
func TestResultFingerprint_DistinguishesRealChanges(t *testing.T) {
	type addedField struct {
		OrderID string `json:"order_id"`
		Amount  int    `json:"amount"`
		Extra   bool   `json:"extra"`
	}
	type renamedField struct {
		Reference string `json:"reference"`
		Amount    int    `json:"amount"`
	}
	type changedKind struct {
		OrderID string `json:"order_id"`
		Amount  string `json:"amount"`
	}

	base := shapeOf(shapeV1{})
	for name, v := range map[string]any{
		"added field":    addedField{},
		"renamed field":  renamedField{},
		"changed kind":   changedKind{},
		"different type": shapeV2{},
	} {
		t.Run(name, func(t *testing.T) {
			require.NotEqual(t, base, shapeOf(v), "%s changes the JSON shape and must be distinguished", name)
		})
	}
	require.NotEmpty(t, base)
	require.False(t, strings.Contains(base, " "), "the fingerprint must be a compact token")
}
