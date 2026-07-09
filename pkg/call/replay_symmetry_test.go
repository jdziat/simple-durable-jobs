package call

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	intctx "github.com/jdziat/simple-durable-jobs/v4/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/internal/handler"
)

// callFirstThenReplay executes Call[T] once (capturing the success checkpoint),
// then replays it from that checkpoint, returning both results. execCount points
// at a counter the handler increments; the harness asserts the handler ran
// exactly once on the first run and did NOT re-execute on replay (the replay leg
// must hit the checkpoint cache) — otherwise both legs would trivially agree and
// the type-symmetry assertions would be false-green.
func callFirstThenReplay[T any](t *testing.T, h any, name string, execCount *int) (first, replay T) {
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
	first, err := Call[T](ctx, name, "arg")
	if err != nil {
		t.Fatalf("first-run Call error: %v", err)
	}
	if saved == nil {
		t.Fatal("expected a success checkpoint to be saved")
	}
	if *execCount != 1 {
		t.Fatalf("expected handler to execute exactly once on first run, got %d", *execCount)
	}
	replayCtx := intctx.WithCallState(intctx.WithJobContext(context.Background(), jobCtx), []core.Checkpoint{*saved})
	replay, err = Call[T](replayCtx, name, "arg")
	if err != nil {
		t.Fatalf("replay Call error: %v", err)
	}
	if *execCount != 1 {
		t.Fatalf("replay must hit the checkpoint cache, not re-execute the handler; execCount=%d", *execCount)
	}
	return first, replay
}

// TestCallReplayTypeSymmetry is the E5-type-divergence regression (PKT-04). Call
// must return the JSON-round-tripped value so the dynamic type a caller observes
// is identical first-run and on replay; before the fix, Call[any] returned the
// handler's concrete type first-run but a decoded generic type on replay, so a
// caller type-assertion worked pre-crash and panicked post-resume.
func TestCallReplayTypeSymmetry(t *testing.T) {
	t.Run("Call[any] yields identical dynamic type first-run and replay", func(t *testing.T) {
		type probeResult struct {
			N int `json:"n"`
		}
		execCount := 0
		h, err := handler.NewHandler(func(ctx context.Context, args string) (probeResult, error) {
			execCount++
			return probeResult{N: 7}, nil
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		first, replay := callFirstThenReplay[any](t, h, "probe", &execCount)

		ft, rt := fmt.Sprintf("%T", first), fmt.Sprintf("%T", replay)
		if ft != rt {
			t.Fatalf("dynamic type differs across replay: first=%s replay=%s", ft, rt)
		}
		if _, isConcrete := first.(probeResult); isConcrete {
			t.Fatalf("Call[any] must return a round-tripped generic value, not the handler's concrete type %s", ft)
		}
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("values differ across replay: first=%#v replay=%#v", first, replay)
		}
	})

	t.Run("concrete struct round-trips to the same value first-run and replay", func(t *testing.T) {
		type Data struct {
			A string `json:"a"`
			B int    `json:"b"`
		}
		execCount := 0
		h, err := handler.NewHandler(func(ctx context.Context, args string) (Data, error) {
			execCount++
			return Data{A: "x", B: 42}, nil
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		first, replay := callFirstThenReplay[Data](t, h, "d", &execCount)
		want := Data{A: "x", B: 42}
		if first != want {
			t.Fatalf("first-run value = %#v, want %#v", first, want)
		}
		if first != replay {
			t.Fatalf("replay value = %#v, want %#v", replay, first)
		}
	})

	t.Run("time.Time normalizes consistently across replay", func(t *testing.T) {
		// A wall-clock time with a monotonic reading and a non-UTC location: JSON
		// drops the monotonic reading and normalizes the zone offset, and the fix
		// applies that same normalization on BOTH the first run and replay so a
		// caller sees an identical value either way.
		fixed := time.Date(2026, 7, 8, 12, 30, 0, 123456789, time.FixedZone("x", 5*3600))
		execCount := 0
		h, err := handler.NewHandler(func(ctx context.Context, args string) (time.Time, error) {
			execCount++
			return fixed, nil
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		first, replay := callFirstThenReplay[time.Time](t, h, "t", &execCount)
		if !first.Equal(replay) {
			t.Fatalf("time.Time differs across replay: first=%v replay=%v", first, replay)
		}
		if !first.Equal(fixed) {
			t.Fatalf("round-tripped time must equal the source instant: first=%v source=%v", first, fixed)
		}
	})

	t.Run("boxed large int64 in an any field is consistent across replay", func(t *testing.T) {
		// 2^53 decodes to float64 through JSON; the guarantee is CONSISTENCY (both
		// paths agree), not lossless preservation of the concrete int64.
		type box struct {
			V any `json:"v"`
		}
		execCount := 0
		h, err := handler.NewHandler(func(ctx context.Context, args string) (box, error) {
			execCount++
			return box{V: int64(1) << 53}, nil
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		first, replay := callFirstThenReplay[box](t, h, "box", &execCount)
		if !reflect.DeepEqual(first, replay) {
			t.Fatalf("boxed int64 differs across replay: first=%#v replay=%#v", first, replay)
		}
	})
}
