package call

import (
	"errors"
	"reflect"
	"testing"
)

// resultFingerprint runs inside every nested Call, on the replay path as well as
// the write path. On replay nothing else marshals the result type, so a panic
// escaping the probe would be a crash the guard itself introduced — in a workflow
// whose real data never triggers it, because the probe feeds the marshaler a value
// it has never seen.

type panicJSON struct{ X int }

func (panicJSON) MarshalJSON() ([]byte, error) { panic("boom from MarshalJSON") }

type holdsPanicJSON struct {
	M panicJSON `json:"m"`
}

type panicText struct{ X int }

func (panicText) MarshalText() ([]byte, error) { panic("boom from MarshalText") }

type holdsPanicText struct {
	T panicText `json:"t"`
}

type errJSON struct{ X int }

func (errJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("nope") }

type holdsErrJSON struct {
	M errJSON `json:"m"`
}

// json cannot marshal a map keyed by a struct.
type structKeyed struct {
	M map[structKey]int `json:"m"`
}
type structKey struct{ K string }

// Recursive shapes must terminate rather than spin. Each of these nests one JSON
// level per hop, so the depth cap is what stops them — and REACHING the cap now
// records no shape at all rather than a truncated one, so all three are
// deliberately unguarded. That is a real, named coverage loss taken in exchange
// for retiring the false-fire family: see maxShapeDepth. It is the cheap
// direction — an unguarded type replays exactly as it did before this feature
// existed, while a false fire wedges a live workflow.
type recSlice struct {
	Kids []recSlice `json:"kids"`
	V    int        `json:"v"`
}
type recMap struct {
	M map[string]recMap `json:"m"`
	V int               `json:"v"`
}
type recPtr struct {
	Next *recPtr `json:"next"`
}

func TestResultShape_NeverPanicsAndAlwaysTerminates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     reflect.Type
		wantAny bool // true: must produce SOME shape; false: must fail open to ""
		// Every false row below is a type this guard deliberately does not cover.
	}{
		{"MarshalJSON panics", reflect.TypeOf(holdsPanicJSON{}), false},
		{"MarshalText panics", reflect.TypeOf(holdsPanicText{}), false},
		{"MarshalJSON errors", reflect.TypeOf(holdsErrJSON{}), false},
		{"map with a struct key", reflect.TypeOf(structKeyed{}), false},
		{"recursive via slice", reflect.TypeOf(recSlice{}), false},
		{"recursive via map", reflect.TypeOf(recMap{}), false},
		{"recursive via pointer", reflect.TypeOf(recPtr{}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("resultFingerprint panicked: %v — this crashes a live Call", r)
				}
			}()
			fp := ResultFingerprintForTest(tc.typ)
			if tc.wantAny && fp == "" {
				t.Errorf("expected a shape, got none")
			}
			if !tc.wantAny && fp != "" {
				t.Errorf("expected no shape (fail open), got %q", fp)
			}
		})
	}
}
