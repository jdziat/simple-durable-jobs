package call

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
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
