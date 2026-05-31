package handler

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Execute – result marshal failure (numOut == 2, json.Marshal of the result
// returns an error). A chan value is not JSON-marshalable, so building a
// Handler directly with a func returning (chan int, error) drives the
// "failed to marshal handler result" branch in Execute.
// ---------------------------------------------------------------------------

func TestHandler_Execute_ResultMarshalFailure_Gen(t *testing.T) {
	// func() (chan int, error) – two return values, first is unmarshalable.
	fn := func() (chan int, error) {
		return make(chan int), nil
	}
	h := &Handler{
		Fn:         reflect.ValueOf(fn),
		ArgsType:   nil,
		HasContext: false,
	}

	out, err := h.Execute(context.Background(), nil)
	require.Error(t, err)
	assert.Nil(t, out)
	assert.Contains(t, err.Error(), "failed to marshal handler result")
}

// ---------------------------------------------------------------------------
// Execute – defensive tail (numOut is neither 1 nor 2). Build a Handler
// directly with a func that returns zero values so the trailing
// "return nil, nil" fallback is exercised.
// ---------------------------------------------------------------------------

func TestHandler_Execute_DefensiveTail_ZeroReturn_Gen(t *testing.T) {
	fn := func(_ context.Context, _ testArgs) {}
	h := &Handler{
		Fn:         reflect.ValueOf(fn),
		ArgsType:   reflect.TypeOf(testArgs{}),
		HasContext: true,
	}

	out, err := h.Execute(context.Background(), []byte(`{"name":"x","value":1}`))
	require.NoError(t, err)
	assert.Nil(t, out)
}
