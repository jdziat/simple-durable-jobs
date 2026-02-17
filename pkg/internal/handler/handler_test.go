package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHandler_RejectsNil(t *testing.T) {
	_, err := NewHandler(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestNewHandler_RejectsTypedNil(t *testing.T) {
	var fn func(ctx context.Context, args string) error = nil
	_, err := NewHandler(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestNewHandler_AcceptsValidFunction(t *testing.T) {
	fn := func(ctx context.Context, args string) error {
		return nil
	}
	h, err := NewHandler(fn)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.True(t, h.Fn.IsValid())
	assert.False(t, h.Fn.IsNil())
}

func TestHandler_Execute_ReturnsErrorForInvalidFn(t *testing.T) {
	// Create a handler with a zero Fn value (simulating corruption)
	h := &Handler{}
	err := h.Execute(context.Background(), []byte("{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil or invalid")
}

func TestExecuteCall_ReturnsErrorForInvalidFn(t *testing.T) {
	// Create a handler with a zero Fn value
	h := &Handler{}
	_, err := ExecuteCall[string](context.Background(), h, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil or invalid")
}
