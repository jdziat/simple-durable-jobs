package handler

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecute_FailedOutput(t *testing.T) {
	cause := errors.New("handler failed")
	for _, tc := range []struct {
		name   string
		output any
		want   string
	}{
		{"path", "/tmp/checkouts/retained-edit", `handler failed; output: "/tmp/checkouts/retained-edit"`},
		{"nil", nil, cause.Error()},
		{"typed_nil", (*string)(nil), cause.Error()},
		{"unserializable", make(chan int), cause.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := NewHandler(func(context.Context) (any, error) { return tc.output, cause })
			require.NoError(t, err)
			result, err := h.Execute(context.Background(), nil)
			require.ErrorIs(t, err, cause)
			assert.EqualError(t, err, tc.want)
			assert.Nil(t, result)
		})
	}
	t.Run("zero_value", func(t *testing.T) {
		h, err := NewHandler(func(context.Context) (string, error) { return "", cause })
		require.NoError(t, err)
		_, err = h.Execute(context.Background(), nil)
		assert.Same(t, cause, err)
	})
	t.Run("bounded", func(t *testing.T) {
		h, err := NewHandler(func(context.Context) (string, error) {
			return strings.Repeat("x", security.MaxErrorMessageLength*2), cause
		})
		require.NoError(t, err)
		_, err = h.Execute(context.Background(), nil)
		require.ErrorIs(t, err, cause)
		assert.LessOrEqual(t, len(err.Error()), len(cause.Error())+len("; output: ")+security.MaxErrorMessageLength)
	})
}
