package signal_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/signal"
	"github.com/stretchr/testify/assert"
)

func TestWaitingError_Error(t *testing.T) {
	// A named signal wait reports the signal name.
	named := &signal.WaitingError{Name: "approval"}
	assert.Equal(t, `job waiting for signal "approval"`, named.Error())
	assert.True(t, named.WorkflowWaiting())

	// The durable-timer sentinel reports the timer message instead.
	timer := &signal.WaitingError{Name: signal.SleepCheckpointType}
	assert.Equal(t, "job waiting for durable timer", timer.Error())
}

func TestIsWaitingError(t *testing.T) {
	assert.True(t, signal.IsWaitingError(&signal.WaitingError{Name: "x"}))
	assert.False(t, signal.IsWaitingError(errors.New("plain")))
	assert.False(t, signal.IsWaitingError(nil))
	// A wrapped WaitingError is not unwrapped by IsWaitingError (it type-asserts
	// the concrete type), documenting the contract.
	assert.False(t, signal.IsWaitingError(fmt.Errorf("wrap: %w", &signal.WaitingError{Name: "x"})))
}
