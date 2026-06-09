package fanout

import (
	"testing"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/stretchr/testify/assert"
)

func TestSub_PrioritySet(t *testing.T) {
	noPriority := Sub("job", "args")
	assert.Equal(t, 0, noPriority.Priority)
	assert.False(t, noPriority.PrioritySet)

	zeroPriority := Sub("job", "args", queue.Priority(0))
	assert.Equal(t, 0, zeroPriority.Priority)
	assert.True(t, zeroPriority.PrioritySet)

	nonZeroPriority := Sub("job", "args", queue.Priority(7))
	assert.Equal(t, 7, nonZeroPriority.Priority)
	assert.True(t, nonZeroPriority.PrioritySet)
}
