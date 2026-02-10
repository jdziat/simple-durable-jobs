package fanout

import (
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// Sub creates a sub-job definition.
func Sub(jobType string, args any, opts ...queue.Option) SubJob {
	sj := SubJob{
		Type:    jobType,
		Args:    args,
		Retries: 3, // default
	}

	// Apply queue options to extract values
	queueOpts := queue.NewOptions()
	for _, opt := range opts {
		opt.Apply(queueOpts)
	}

	if queueOpts.Queue != "" && queueOpts.Queue != "default" {
		sj.Queue = queueOpts.Queue
	}
	if queueOpts.Priority != 0 {
		sj.Priority = queueOpts.Priority
	}
	if queueOpts.MaxRetries > 0 {
		sj.Retries = queueOpts.MaxRetries
	}

	return sj
}
