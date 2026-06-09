package fanout

import (
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
)

// Sub creates a sub-job definition. A queue.Timeout option bounds this
// sub-job's handler execution.
func Sub(jobType string, args any, opts ...queue.Option) SubJob {
	sj := SubJob{
		Type:    jobType,
		Args:    args,
		Retries: queue.DefaultJobRetries,
	}

	// Apply queue options to extract values
	if len(opts) > 0 {
		queueOpts := &queue.Options{MaxRetries: queue.DefaultJobRetries}
		for _, opt := range opts {
			opt.Apply(queueOpts)
		}

		if queueOpts.Queue != "" {
			sj.Queue = queueOpts.Queue
		}
		sj.Priority = queueOpts.Priority
		sj.PrioritySet = queueOpts.PrioritySet()
		sj.Retries = queueOpts.MaxRetries
		sj.Timeout = queueOpts.Timeout
	}

	return sj
}
