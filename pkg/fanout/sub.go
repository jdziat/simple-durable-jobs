package fanout

import (
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
)

// Sub creates a sub-job definition. A queue.Timeout option bounds this
// sub-job's handler execution.
func Sub(jobType string, args any, opts ...queue.Option) SubJob {
	// Retries is deliberately NOT stamped here. Doing so made it non-zero for
	// every Sub()-built child, so buildSubJobs' `retries == 0` fallback could
	// never reach cfg.retries — making WithFanOutRetries 100% dead for Sub().
	sj := SubJob{
		Type: jobType,
		Args: args,
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
		sj.RetriesSet = queueOpts.RetriesSet()
		if sj.RetriesSet {
			sj.Retries = queueOpts.MaxRetries
		}
		sj.Timeout = queueOpts.Timeout
		sj.Determinism = queueOpts.Determinism
		sj.Delay = queueOpts.Delay
		sj.RunAt = queueOpts.RunAt
		sj.DedupOptionsIgnored = queueOpts.DedupRequested()
	}

	return sj
}
