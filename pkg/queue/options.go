// Package queue provides the Queue orchestrator for the jobs package.
package queue

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// Options holds configuration for job enqueueing and registration.
type Options struct {
	Queue          string
	Priority       int
	MaxRetries     int
	Delay          time.Duration
	RunAt          *time.Time
	UniqueKey      string
	Determinism    DeterminismMode
	determinismSet bool
	prioritySet    bool
	// Timezone is reserved for future use and is currently ignored; schedules evaluate in UTC.
	Timezone *time.Location
	Timeout  time.Duration // max wall time for handler execution; 0 means no limit
	Backoff  core.BackoffPolicy
}

// NewOptions creates Options with defaults.
func NewOptions() *Options {
	return &Options{
		Queue:      "default",
		Priority:   0,
		MaxRetries: DefaultJobRetries,
	}
}

// Option modifies Options.
type Option interface {
	Apply(*Options)
}

type optionFunc func(*Options)

func (f optionFunc) Apply(o *Options) { f(o) }

// QueueOpt sets the queue name.
func QueueOpt(name string) Option {
	return optionFunc(func(o *Options) {
		o.Queue = name
	})
}

// Priority sets the job priority (higher = runs first).
func Priority(p int) Option {
	return optionFunc(func(o *Options) {
		o.Priority = p
		o.prioritySet = true
	})
}

// PrioritySet reports whether a Priority option was explicitly applied.
func (o *Options) PrioritySet() bool { return o.prioritySet }

// Retries sets the maximum retry count.
// Values are clamped to [0, MaxRetries] (100).
func Retries(n int) Option {
	return optionFunc(func(o *Options) {
		o.MaxRetries = security.ClampRetries(n)
	})
}

// Delay schedules the job to run after a duration.
func Delay(d time.Duration) Option {
	return optionFunc(func(o *Options) {
		o.Delay = d
	})
}

// At schedules the job to run at a specific time.
func At(t time.Time) Option {
	return optionFunc(func(o *Options) {
		o.RunAt = &t
	})
}

// Timeout sets the maximum wall time for handler execution.
// When the deadline expires, the handler's context is cancelled.
// 0 means no limit (default).
func Timeout(d time.Duration) Option {
	return optionFunc(func(o *Options) {
		o.Timeout = d
	})
}

// WithHandlerBackoff sets the retry backoff policy for this handler.
func WithHandlerBackoff(p core.BackoffPolicy) Option {
	return optionFunc(func(o *Options) {
		o.Backoff = p
	})
}

// Unique ensures only one job with this key exists.
func Unique(key string) Option {
	return optionFunc(func(o *Options) {
		o.UniqueKey = key
	})
}

// DeterminismMode controls how strictly Call replay is validated against the
// recorded checkpoints. The three modes form an increasing-strictness ladder.
type DeterminismMode int

const (
	// ExplicitCheckpoints (default) errors when a replayed Call's type does not
	// match the checkpoint recorded at that index. Extra Calls that were not in
	// the original run execute fresh, and recorded checkpoints the replay does
	// not reach are tolerated.
	ExplicitCheckpoints DeterminismMode = iota
	// Strict applies the ExplicitCheckpoints type check AND additionally
	// requires that every recorded Call checkpoint is replayed. If the handler
	// issues fewer or reordered Calls than the run that produced the
	// checkpoints, the job fails terminally (non-retryable) after the handler
	// returns. Use this to catch nondeterministic handlers early.
	Strict
	// BestEffort logs replay checkpoint type mismatches and re-executes the
	// call instead of erroring. The most permissive mode.
	BestEffort
)

// Determinism sets the Call replay mode.
func Determinism(mode DeterminismMode) Option {
	return optionFunc(func(o *Options) {
		o.Determinism = mode
		o.determinismSet = true
	})
}

// Default values.
var (
	DefaultJobRetries  = 2
	DefaultCallRetries = 3
)
