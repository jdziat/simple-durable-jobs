// Package queue provides the Queue orchestrator for the jobs package.
package queue

import (
	"time"

	"github.com/jdziat/simple-durable-jobs/pkg/security"
)

// Options holds configuration for job enqueueing and registration.
type Options struct {
	Queue       string
	Priority    int
	MaxRetries  int
	Delay       time.Duration
	RunAt       *time.Time
	UniqueKey   string
	Determinism DeterminismMode
	Timezone    *time.Location
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
	})
}

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

// Unique ensures only one job with this key exists.
func Unique(key string) Option {
	return optionFunc(func(o *Options) {
		o.UniqueKey = key
	})
}

// DeterminismMode controls replay strictness.
type DeterminismMode int

const (
	ExplicitCheckpoints DeterminismMode = iota
	Strict
	BestEffort
)

// Determinism sets the replay mode.
func Determinism(mode DeterminismMode) Option {
	return optionFunc(func(o *Options) {
		o.Determinism = mode
	})
}

// Default values.
var (
	DefaultJobRetries  = 2
	DefaultCallRetries = 3
)
