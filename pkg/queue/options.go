package queue

import (
	"errors"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
)

// Options holds configuration for job enqueueing and registration.
type Options struct {
	Queue string
	// Tenant identifies the tenant that owns an enqueued job.
	Tenant string
	// Metadata stores queryable string tags for an enqueued job.
	Metadata        *core.MetadataMap
	MaxMetadataSize int
	Priority        int
	MaxRetries      int
	Delay           time.Duration
	RunAt           *time.Time
	UniqueKey       string
	IdempotencyKey  string
	UniqueForTTL    time.Duration
	UniqueLockTTL   time.Duration
	Determinism     DeterminismMode
	determinismSet  bool
	prioritySet     bool
	windowedDedup   windowedDedupMode
	// Timezone is reserved for future use and is currently ignored; schedules evaluate in UTC.
	//
	// Deprecated: never read; schedules evaluate in UTC. Will be removed in v3.
	Timezone *time.Location
	Timeout  time.Duration // max wall time for handler execution; 0 means no limit
	Backoff  core.BackoffPolicy
}

// NewOptions creates Options with defaults.
func NewOptions() *Options {
	return &Options{
		Queue:           "default",
		MaxMetadataSize: DefaultMaxMetadataSize,
		Priority:        0,
		MaxRetries:      DefaultJobRetries,
	}
}

// Option modifies Options.
type Option interface {
	Apply(*Options)
}

// BatchEntry describes one job to enqueue through Queue.EnqueueBatch.
type BatchEntry struct {
	Name    string
	Args    any
	Options []Option
}

// Batch creates a batch enqueue entry.
func Batch(name string, args any, opts ...Option) BatchEntry {
	return BatchEntry{Name: name, Args: args, Options: opts}
}

type optionFunc func(*Options)

func (f optionFunc) Apply(o *Options) { f(o) }

type windowedDedupMode int

const (
	windowedDedupNone windowedDedupMode = iota
	windowedDedupIdempotencyKey
	windowedDedupUniqueFor
)

// QueueOpt sets the queue name.
func QueueOpt(name string) Option {
	return optionFunc(func(o *Options) {
		o.Queue = name
	})
}

// WithTenant sets the tenant that owns the job. The tenant is a label and a
// query/dequeue filter; it is validated for length only (unlike Queue, it is not
// charset-validated or case-normalized, so callers requiring canonical forms must
// normalize the value themselves). It does NOT scope deduplication: Unique and
// IdempotencyKey keys are matched fleet-wide, so namespace them per tenant (e.g.
// Unique("tenant:"+tenantID+":"+key)) if you need cross-tenant isolation.
func WithTenant(t string) Option {
	return optionFunc(func(o *Options) {
		o.Tenant = t
	})
}

// WithMetadata replaces the job metadata with a defensive copy of m.
// It replaces any metadata set by earlier WithMetadata or WithMeta options.
func WithMetadata(m map[string]string) Option {
	return optionFunc(func(o *Options) {
		o.Metadata = cloneMetadataMap(m)
	})
}

// WithMeta adds or replaces one metadata key/value pair.
func WithMeta(key, value string) Option {
	return optionFunc(func(o *Options) {
		if o.Metadata == nil {
			metadata := make(core.MetadataMap, 1)
			o.Metadata = &metadata
		}
		(*o.Metadata)[key] = value
	})
}

// WithMaxMetadataSize sets the maximum marshaled metadata size in bytes.
// A value <= 0 disables the limit (unlimited); the default is DefaultMaxMetadataSize.
func WithMaxMetadataSize(n int) Option {
	return optionFunc(func(o *Options) {
		o.MaxMetadataSize = n
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

// Unique ensures only one active job with this key exists. The key is matched
// fleet-wide, NOT per-tenant — WithTenant does not scope it, so two tenants
// enqueuing the same key collide and the second is suppressed. For per-tenant
// isolation, namespace the key yourself, e.g. Unique("tenant:"+tenantID+":"+key).
func Unique(key string) Option {
	return optionFunc(func(o *Options) {
		o.UniqueKey = key
	})
}

// IdempotencyKey deduplicates enqueue attempts with the same caller-supplied
// key for ttl. A duplicate enqueue during the window returns the original job
// ID without creating a second job.
//
// Like Unique, the key is matched fleet-wide, not per-tenant; namespace it (e.g.
// "tenant:"+tenantID+":"+key) if you need per-tenant idempotency.
func IdempotencyKey(key string, ttl time.Duration) Option {
	return optionFunc(func(o *Options) {
		o.IdempotencyKey = key
		o.UniqueForTTL = 0
		o.UniqueLockTTL = ttl
		o.windowedDedup = windowedDedupIdempotencyKey
	})
}

// UniqueFor deduplicates enqueue attempts with the same queue, job name, and
// canonical plaintext arguments for ttl. A duplicate enqueue during the window
// returns the original job ID without creating a second job.
func UniqueFor(ttl time.Duration) Option {
	return optionFunc(func(o *Options) {
		o.IdempotencyKey = ""
		o.UniqueForTTL = ttl
		o.UniqueLockTTL = ttl
		o.windowedDedup = windowedDedupUniqueFor
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
	DefaultJobRetries      = 2
	DefaultCallRetries     = 3
	DefaultMaxMetadataSize = 64 << 10

	ErrJobMetadataTooLarge = errors.New("jobs: job metadata exceeds size limit")
)

func cloneMetadata(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneMetadataMap(m map[string]string) *core.MetadataMap {
	cloned := cloneMetadata(m)
	if cloned == nil {
		return nil
	}
	metadata := core.MetadataMap(cloned)
	return &metadata
}
