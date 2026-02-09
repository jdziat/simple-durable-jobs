package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Queue manages job registration, enqueueing, and processing.
type Queue struct {
	storage       Storage
	handlers      map[string]*jobHandler
	scheduledJobs map[string]*ScheduledJob
	mu            sync.RWMutex

	// Hooks
	onStart    []func(context.Context, *Job)
	onComplete []func(context.Context, *Job)
	onFail     []func(context.Context, *Job, error)
	onRetry    []func(context.Context, *Job, int, error)

	// Event stream
	events    chan Event
	eventSubs []chan Event

	// Config
	determinism DeterminismMode
}

type jobHandler struct {
	fn         reflect.Value
	argsType   reflect.Type
	hasContext bool
}

// New creates a new Queue with the given storage backend.
func New(s Storage) *Queue {
	return &Queue{
		storage:     s,
		handlers:    make(map[string]*jobHandler),
		determinism: ExplicitCheckpoints,
		events:      make(chan Event, 1000),
	}
}

// Register registers a job handler function.
// The function must have signature: func(ctx context.Context, args T) error
func (q *Queue) Register(name string, fn any, opts ...Option) {
	q.mu.Lock()
	defer q.mu.Unlock()

	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		panic(fmt.Sprintf("jobs: handler for %q must be a function", name))
	}

	handler := &jobHandler{fn: fnVal}

	// Parse function signature
	numIn := fnType.NumIn()
	if numIn < 1 || numIn > 2 {
		panic(fmt.Sprintf("jobs: handler for %q must have 1-2 arguments", name))
	}

	argIdx := 0
	if fnType.In(0).Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
		handler.hasContext = true
		argIdx = 1
	}

	if argIdx < numIn {
		handler.argsType = fnType.In(argIdx)
	}

	// Validate return type - allow error or (T, error)
	numOut := fnType.NumOut()
	if numOut == 1 {
		if !fnType.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			panic(fmt.Sprintf("jobs: handler for %q must return error", name))
		}
	} else if numOut == 2 {
		if !fnType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			panic(fmt.Sprintf("jobs: handler for %q must return (T, error)", name))
		}
	} else {
		panic(fmt.Sprintf("jobs: handler for %q must return error or (T, error)", name))
	}

	q.handlers[name] = handler
}

// HasHandler checks if a handler is registered.
func (q *Queue) HasHandler(name string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.handlers[name]
	return ok
}

// Enqueue adds a job to the queue.
func (q *Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (string, error) {
	q.mu.RLock()
	_, ok := q.handlers[name]
	q.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("jobs: no handler registered for %q", name)
	}

	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("jobs: failed to marshal args: %w", err)
	}

	job := &Job{
		ID:         uuid.New().String(),
		Type:       name,
		Args:       argsBytes,
		Queue:      options.Queue,
		Priority:   options.Priority,
		MaxRetries: options.MaxRetries,
		Status:     StatusPending,
	}

	if options.Delay > 0 {
		runAt := time.Now().Add(options.Delay)
		job.RunAt = &runAt
	}
	if options.RunAt != nil {
		job.RunAt = options.RunAt
	}

	if err := q.storage.Enqueue(ctx, job); err != nil {
		return "", fmt.Errorf("jobs: failed to enqueue: %w", err)
	}

	return job.ID, nil
}

// Schedule registers a recurring job.
func (q *Queue) Schedule(name string, schedule Schedule, opts ...Option) {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	q.mu.Lock()
	if q.scheduledJobs == nil {
		q.scheduledJobs = make(map[string]*ScheduledJob)
	}
	q.scheduledJobs[name] = &ScheduledJob{
		Name:     name,
		Schedule: schedule,
		Options:  options,
	}
	q.mu.Unlock()
}

// Storage returns the underlying storage.
func (q *Queue) Storage() Storage {
	return q.storage
}

// SetDeterminism sets the default determinism mode.
func (q *Queue) SetDeterminism(mode DeterminismMode) {
	q.determinism = mode
}

// Placeholder types for later implementation
type Event interface {
	eventMarker()
}

type ScheduledJob struct {
	Name     string
	Schedule Schedule
	Args     any
	Options  *Options
}

type Schedule interface {
	Next(from time.Time) time.Time
}

type jobContextKey struct{}

type jobContext struct {
	job   *Job
	queue *Queue
}

func withJobContext(ctx context.Context, job *Job, q *Queue) context.Context {
	return context.WithValue(ctx, jobContextKey{}, &jobContext{job: job, queue: q})
}

func getJobContext(ctx context.Context) *jobContext {
	if jc, ok := ctx.Value(jobContextKey{}).(*jobContext); ok {
		return jc
	}
	return nil
}
