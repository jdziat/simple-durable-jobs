package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
)

// WorkerOption configures a Worker.
type WorkerOption interface {
	ApplyWorker(*WorkerConfig)
}

type workerOptionFunc func(*WorkerConfig)

func (f workerOptionFunc) ApplyWorker(c *WorkerConfig) { f(c) }

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	Queues          map[string]int // queue name -> concurrency
	PollInterval    time.Duration
	WorkerID        string
	EnableScheduler bool
}

// Concurrency sets the concurrency for a queue.
func Concurrency(n int) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		for k := range c.Queues {
			c.Queues[k] = n
		}
	})
}

// WorkerQueue adds a queue to process with optional concurrency.
func WorkerQueue(name string, opts ...WorkerOption) WorkerOption {
	return workerOptionFunc(func(c *WorkerConfig) {
		if c.Queues == nil {
			c.Queues = make(map[string]int)
		}
		c.Queues[name] = 10 // default concurrency
		for _, opt := range opts {
			opt.ApplyWorker(c)
		}
	})
}

// Worker processes jobs from the queue.
type Worker struct {
	queue  *Queue
	config WorkerConfig
	logger *slog.Logger
	wg     sync.WaitGroup
}

// NewWorker creates a new worker.
func (q *Queue) NewWorker(opts ...WorkerOption) *Worker {
	config := WorkerConfig{
		Queues:       map[string]int{"default": 10},
		PollInterval: 100 * time.Millisecond,
		WorkerID:     uuid.New().String(),
	}

	for _, opt := range opts {
		opt.ApplyWorker(&config)
	}

	return &Worker{
		queue:  q,
		config: config,
		logger: slog.Default(),
	}
}

// Start begins processing jobs. Blocks until context is cancelled.
func (w *Worker) Start(ctx context.Context) error {
	queues := make([]string, 0, len(w.config.Queues))
	for q := range w.config.Queues {
		queues = append(queues, q)
	}

	totalConcurrency := 0
	for _, c := range w.config.Queues {
		totalConcurrency += c
	}

	jobsChan := make(chan *Job, totalConcurrency)

	for i := 0; i < totalConcurrency; i++ {
		w.wg.Add(1)
		go w.processLoop(ctx, jobsChan)
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobsChan)
			w.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			job, err := w.queue.storage.Dequeue(ctx, queues, w.config.WorkerID)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					w.logger.Error("failed to dequeue", "error", err)
				}
				continue
			}
			if job != nil {
				select {
				case jobsChan <- job:
				case <-ctx.Done():
				}
			}
		}
	}
}

func (w *Worker) processLoop(ctx context.Context, jobs <-chan *Job) {
	defer w.wg.Done()

	for job := range jobs {
		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *Job) {
	w.queue.mu.RLock()
	handler, ok := w.queue.handlers[job.Type]
	w.queue.mu.RUnlock()

	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		w.queue.storage.Fail(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Call start hooks
	for _, fn := range w.queue.onStart {
		fn(ctx, job)
	}

	err := w.executeHandler(ctx, job, handler)

	if err != nil {
		w.handleError(ctx, job, err)
	} else {
		w.queue.storage.Complete(ctx, job.ID, nil)
		for _, fn := range w.queue.onComplete {
			fn(ctx, job)
		}
	}
}

func (w *Worker) executeHandler(ctx context.Context, job *Job, handler *jobHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Create job context
	jobCtx := withJobContext(ctx, job, w.queue)

	// Load checkpoints for replay
	checkpoints, err := w.queue.storage.GetCheckpoints(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("failed to load checkpoints: %w", err)
	}
	jobCtx = withCallState(jobCtx, checkpoints)

	var args []reflect.Value

	if handler.hasContext {
		args = append(args, reflect.ValueOf(jobCtx))
	}

	if handler.argsType != nil {
		argVal := reflect.New(handler.argsType)
		if err := json.Unmarshal(job.Args, argVal.Interface()); err != nil {
			return fmt.Errorf("failed to unmarshal args: %w", err)
		}
		args = append(args, argVal.Elem())
	}

	results := handler.fn.Call(args)

	// Handle return values
	numOut := handler.fn.Type().NumOut()
	if numOut == 1 {
		if !results[0].IsNil() {
			return results[0].Interface().(error)
		}
	} else if numOut == 2 {
		if !results[1].IsNil() {
			return results[1].Interface().(error)
		}
	}
	return nil
}

func (w *Worker) handleError(ctx context.Context, job *Job, err error) {
	// Check for NoRetry
	var noRetry *NoRetryError
	if errors.As(err, &noRetry) {
		w.queue.storage.Fail(ctx, job.ID, err.Error(), nil)
		for _, fn := range w.queue.onFail {
			fn(ctx, job, err)
		}
		return
	}

	// Check for RetryAfter
	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		if job.Attempt < job.MaxRetries {
			retryAt := time.Now().Add(retryAfter.Delay)
			w.queue.storage.Fail(ctx, job.ID, err.Error(), &retryAt)
			for _, fn := range w.queue.onRetry {
				fn(ctx, job, job.Attempt, err)
			}
			return
		}
	}

	// Normal retry logic
	if job.Attempt < job.MaxRetries {
		backoff := w.calculateBackoff(job.Attempt)
		retryAt := time.Now().Add(backoff)
		w.queue.storage.Fail(ctx, job.ID, err.Error(), &retryAt)
		for _, fn := range w.queue.onRetry {
			fn(ctx, job, job.Attempt, err)
		}
	} else {
		w.queue.storage.Fail(ctx, job.ID, err.Error(), nil)
		for _, fn := range w.queue.onFail {
			fn(ctx, job, err)
		}
	}
}

func (w *Worker) calculateBackoff(attempt int) time.Duration {
	base := time.Second
	backoff := base * (1 << attempt)
	if backoff > time.Minute {
		backoff = time.Minute
	}
	return backoff
}
