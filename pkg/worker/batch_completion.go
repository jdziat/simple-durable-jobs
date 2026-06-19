package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

var errBatchCompletionClosed = errors.New("batch completion accumulator closed")

type batchCompletionResult struct {
	committed bool
	err       error
}

type batchCompletionSubmission struct {
	item   storage.BatchCompleteItem
	result chan batchCompletionResult
}

type batchCompleter struct {
	storage  batchCompleteStorage
	workerID string
	maxBatch int
	maxDelay time.Duration
	retry    RetryConfig
	logger   *slog.Logger
	baseCtx  context.Context

	submitCh  chan *batchCompletionSubmission
	closeCh   chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
}

func newBatchCompleter(
	baseCtx context.Context,
	storage batchCompleteStorage,
	workerID string,
	maxBatch int,
	maxDelay time.Duration,
	retry RetryConfig,
	logger *slog.Logger,
) *batchCompleter {
	// Defense-in-depth: the WithBatchCompletion option already clamps, but a
	// constructed-directly accumulator (tests, future callers) must stay bounded.
	if maxBatch < 1 {
		maxBatch = 1
	}
	if maxBatch > maxBatchCompletionBatch {
		maxBatch = maxBatchCompletionBatch
	}
	if maxDelay < minBatchCompletionDelay {
		maxDelay = minBatchCompletionDelay
	}
	if maxDelay > maxBatchCompletionDelay {
		maxDelay = maxBatchCompletionDelay
	}
	return &batchCompleter{
		storage:  storage,
		workerID: workerID,
		maxBatch: maxBatch,
		maxDelay: maxDelay,
		retry:    retry,
		logger:   logger,
		baseCtx:  baseCtx,
		submitCh: make(chan *batchCompletionSubmission, maxBatch),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (b *batchCompleter) Submit(jobID core.UUID, result []byte) (bool, error) {
	if b == nil || b.closed.Load() {
		return false, errBatchCompletionClosed
	}

	sub := &batchCompletionSubmission{
		item: storage.BatchCompleteItem{
			JobID:  jobID,
			Result: result,
		},
		result: make(chan batchCompletionResult, 1),
	}

	select {
	case b.submitCh <- sub:
	case <-b.closeCh:
		return false, errBatchCompletionClosed
	}

	select {
	case res := <-sub.result:
		return res.committed, res.err
	case <-b.doneCh:
		select {
		case res := <-sub.result:
			return res.committed, res.err
		default:
			return false, errBatchCompletionClosed
		}
	}
}

func (b *batchCompleter) Close() {
	if b == nil {
		return
	}
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.closeCh)
	})
}

func (b *batchCompleter) run() {
	defer close(b.doneCh)

	var (
		batch  []*batchCompletionSubmission
		timer  *time.Timer
		timerC <-chan time.Time
	)
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	startTimer := func() {
		if timer == nil {
			timer = time.NewTimer(b.maxDelay)
			timerC = timer.C
		}
	}
	add := func(sub *batchCompletionSubmission) {
		if len(batch) == 0 {
			startTimer()
		}
		batch = append(batch, sub)
	}
	flush := func(ctx context.Context) {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		b.flush(ctx, batch)
		batch = nil
	}
	flushOnClose := func() {
		b.closed.Store(true)
		for {
			select {
			case sub := <-b.submitCh:
				add(sub)
			default:
				ctx, cancel := context.WithTimeout(context.WithoutCancel(b.baseCtx), 5*time.Second)
				flush(ctx)
				cancel()
				return
			}
		}
	}
	defer stopTimer()

	for {
		select {
		case sub := <-b.submitCh:
			add(sub)
			if len(batch) >= b.maxBatch {
				flush(b.baseCtx)
			}
		case <-timerC:
			timer = nil
			timerC = nil
			flush(b.baseCtx)
		case <-b.closeCh:
			flushOnClose()
			return
		}
	}
}

func (b *batchCompleter) flush(ctx context.Context, batch []*batchCompletionSubmission) {
	items := make([]storage.BatchCompleteItem, len(batch))
	for i, sub := range batch {
		items[i] = sub.item
	}

	var committed []core.UUID
	err := retryWithBackoff(ctx, b.retry, func() error {
		var batchErr error
		committed, batchErr = b.storage.BatchComplete(ctx, b.workerID, items)
		return batchErr
	})
	if err != nil {
		for _, sub := range batch {
			sub.result <- batchCompletionResult{err: err}
		}
		return
	}

	committedSet := make(map[core.UUID]struct{}, len(committed))
	for _, id := range committed {
		committedSet[id] = struct{}{}
	}
	for _, sub := range batch {
		_, ok := committedSet[sub.item.JobID]
		sub.result <- batchCompletionResult{committed: ok}
	}
}
