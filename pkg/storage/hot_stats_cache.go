package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"google.golang.org/protobuf/proto"
)

// The dashboard and metrics scrape read per-queue/per-status aggregates
// (GetQueueDepthStats, QueueDeadLetterCounts, QueueOldestPendingAt) that GROUP /
// MIN over the WHOLE jobs table with no history bound, so their cost grows as
// completed history accumulates. Many dashboard tabs polling every few seconds
// plus a Prometheus scrape multiply that unbounded scan. A short-TTL,
// single-flighted in-process cache collapses all of that to at most one aggregate
// per TTL per storage instance without changing any SQL (so the dequeue index and
// query plans are untouched) and without changing the numbers' meaning.
const (
	// defaultHotStatsCacheTTL bounds how stale a coalesced aggregate snapshot may
	// be. It sits below the dashboard's ~5s poll and the typical ~15s metrics
	// scrape, so each poll/scrape still sees <=TTL-stale data while concurrent
	// readers coalesce onto one DB aggregate.
	defaultHotStatsCacheTTL = 2 * time.Second
	// hotStatLoadTimeout bounds a single shared aggregate load. The load is
	// detached from the caller's context (below), so it needs its own deadline.
	hotStatLoadTimeout = 5 * time.Second
)

// hotStatCaches holds the per-aggregate coalescing caches. It lives behind a
// pointer on GormStorage so GormStorage stays comparable: the caches embed a
// sync.Mutex and slice/map-typed value fields, which are not comparable, and
// GormStorage must remain comparable (an exported concrete type — a struct
// becoming non-comparable is an api-compat break; see indexedMetadataKeys).
type hotStatCaches struct {
	queueDepth    hotStatCache[[]*jobsv1.QueueStats]
	deadLetter    hotStatCache[map[string]int]
	oldestPending hotStatCache[map[string]time.Time]
}

// hotStatCache is a single-flight, short-TTL cache for one aggregate. Concurrent
// misses collapse onto ONE DB round-trip; errors are never cached (a transient
// failure must not stick for the whole TTL); every returned value is a deep copy
// so a caller cannot mutate the shared snapshot or race a concurrent reader.
type hotStatCache[T any] struct {
	mu   sync.Mutex
	val  T
	good bool
	exp  time.Time
	load *hotStatLoad[T]
}

type hotStatLoad[T any] struct {
	done chan struct{}
	val  T
	err  error
}

// do returns the cached value when it is still fresh; otherwise it runs fn once,
// coalescing every concurrent miss onto that single call, caches a SUCCESSFUL
// result for ttl, and returns a deep copy via clone. A ttl <= 0 bypasses the
// cache entirely (each call runs fn) — used by tests that assert exact
// post-mutation counts.
func (c *hotStatCache[T]) do(ctx context.Context, ttl time.Duration, clone func(T) T, fn func(context.Context) (T, error)) (T, error) {
	if ttl <= 0 {
		return fn(ctx)
	}

	c.mu.Lock()
	if c.good && time.Now().Before(c.exp) {
		v := c.val
		c.mu.Unlock()
		return clone(v), nil
	}
	if c.load != nil {
		// A load is already in flight — wait for it instead of starting another.
		load := c.load
		c.mu.Unlock()
		<-load.done
		if load.err != nil {
			var zero T
			return zero, load.err
		}
		return clone(load.val), nil
	}
	// Become the single loader for this miss.
	load := &hotStatLoad[T]{done: make(chan struct{})}
	c.load = load
	c.mu.Unlock()

	// Run the query OUTSIDE the mutex (do not serialize all readers behind a slow
	// scan) and DETACHED from the caller's cancellation: the result feeds every
	// waiter, so one caller disconnecting must not cancel it — bound it with a
	// timeout instead. The call is defer-guarded so a panic in fn cannot skip the
	// finalize block below: that would leave c.load non-nil with load.done never
	// closed, wedging this aggregate forever and leaking every waiter goroutine. A
	// panic is converted to an error (never cached; surfaced to the caller and all
	// waiters), matching the worker's panic-as-failure posture.
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hotStatLoadTimeout)
	var val T
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("hot-stats load panicked: %v", r)
			}
		}()
		val, err = fn(loadCtx)
	}()
	cancel()

	c.mu.Lock()
	load.val, load.err = val, err
	if err == nil { // never cache an error
		c.val = val
		c.good = true
		c.exp = time.Now().Add(ttl)
	}
	c.load = nil
	close(load.done)
	c.mu.Unlock()

	if err != nil {
		var zero T
		return zero, err
	}
	return clone(val), nil
}

func cloneQueueStatsSlice(in []*jobsv1.QueueStats) []*jobsv1.QueueStats {
	if in == nil {
		return nil
	}
	out := make([]*jobsv1.QueueStats, len(in))
	for i, qs := range in {
		out[i] = proto.Clone(qs).(*jobsv1.QueueStats)
	}
	return out
}

func cloneStringIntMap(in map[string]int) map[string]int {
	if in == nil {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringTimeMap(in map[string]time.Time) map[string]time.Time {
	if in == nil {
		return nil
	}
	out := make(map[string]time.Time, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
