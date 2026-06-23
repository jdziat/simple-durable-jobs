package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// Benchmarks for the hot storage paths. Run against the production backend to
// get representative numbers:
//
//	TEST_DATABASE_URL='postgres://...' go test -bench=. -benchmem ./pkg/storage/
//
// With no env var they run on in-memory SQLite (useful as a relative baseline,
// but not representative of the distributed FOR UPDATE SKIP LOCKED path).

func benchStorage(b *testing.B) *GormStorage {
	b.Helper()
	s := NewGormStorage(openTestDB(b))
	if err := s.Migrate(context.Background()); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	return s
}

func benchJob() *core.Job {
	return &core.Job{Type: "bench", Queue: "default", Args: []byte(`{"n":1}`)}
}

// BenchmarkEnqueue measures single-row insert throughput.
func BenchmarkEnqueue(b *testing.B) {
	s := benchStorage(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Enqueue(ctx, benchJob()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDequeueSingleWorker measures the dequeue (lock-and-claim) cost with a
// single worker and no contention.
func BenchmarkDequeueSingleWorker(b *testing.B) {
	s := benchStorage(b)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if err := s.Enqueue(ctx, benchJob()); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Dequeue(ctx, []string{"default"}, "w1"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDequeueContended measures dequeue throughput under concurrent
// workers contending for the same queue — the metric that matters for fleet
// scaling. Skipped on SQLite, which is single-writer by design.
func BenchmarkDequeueContended(b *testing.B) {
	s := benchStorage(b)
	if s.isSQLite {
		b.Skip("contended dequeue is only meaningful on Postgres/MySQL (SQLite is single-writer)")
	}
	ctx := context.Background()
	const seed = 50000
	for i := 0; i < seed; i++ {
		if err := s.Enqueue(ctx, benchJob()); err != nil {
			b.Fatal(err)
		}
	}
	var claimed atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	var n atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		wid := fmt.Sprintf("w-%d", n.Add(1))
		for pb.Next() {
			job, err := s.Dequeue(ctx, []string{"default"}, wid)
			if err != nil {
				b.Error(err)
				return
			}
			if job != nil {
				claimed.Add(1)
			}
		}
	})
}

// BenchmarkBatchComplete compares per-job completion against one batched
// completion for a fixed chunk of N leaf jobs. Seeding (enqueue+dequeue) is
// excluded from the timer. Divide ns/op by N for per-job cost; the ratio between
// the two sub-benchmarks is the batching speedup (dominated by fsync-per-commit
// on a real backend, so run with TEST_DATABASE_URL set).
func BenchmarkBatchComplete(b *testing.B) {
	const N = 200
	ctx := context.Background()

	seed := func(b *testing.B, s *GormStorage) []BatchCompleteItem {
		b.Helper()
		items := make([]BatchCompleteItem, N)
		for k := 0; k < N; k++ {
			j := benchJob()
			if err := s.Enqueue(ctx, j); err != nil {
				b.Fatalf("seed enqueue: %v", err)
			}
			got, err := s.Dequeue(ctx, []string{"default"}, "w")
			if err != nil || got == nil {
				b.Fatalf("seed dequeue: %v (nil=%v)", err, got == nil)
			}
			items[k] = BatchCompleteItem{JobID: got.ID, Result: []byte(`{"ok":true}`)}
		}
		return items
	}

	b.Run("per_job_CompleteWithResult", func(b *testing.B) {
		s := benchStorage(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			items := seed(b, s)
			b.StartTimer()
			for _, it := range items {
				if _, err := s.CompleteWithResult(ctx, it.JobID, "w", it.Result); err != nil {
					b.Fatalf("complete: %v", err)
				}
			}
		}
		b.ReportMetric(float64(N), "jobs/op")
	})

	b.Run("batched_BatchComplete", func(b *testing.B) {
		s := benchStorage(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			items := seed(b, s)
			b.StartTimer()
			if _, err := s.BatchComplete(ctx, "w", items); err != nil {
				b.Fatalf("batch complete: %v", err)
			}
		}
		b.ReportMetric(float64(N), "jobs/op")
	})
}
