package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
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
