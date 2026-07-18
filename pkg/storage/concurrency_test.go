package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func uniqueSlotName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestConcurrencySlotLimitReleaseAndExpiry(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-1"), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-2"), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-3"), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.False(t, ok, "third live slot must be denied at limit 2")

	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-1"), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "re-acquiring the same job renews instead of double-counting")

	require.NoError(t, s.ReleaseConcurrencySlot(ctx, slot, testUUID("job-2")))
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-3"), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "released slot should free capacity")

	expiredSlot := slot + "-expired"
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  expiredSlot,
		JobID:     testUUID("expired-job"),
		WorkerID:  "worker-1",
		ExpiresAt: time.Now().Add(-time.Hour),
	}).Error)
	ok, err = s.TryAcquireConcurrencySlot(ctx, expiredSlot, testUUID("fresh-job"), "worker-2", 1, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "expired slots must not count against the cap")
}

func liveSlotCount(t *testing.T, s *GormStorage, slot string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, s.db.Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id <> ? AND expires_at >= ?", slot, core.NilUUID, time.Now()).
		Count(&n).Error)
	return n
}

// F2: an EXPIRED self-slot row (left behind by a crashed/OOM-killed worker) must
// NOT be renewed past the cap. The renew is idempotent only for a LIVE holder; an
// expired self-row must fall through to the cap check, and — once the cap has room
// — be RECLAIMED via upsert, not denied forever by its own stale row.
func TestTryAcquireConcurrencySlot_ExpiredSelfRowDoesNotBypassCap(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)
	jobX := testUUID("job-x")
	jobY := testUUID("job-y")

	// X holds the only slot (limit 1, used as a distributed mutex).
	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, jobX, "wA", 1, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	// X crashes: its row is left behind but expires (simulate by backdating).
	require.NoError(t, s.db.WithContext(ctx).Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id = ?", slot, jobX).
		Update("expires_at", time.Now().Add(-time.Hour)).Error)

	// Y takes the slot freed by X's expiry.
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, jobY, "wB", 1, time.Hour)
	require.NoError(t, err)
	require.True(t, ok, "Y takes the slot X freed by expiring")

	// X is re-dispatched: its expired leftover row must NOT be renewed past the cap
	// while Y holds the only slot (the over-admission bug).
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, jobX, "wA", 1, time.Hour)
	require.NoError(t, err)
	assert.False(t, ok, "expired self-row must not bypass the limit-1 cap; X must wait for Y")
	require.Equal(t, int64(1), liveSlotCount(t, s, slot), "exactly one live holder for a limit-1 cap")

	// Once Y releases, X reclaims its (still-expired) leftover row via the
	// create/upsert path — it must not be permanently denied by its own stale row.
	require.NoError(t, s.ReleaseConcurrencySlot(ctx, slot, jobY))
	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, jobX, "wA", 1, time.Hour)
	require.NoError(t, err)
	assert.True(t, ok, "X reclaims its expired leftover row once the cap has room")
	require.Equal(t, int64(1), liveSlotCount(t, s, slot))
}

func TestTryAcquireConcurrencySlotLiveCountUsesPlainCount(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Info)}
	s.db = s.db.Session(&gorm.Session{Logger: capture})

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-1"), "worker-1", 1, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-2"), "worker-2", 1, time.Hour)
	require.NoError(t, err)
	require.False(t, ok, "acquisition past the live-slot limit must be refused")

	var countStmt string
	for _, stmt := range capture.stmts {
		lower := strings.ToLower(stmt)
		if strings.Contains(lower, "count(") && strings.Contains(lower, "concurrency_slots") {
			countStmt = stmt
			break
		}
	}
	require.NotEmpty(t, countStmt, "live-slot admission must use an aggregate COUNT")
	upper := strings.ToUpper(countStmt)
	assert.NotContains(t, upper, "FOR UPDATE", "live-slot COUNT must not row-lock holders")
	assert.NotContains(t, upper, "SKIP LOCKED", "live-slot COUNT must not skip locked live holders")
}

func TestRenewConcurrencySlotRenewOnly(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, testUUID("job-live"), "worker-1", 1, time.Second)
	require.NoError(t, err)
	require.True(t, ok)

	var before core.ConcurrencySlot
	require.NoError(t, s.db.WithContext(ctx).
		Where("slot_name = ? AND job_id = ?", slot, testUUID("job-live")).
		First(&before).Error)

	renewed, err := s.RenewConcurrencySlot(ctx, slot, testUUID("job-live"), time.Hour)
	require.NoError(t, err)
	require.True(t, renewed)

	var after core.ConcurrencySlot
	require.NoError(t, s.db.WithContext(ctx).
		Where("slot_name = ? AND job_id = ?", slot, testUUID("job-live")).
		First(&after).Error)
	assert.True(t, after.ExpiresAt.After(before.ExpiresAt), "live slot renewal should advance expiry")
	assert.Equal(t, before.WorkerID, after.WorkerID, "renew-only update preserves the current worker id")

	require.NoError(t, s.ReleaseConcurrencySlot(ctx, slot, testUUID("job-live")))
	renewed, err = s.RenewConcurrencySlot(ctx, slot, testUUID("job-live"), time.Hour)
	require.NoError(t, err)
	assert.False(t, renewed, "released slots must not be resurrected by renewal")

	var heldRows int64
	require.NoError(t, s.db.WithContext(ctx).
		Model(&core.ConcurrencySlot{}).
		Where("slot_name = ? AND job_id = ?", slot, testUUID("job-live")).
		Count(&heldRows).Error)
	assert.Equal(t, int64(0), heldRows)
}

func TestConcurrencySlotConcurrentLastSlotRace(t *testing.T) {
	s := newConcurrentTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)

	ok, err := s.TryAcquireConcurrencySlot(ctx, slot, core.NewID(), "worker-1", 2, time.Hour)
	require.NoError(t, err)
	require.True(t, ok)

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			acquired, acquireErr := s.TryAcquireConcurrencySlot(
				ctx,
				slot,
				core.NewID(),
				fmt.Sprintf("worker-%d", i),
				2,
				time.Hour,
			)
			if acquireErr != nil {
				errs <- acquireErr
				return
			}
			results <- acquired
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	winners := 0
	for acquired := range results {
		if acquired {
			winners++
		}
	}
	assert.Equal(t, 1, winners, "exactly one racer should acquire the final slot")
}

func TestDeleteExpiredConcurrencySlotsPreservesLiveAndSentinel(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	slot := uniqueSlotName(t)
	now := time.Now().UTC()

	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     "",
		WorkerID:  "",
		ExpiresAt: time.Unix(0, 0).UTC(),
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     testUUID("expired-job"),
		WorkerID:  "worker-1",
		ExpiresAt: now.Add(-time.Hour),
	}).Error)
	require.NoError(t, s.db.WithContext(ctx).Create(&core.ConcurrencySlot{
		SlotName:  slot,
		JobID:     testUUID("live-job"),
		WorkerID:  "worker-1",
		ExpiresAt: now.Add(time.Hour),
	}).Error)

	deleted, err := s.DeleteExpiredConcurrencySlots(ctx, now)
	require.NoError(t, err)
	// DeleteExpiredConcurrencySlots is a global GC sweep; on a shared external
	// test DB it may also reap other tests' expired slots, so assert it removed
	// at least our expired row. The slot-scoped remaining check below is the
	// precise correctness assertion (sentinel + live row preserved).
	assert.GreaterOrEqual(t, deleted, int64(1))

	var remaining []core.ConcurrencySlot
	require.NoError(t, s.db.WithContext(ctx).
		Where("slot_name = ?", slot).
		Order("job_id ASC").
		Find(&remaining).Error)
	require.Len(t, remaining, 2)
	assert.Equal(t, core.NilUUID, remaining[0].JobID, "sentinel row must not be deleted")
	assert.Equal(t, testUUID("live-job"), remaining[1].JobID, "live held slot must not be deleted")
}

func TestDeleteExpiredConcurrencySlotsUsesDBClockOnDBBackends(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	capture := &stmtCaptureLogger{Interface: logger.Default.LogMode(logger.Info)}
	s.db = s.db.Session(&gorm.Session{DryRun: true, Logger: capture})
	s.isSQLite = false

	cutoff := time.Date(2099, time.January, 2, 3, 4, 5, 0, time.UTC)
	_, err := s.DeleteExpiredConcurrencySlots(ctx, cutoff)
	require.NoError(t, err)
	require.NotEmpty(t, capture.stmts)

	sql := strings.Join(capture.stmts, "\n")
	// Match both Postgres NOW() and MySQL NOW(6) (s.nowExpr() renders the
	// fractional form on MySQL); the point is the server clock, not the cutoff.
	assert.Contains(t, sql, "expires_at < NOW(", "DB-clock sweep must compare against server time")
	assert.NotContains(t, sql, "2099", "DB-clock sweep must ignore the caller cutoff")
}
