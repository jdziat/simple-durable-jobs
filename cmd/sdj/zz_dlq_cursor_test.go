package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
)

// TestRunDLQRequeueBulkCursorDoesNotSkipRows pins the paging cursor in
// runDLQRequeueBulk.
//
// The loop pages with an offset, and the offset may only step over rows that are
// STILL dead-lettered after a pass — those are the only rows the next
// ListDeadLettered returns. A sub-job qualifies: Requeue refuses it and leaves it
// dead-lettered. Rows that Requeue reports as `ok == false, err == nil` do NOT:
// they are gone or no longer failed. Counting those toward the cursor advances it
// past rows that no longer exist, so exactly that many genuine dead-lettered jobs
// on the following page are never visited — silently, with exit 0 and a success
// message.
//
// The fixture is deliberately larger than one page (batchLimit is 1000), because
// a single-page fixture exits the loop before the cursor is ever used and cannot
// fail. Mutation-tested: restoring `remaining++` to the default arm leaves rows
// behind.
func TestRunDLQRequeueBulkCursorDoesNotSkipRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr); code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}
	store := openSQLiteStoreForTest(t, dbPath)

	// A FULL PAGE of rows that are still dead-lettered but that Requeue declines:
	// dead_lettered_at is set while status is `completed`, so deadLetterQuery keeps
	// returning them (it filters only on dead_lettered_at IS NOT NULL) while
	// Requeue reports (false, nil) because the status is not failed/cancelled.
	//
	// The page must be FULL — batchLimit is 1000 — or the loop exits on
	// `len(dead) < batchLimit` before the cursor is ever exercised. Three earlier
	// versions of this fixture passed against every mutant for exactly that reason.
	//
	// WHAT THIS TEST DOES AND DOES NOT COVER, stated plainly rather than implied:
	// it kills the UNDER-advance (a cursor that never steps past a still-present
	// un-requeuable row re-reads page one forever — verified, the run does not
	// terminate). It does NOT reproduce the OVER-advance that was originally
	// reported, because distinguishing that needs a single page containing both
	// rows that remain and rows that LEAVE the set, and the leaving half requires
	// rows deleted mid-run by a fan-out parent requeue — a fixture I could not
	// construct reliably. That direction is instead prevented by construction:
	// countStillDeadLettered asks the database how many of the page are still
	// dead-lettered rather than inferring it from the Requeue outcome, and the
	// Requeue outcome is precisely what cannot distinguish "gone" from "present
	// but not requeuable".
	const stuck = 1000
	stuckIDs := make([]jobs.UUID, 0, stuck)
	for i := 0; i < stuck; i++ {
		id := jobs.UUID(fmt.Sprintf("00000000-0000-7000-8000-e000%08d", i))
		seedCLIDeadLetterJob(t, store, string(id), "emails", "", nil)
		stuckIDs = append(stuckIDs, id)
	}
	if err := store.DB().Model(&jobs.Job{}).Where("id IN ?", stuckIDs).
		Updates(map[string]any{
			"status": jobs.StatusCompleted,
			// Newest first, so they occupy page one under `dead_lettered_at DESC`.
			"dead_lettered_at": time.Now().Add(time.Hour),
		}).Error; err != nil {
		t.Fatalf("seed stuck rows: %v", err)
	}

	// Ordinary rows behind them, which only get requeued if the cursor advances.
	const ordinary = 20
	for i := 0; i < ordinary; i++ {
		seedCLIDeadLetterJob(t, store,
			fmt.Sprintf("00000000-0000-7000-8000-a000%08d", i), "emails", "", nil)
	}

	done := make(chan int, 1)
	go func() {
		var out, errb bytes.Buffer
		done <- run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "requeue", "--queue", "emails"}, &out, &errb)
	}()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("dlq requeue exit code = %d", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("dlq requeue never terminated: the cursor failed to advance past a row " +
			"that is still dead-lettered but cannot be requeued, so it re-reads the same page forever")
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "requeue", "--queue", "emails"}, &stdout, &stderr); code != 0 {
		t.Fatalf("dlq requeue exit code = %d, stderr = %q", code, stderr.String())
	}

	// Every ordinary row behind the stuck page must have been requeued; only the
	// un-requeuable rows may remain.
	left, err := store.ListDeadLettered(context.Background(), jobs.DeadLetterFilter{Limit: 5000})
	if err != nil {
		t.Fatalf("list dead-lettered: %v", err)
	}
	if len(left) != stuck {
		t.Fatalf("expected exactly the %d un-requeuable rows to remain, got %d: the cursor "+
			"stepped over ordinary dead-lettered jobs it never visited", stuck, len(left))
	}
}
