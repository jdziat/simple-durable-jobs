package ui

import (
	"strings"
	"testing"
	"time"
)

// protobufMaxTimestamp is timestamppb's maximum, and this repo's canonical
// open-ended upper bound: pkg/storage.representableBound handles it, and
// ui/zz_listjobs_window_test.go pins it for ListJobs.
var protobufMaxTimestamp = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

// TestStatsTimestampPredicate_ClampComparesTheInstantNotTheYear pins the
// distinction an earlier version of this clamp got wrong.
//
// That version tested `bound.Year() > 9999`. Every instant in the final HALF
// MILLISECOND of year 9999 passes that test — including protobufMaxTimestamp —
// while SQLite's julianday() already returns NULL from '…23:59:59.9995' upward,
// because it rounds the seconds fraction into a millisecond julian-day integer.
// So the clamp never fired for the one value most likely to be passed, the bind
// reached SQL unclamped, `julianday(timestamp) <= NULL` was NULL, and every row
// whose stored clock face differed from the bound's was dropped: half the
// throughput buckets silently missing, and a prune-everything cutoff that
// no-opped on most rows.
//
// The assertion is on the BIND, not on a row count, so it holds without a
// database and cannot pass by accident on a single-faced fixture.
func TestStatsTimestampPredicate_ClampComparesTheInstantNotTheYear(t *testing.T) {
	_, args := statsTimestampPredicate(true, "<=", protobufMaxTimestamp)
	if len(args) == 0 {
		t.Fatal("predicate carried no binds")
	}
	for i, a := range args {
		got, ok := a.(time.Time)
		if !ok {
			continue
		}
		if got.After(statsRepresentableBoundCeil) {
			t.Fatalf("bind[%d] = %s reached SQL unclamped; julianday() returns NULL from "+
				"…59.9995 upward, so this drops every foreign-faced row. A Year() test cannot "+
				"catch it — the whole final half-millisecond of 9999 is still year 9999",
				i, got.Format(time.RFC3339Nano))
		}
	}
}

// TestStatsTimestampPredicate_RangeCheckRunsOnEveryDialect pins that the clamp is
// NOT inside the SQLite arm.
//
// Putting it there is the same defect commit 2005256 fixed in pkg/storage:
// MySQL's DATETIME ends at 9999-12-31 23:59:59, so an unclamped far-future bound
// returns zero rows or fails the query outright. It was reintroduced here, and is
// pinned so there cannot be a third time.
func TestStatsTimestampPredicate_RangeCheckRunsOnEveryDialect(t *testing.T) {
	for _, isSQLite := range []bool{true, false} {
		_, args := statsTimestampPredicate(isSQLite, "<=", protobufMaxTimestamp)
		for i, a := range args {
			got, ok := a.(time.Time)
			if !ok {
				continue
			}
			if got.After(statsRepresentableBoundCeil) {
				t.Fatalf("isSQLite=%v: bind[%d] = %s is beyond what the backend can store; "+
					"the range check must run BEFORE the dialect split",
					isSQLite, i, got.Format(time.RFC3339Nano))
			}
		}
	}
}

// TestStatsTimestampPredicate_OutOfRangeBoundStillRestrictsTheDelete keeps the
// clamp from becoming an empty predicate again.
//
// An earlier draft returned "" for a bound that "excludes nothing" — correct for
// pkg/storage, where every caller is a SELECT filter, and catastrophic here:
// PruneStats hands this straight to a DELETE, so an empty predicate is a table
// wipe rather than an over-inclusive read.
func TestStatsTimestampPredicate_OutOfRangeBoundStillRestrictsTheDelete(t *testing.T) {
	for name, bound := range map[string]time.Time{
		"far future":  time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC),
		"far past":    time.Date(-500, 1, 1, 0, 0, 0, 0, time.UTC),
		"proto max":   protobufMaxTimestamp,
		"end of 9999": time.Date(9999, 12, 31, 23, 59, 59, 999500000, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			pred, args := statsTimestampPredicate(true, "<", bound)
			if pred == "" {
				t.Fatal("PruneStats passes this predicate to a DELETE: an empty predicate deletes EVERY row")
			}
			if !strings.Contains(pred, "timestamp") {
				t.Fatalf("predicate must still constrain the timestamp column, got %q", pred)
			}
			if len(args) == 0 {
				t.Fatal("a constraining predicate must carry its bind")
			}
		})
	}
}
