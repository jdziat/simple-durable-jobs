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

// TestStatsRepresentableBound_ReFacesWhatSQLiteCannotParse pins the two re-faces
// that a "mirror pkg/storage" rewrite DELETED.
//
// The rewrite added an instant clamp and dropped both face guards, so the ui copy
// ended up mirroring pkg/storage LESS faithfully than the version it replaced —
// it had neither. Each shape below has a perfectly representable INSTANT and only
// an unrenderable WALL, which is exactly what an instant-only check cannot see:
//
//   - an offset beyond ±14:00, which SQLite's date parser refuses outright;
//   - an instant in the last 14h of year 9999 on a positive face, whose wall is a
//     five-digit year — julianday() returns NULL and both lexical arms invert,
//     because "10000-" sorts BELOW "2026-".
//
// Either one made GetStatsHistory return nothing and PruneStats delete nothing,
// with no error anywhere.
func TestStatsRepresentableBound_ReFacesWhatSQLiteCannotParse(t *testing.T) {
	t.Run("offset beyond the parser cap is re-faced", func(t *testing.T) {
		bound := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("plus1500", 15*3600))
		got := statsRepresentableBound(bound)
		if _, off := got.Zone(); absStatsDuration(time.Duration(off)*time.Second) > statsMaxParsableFaceOffset {
			t.Fatalf("bound kept a face SQLite cannot parse (%s); julianday() returns NULL for it, "+
				"so every foreign-faced row is dropped", got.Format(time.RFC3339Nano))
		}
		if !got.Equal(bound) {
			t.Fatalf("re-facing must be INSTANT-PRESERVING: %s != %s", got, bound)
		}
	})

	t.Run("a five-digit WALL is re-faced even though the instant is in range", func(t *testing.T) {
		// Inside the ceil as an instant, but "10000-..." once rendered on +05:30.
		bound := time.Date(9999, 12, 31, 20, 0, 0, 0, time.UTC).In(time.FixedZone("plus0530", 5*3600+1800))
		if bound.After(statsRepresentableBoundCeil) {
			t.Fatal("FIXTURE BROKEN: this instant must be INSIDE the ceil, or it proves nothing")
		}
		got := statsRepresentableBound(bound)
		if got.Year() > 9999 {
			t.Fatalf("bound still renders a five-digit year (%s): both lexical arms invert and "+
				"julianday() goes NULL", got.Format(time.RFC3339Nano))
		}
	})
}

// TestStatsRepresentableBound_MatchesStorageOnEveryShape is the test that would
// have caught this class at the source: the two copies of the rule must agree.
//
// They cannot share code — package ui cannot reach pkg/storage's unexported
// helper — so the ONLY thing keeping them in step is a test that walks the same
// bound shapes through this copy and asserts the properties pkg/storage's copy
// guarantees. Divergence between the two, not any single bug, is what produced
// both round-31 and round-32 findings.
func TestStatsRepresentableBound_MatchesStorageOnEveryShape(t *testing.T) {
	for name, bound := range map[string]time.Time{
		"proto max":            protobufMaxTimestamp,
		"julianday null band":  time.Date(9999, 12, 31, 23, 59, 59, 999500000, time.UTC),
		"five-digit wall":      time.Date(9999, 12, 31, 20, 0, 0, 0, time.UTC).In(time.FixedZone("p0530", 5*3600+1800)),
		"unparsable face":      time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("p1500", 15*3600)),
		"unparsable face west": time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("m1500", -15*3600)),
		"far past":             time.Date(-500, 1, 1, 0, 0, 0, 0, time.UTC),
		"ordinary":             time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			got := statsRepresentableBound(bound)

			// 1. renderable as a four-digit year
			if got.Year() > 9999 || got.Year() < 1 {
				t.Fatalf("bound renders an unparsable year: %s", got.Format(time.RFC3339Nano))
			}
			// 2. on a face SQLite's parser accepts
			if _, off := got.Zone(); absStatsDuration(time.Duration(off)*time.Second) > statsMaxParsableFaceOffset {
				t.Fatalf("bound kept an unparsable face: %s", got.Format(time.RFC3339Nano))
			}
			// 3. inside the julianday-parsable range
			if got.After(statsRepresentableBoundCeil) || got.Before(statsRepresentableBoundFloor) {
				t.Fatalf("bound outside the representable range: %s", got.Format(time.RFC3339Nano))
			}
		})
	}
}
