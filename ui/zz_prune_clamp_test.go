package ui

import (
	"strings"
	"testing"
	"time"
)

// A prune bound past SQLite's representable range must still produce a WHERE
// clause. An earlier draft of the range check returned an EMPTY predicate for a
// bound that "excludes nothing" — correct for pkg/storage, where every caller is
// a SELECT filter, and catastrophic here: PruneStats hands this straight to a
// DELETE, so an empty predicate is a table wipe rather than an over-inclusive
// read.
func TestStatsTimestampPredicate_OutOfRangeBoundStillRestrictsTheDelete(t *testing.T) {
	for name, bound := range map[string]time.Time{
		"far future": time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC),
		"far past":   time.Date(-500, 1, 1, 0, 0, 0, 0, time.UTC),
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
