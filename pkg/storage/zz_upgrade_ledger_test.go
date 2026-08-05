package storage

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const upgradeLedgerPath = "../../UPGRADE.md"

// TestUpgradeLedgerNamesEveryMigration pins UPGRADE.md's Rollback ledger to the
// migration registry.
//
// UPGRADE.md is the one artifact an operator uses to plan a schema upgrade, and it
// ships INSIDE the immutable module zip — once a version is tagged, a wrong ledger
// can only be corrected by cutting another release. Migration 41 was added and the
// ledger was left claiming five migrations with the table ending at v40; a CTO
// review caught it hours before the tag.
//
// Nothing tied the two together, which is why it drifted. The repo already applies
// exactly this technique to godoc and the docs site (zz_documented_signatures_test.go,
// zz_documented_api_reference_test.go, zz_events_doc_test.go); this extends it to the
// migration ledger, the one place where being wrong is permanent.
func TestUpgradeLedgerNamesEveryMigration(t *testing.T) {
	b, err := os.ReadFile(upgradeLedgerPath)
	require.NoErrorf(t, err, "cannot read %s; if it moved, move this guard with it", upgradeLedgerPath)
	page := string(b)

	head := len(schemaMigrations)
	require.NotZero(t, head, "no migrations registered; this guard would be vacuous")

	// The ledger documents the CURRENT release line. Every migration at or above the
	// line's first entry must appear as a **vNN** row.
	rows := regexp.MustCompile(`\*\*v(\d+)\*\*`).FindAllStringSubmatch(page, -1)
	documented := map[string]bool{}
	lowest := head
	for _, m := range rows {
		documented[m[1]] = true
		var n int
		if _, err := fmt.Sscanf(m[1], "%d", &n); err == nil && n < lowest {
			lowest = n
		}
	}
	require.NotEmpty(t, rows, "%s has no **vNN** ledger rows at all", upgradeLedgerPath)

	for n := lowest; n <= head; n++ {
		require.Truef(t, documented[fmt.Sprint(n)],
			"%s documents migrations v%d..v%d but omits **v%d**; the registry head is %d. "+
				"This file ships inside the immutable module zip, so a wrong ledger cannot be "+
				"corrected after the tag — only superseded by another release.",
			upgradeLedgerPath, lowest, head, n, head)
	}

	// The prose count must match the row count for this line, or the table and the
	// sentence above it disagree — which is how the last drift read as correct.
	inLine := head - lowest + 1
	words := map[int]string{4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine"}
	if w, ok := words[inLine]; ok {
		require.Containsf(t, strings.ToLower(page), w+" forward-only migrations",
			"%s should say %q forward-only migrations for v%d..v%d", upgradeLedgerPath, w, lowest, head)
	}
}
