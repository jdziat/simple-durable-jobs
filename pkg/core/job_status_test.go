package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTerminalJobStatusesClassifyAllStatuses(t *testing.T) {
	require.Equal(t, []JobStatus{StatusCompleted, StatusFailed, StatusCancelled}, TerminalJobStatuses)

	nonTerminal := map[JobStatus]bool{
		StatusPending:  true,
		StatusRunning:  true,
		StatusRetrying: true,
		StatusWaiting:  true,
		StatusPaused:   true,
	}
	terminal := make(map[JobStatus]bool, len(TerminalJobStatuses))
	for _, status := range TerminalJobStatuses {
		require.Falsef(t, terminal[status], "duplicate terminal status %q", status)
		terminal[status] = true
		require.Truef(t, status.IsTerminal(), "%q should be terminal", status)
		require.Falsef(t, nonTerminal[status], "%q classified as both terminal and non-terminal", status)
	}

	classified := make(map[JobStatus]bool, len(AllJobStatuses))
	for _, status := range AllJobStatuses {
		require.Falsef(t, classified[status], "duplicate status in AllJobStatuses: %q", status)
		classified[status] = true

		isTerminal := status.IsTerminal()
		isNonTerminal := nonTerminal[status]
		require.NotEqualf(t, isTerminal, isNonTerminal, "%q must be classified exactly once", status)
	}

	require.Len(t, classified, len(terminal)+len(nonTerminal), "AllJobStatuses drifted without terminal/non-terminal classification")
}

// TestActiveDedupStatusesIsEveryNonTerminalStatus pins the set that four docs
// pages enumerate in prose:
//
//	docs/content/docs/api-reference/job-options.md
//	docs/content/docs/api-reference/types.md
//	docs/content/docs/migrating-from-river.md
//	docs/content/docs/migrating-from-asynq.md
//
// Those pages spent four releases asserting the OLD rule ("pending or running")
// after the set was widened, because nothing failed when the set changed. This
// test is that missing signal: if you edit ActiveDedupStatuses, update the four
// pages above in the same commit.
func TestActiveDedupStatusesIsEveryNonTerminalStatus(t *testing.T) {
	require.Equal(t, []JobStatus{
		StatusPending, StatusRunning, StatusRetrying, StatusWaiting, StatusPaused,
	}, ActiveDedupStatuses)

	// The prose does not just list statuses, it states a RULE: the key is held in
	// every non-terminal status and released only at terminal. Assert the rule,
	// so adding a new non-terminal status fails here too.
	held := make(map[JobStatus]bool, len(ActiveDedupStatuses))
	for _, status := range ActiveDedupStatuses {
		require.Falsef(t, held[status], "duplicate status in ActiveDedupStatuses: %q", status)
		require.Falsef(t, status.IsTerminal(), "terminal status %q holds the unique key", status)
		held[status] = true
	}
	for _, status := range AllJobStatuses {
		if !status.IsTerminal() {
			require.Truef(t, held[status], "non-terminal status %q does not hold the unique key, "+
				"contradicting the four docs pages that say every non-terminal status does", status)
		}
	}
}
