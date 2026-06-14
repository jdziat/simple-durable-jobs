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
