package ui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDocumentedStatsRetentionDefaultMatchesTheDocs pins the stats-retention
// default that docs/content/docs/embedded-ui.md and the README sample comment
// print as a literal number. Both said 7 days while the collector used 31.
// The value is not arbitrary: it must exceed the dashboard's longest throughput
// window (30d) or that window silently renders short.
func TestDocumentedStatsRetentionDefaultMatchesTheDocs(t *testing.T) {
	sc := NewStatsCollector(nil, nil)
	require.Equal(t, 31*24*time.Hour, sc.retention, "documented default stats retention")
	require.Greater(t, sc.retention, 30*24*time.Hour,
		"retention must exceed the dashboard's longest (30d) window, as embedded-ui.md explains")
}
