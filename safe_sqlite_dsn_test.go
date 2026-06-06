package jobs_test

import (
	"strings"
	"testing"

	jobs "github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
)

func TestSafeSQLiteDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		sep  string
	}{
		{
			name: "plain path",
			path: "jobs.db",
			sep:  "?",
		},
		{
			name: "existing query",
			path: "jobs.db?cache=shared",
			sep:  "&",
		},
		{
			name: "trailing question mark",
			path: "jobs.db?",
			sep:  "",
		},
		{
			name: "trailing ampersand",
			path: "jobs.db?cache=shared&",
			sep:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := jobs.SafeSQLiteDSN(tt.path)
			assert.True(t, strings.HasPrefix(got, tt.path+tt.sep))
			assert.Contains(t, got, "_journal_mode=WAL")
			assert.Contains(t, got, "_busy_timeout=5000")
			assert.Contains(t, got, "_txlock=immediate")
		})
	}
}
