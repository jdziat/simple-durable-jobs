package queue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDocumentedRetriesDefaultMatchesTheDocs pins the retry default that
// docs/content/docs/api-reference/job-options.md prints as a literal number.
// That page said 3 while the code said 2. Update it in the same commit as any
// change here.
func TestDocumentedRetriesDefaultMatchesTheDocs(t *testing.T) {
	require.Equal(t, 2, DefaultJobRetries, "documented default for Retries(n)")
	require.Equal(t, DefaultJobRetries, NewOptions().MaxRetries,
		"the constant and the value NewOptions actually applies must agree")
}
