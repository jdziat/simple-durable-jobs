package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func TestEnumMigrationLiteralsDoNotDrift(t *testing.T) {
	t.Parallel()

	// Adding an enum requires updating both core.AllX and the migration literal.
	assertEnumLiteralMatches(t, "job statuses", core.AllJobStatuses, jobStatuses)
	assertEnumLiteralMatches(t, "fan-out strategies", core.AllFanOutStrategies, fanOutStrategies)
	assertEnumLiteralMatches(t, "fan-out statuses", core.AllFanOutStatuses, fanOutStatuses)
}

func assertEnumLiteralMatches[T ~string](t *testing.T, name string, values []T, literal string) {
	t.Helper()

	for _, value := range values {
		assert.Contains(t, literal, "'"+string(value)+"'", name)
	}
	assert.Equal(t, len(values), strings.Count(literal, "'")/2, name)
}
