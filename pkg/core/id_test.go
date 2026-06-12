package core

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIDReturnsUUIDv7String(t *testing.T) {
	id := NewID()

	assert.Len(t, id, 36)

	parsed, err := uuid.Parse(id)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), parsed.Version())
}

func TestNewIDLexicallyIncreases(t *testing.T) {
	id1 := NewID()
	id2 := NewID()

	assert.Less(t, id1, id2)
}

func TestNewIDUnique(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, n)

	for range n {
		id := NewID()
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = struct{}{}
	}

	assert.Len(t, seen, n)
}
