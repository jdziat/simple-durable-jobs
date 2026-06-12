package core

import "github.com/google/uuid"

// NewID returns a time-ordered UUIDv7 string (S07): the leading millisecond timestamp makes inserts append near the right edge of the clustered PK B-tree instead of scattering like random v4. Falls back to v4 only if the OS entropy source fails (NewV7's only error path).
func NewID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.New().String()
}
