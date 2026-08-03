package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// TestRetentionPinIsFaceIndependent pins the live-window guard against the clock
// face its row was written on.
//
// SQLite stores timestamps as offset-suffixed TEXT and compares them LEXICALLY,
// so a bare `expires_at > ?` is only meaningful while the stored row and the
// bound value share a trailing offset. Production writes expires_at UTC-faced,
// but a legacy row, another tool or a direct Create can carry any face — and the
// failure is silent and one-directional: the guard stops matching, retention
// collects the job, and a live idempotency window is destroyed. That is the
// double charge the guard exists to prevent.
//
// Measured before the fix: a live 1h window written on a LOCAL face did not pin
// its job on SQLite (deleted=1) while pinning correctly on Postgres and MySQL —
// so SQLite was the false green and the backend legs are what exposed it.
func TestRetentionPinIsFaceIndependent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expires time.Time
	}{
		{"UTC-faced", time.Now().UTC().Add(time.Hour)},
		{"local-faced", time.Now().Add(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := newTestStorage(t)
			old := time.Now().Add(-2 * time.Hour).UTC()
			id := core.NewID()
			require.NoError(t, s.db.Create(&core.Job{
				ID: id, Type: "wf", Queue: "default",
				Status: core.StatusCompleted, CompletedAt: &old,
			}).Error)
			require.NoError(t, s.db.Create(&core.UniqueLock{
				ScopeHash: "scope-" + tc.name, JobID: id, ExpiresAt: tc.expires,
			}).Error)

			deleted, err := s.DeleteTerminalJobsOlderThan(ctx, core.StatusCompleted, time.Hour, 100)
			require.NoError(t, err)
			t.Logf("%s: deleted=%d (0 = pinned correctly, 1 = PIN FAILED)", tc.name, deleted)
			require.Equalf(t, int64(0), deleted,
				"a live window written on a %s clock did not pin its job", tc.name)
		})
	}
}
