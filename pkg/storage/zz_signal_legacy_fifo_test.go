package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// FIFO delivery must survive the upgrade boundary.
//
// created_at is now written in UTC, but an upgraded database still holds rows sent
// by older releases wearing the sender's local offset, and a rolling deploy writes
// both faces at once. The consume paths order by created_at, which on SQLite is a
// LEXICAL compare — so normalizing only the WRITE side leaves the ordering wrong
// for exactly the rows that already exist.
//
// Measured before the fix, at TZ=Asia/Kolkata: a signal sent 30ms EARLIER by an
// older binary was delivered SECOND. Green at UTC and at -07:00, which is why the
// positive-offset CI leg is what makes this a gate.
func TestSignalFIFOSurvivesTheUpgradeBoundary(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("sqlite only")
	}
	job := &core.Job{Type: "p", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
	require.NoError(t, s.Enqueue(ctx, job))

	// FIRST, written exactly as every release before this one wrote it: the
	// sender's LOCAL clock face. Raw SQL because GORM would normalize a time.Time.
	old := time.Now().Add(-30 * time.Millisecond)
	text := old.Format("2006-01-02 15:04:05.999999999-07:00")
	require.NoError(t, s.db.WithContext(ctx).Exec(
		`INSERT INTO signals (id, job_id, name, payload, created_at) VALUES (?, ?, ?, ?, ?)`,
		core.NewID(), job.ID, "approval", []byte(`"FIRST"`), text).Error)

	// SECOND, written the NEW way (UTC), now.
	require.NoError(t, s.SendSignal(ctx, job.ID, "approval", json.RawMessage(`"SECOND"`)))

	got, err := s.ConsumeSignal(ctx, job.ID, "approval")
	require.NoError(t, err)
	require.NotNil(t, got)
	t.Logf("TZ=%s  first delivered = %s", time.Now().Location(), string(got.Payload))
	require.JSONEq(t, `"FIRST"`, string(got.Payload),
		"the signal sent FIRST — by an older binary, on the local clock face — was not "+
			"delivered first. core.Signal documents FIFO (CreatedAt) order, and an upgraded "+
			"database holds both faces at once, so the ORDER BY has to compare instants "+
			"rather than rendered text")
}
