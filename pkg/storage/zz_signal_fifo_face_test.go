package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signals.created_at implements the documented FIFO delivery contract, so it must
// wear ONE clock face regardless of which process sent the signal.
//
// core.Signal's godoc promises delivery "in FIFO (CreatedAt) order per (JobID,
// Name)", and every consume path orders by `created_at ASC`. On SQLite that column
// is TEXT and the ORDER BY is a LEXICAL compare — so a value carrying the sender's
// local offset sorts against one carrying UTC by its digits, not its instant.
//
// It was written by GORM's autoCreateTime, i.e. a bare time.Now(). Two failure
// shapes, and the second is the one that matters:
//
//   - one host across a DST fall-back: inverts twice a year
//   - two senders on DIFFERENT offsets — a UTC container and a local-TZ host, which
//     is the ordinary shape of a mixed deployment — inverts PERMANENTLY
//
// The sibling column is the tell: consumed_at already goes through nowWriteValue().
//
// Like the other clock-face guards here this is vacuously green under TZ=UTC, which
// is what CI runners use; the TZ=America/Los_Angeles leg in the SQLite matrix is
// what makes it a gate.
func TestSignalCreatedAtWritesAFaceIndependentTimestamp(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	if !s.isSQLite {
		t.Skip("stored clock faces are a SQLite storage property")
	}

	job := &core.Job{Type: "sig.fifo", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.SendSignal(ctx, job.ID, "approval", json.RawMessage(`"FIRST"`)))

	var raw string
	require.NoError(t, s.db.WithContext(ctx).Raw(
		"SELECT CAST(created_at AS TEXT) FROM signals WHERE job_id = ?", job.ID).Scan(&raw).Error)
	_, hostOffset := time.Now().Zone()
	t.Logf("TZ offset %ds -> stored signals.created_at TEXT = %q", hostOffset, raw)

	assert.True(t, strings.HasSuffix(raw, "+00:00") || strings.HasSuffix(raw, "Z"),
		"signals.created_at must wear one face regardless of the sender's zone (got %q); "+
			"it is the ORDER BY that decides FIFO delivery and is compared lexically", raw)
}

// The behavioural consequence, asserted separately so a failure says which layer
// broke: signals sent in order must be DELIVERED in order, whatever zone the
// senders are in.
//
// This drives the production send and consume paths rather than inspecting a
// column, because FIFO is the user-visible promise and the column is only how it is
// implemented.
func TestSignalsAreDeliveredInSendOrder(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)

	job := &core.Job{Type: "sig.fifo", Queue: "default", Args: []byte(`{}`), MaxRetries: 3}
	require.NoError(t, s.Enqueue(ctx, job))
	require.NoError(t, s.db.WithContext(ctx).Model(&core.Job{}).
		Where("id = ?", job.ID).
		Updates(map[string]any{"status": core.StatusRunning, "locked_by": "worker-A"}).Error)

	require.NoError(t, s.SendSignal(ctx, job.ID, "approval", json.RawMessage(`"FIRST"`)))
	// A gap large enough that ordering is unambiguous, small enough to stay fast.
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, s.SendSignal(ctx, job.ID, "approval", json.RawMessage(`"SECOND"`)))

	first, err := s.ConsumeSignal(ctx, job.ID, "approval")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.JSONEq(t, `"FIRST"`, string(first.Payload),
		"the signal sent first must be delivered first; core.Signal documents FIFO "+
			"(CreatedAt) order per (JobID, Name)")

	second, err := s.ConsumeSignal(ctx, job.ID, "approval")
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.JSONEq(t, `"SECOND"`, string(second.Payload))
}
