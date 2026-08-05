package storage

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/require"
)

// A job whose payload the codec cannot decode is claimed, excluded from the batch
// and released — deliberately, so one poison row cannot strand every sibling. The
// code comment called that "a visible, self-contained symptom of a misconfigured
// codec", and it was not visible at all: no log, no metric, no hook. The row was
// re-claimed and re-failed forever and the only outward sign was a job that never
// progressed, so a key rotation could stall a whole queue with nothing to point at.
func TestDecodeClaimedBatch_PoisonPayloadIsReported(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	good := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusRunning, LockedBy: "w1", Args: []byte(`{"ok":1}`)}
	poison := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusRunning, LockedBy: "w1", Args: []byte(poisonMarker)}
	require.NoError(t, s.db.Create(good).Error)
	require.NoError(t, s.db.Create(poison).Error)
	// The codec only ever sees BYTES, never the job id — which is exactly the
	// position a real codec is in when a key rotation makes some rows unreadable.
	s.codec = poisonCodec{}

	before := s.PoisonPayloadDrops()
	out, err := s.decodeClaimedBatch(ctx, []*core.Job{good, poison}, "w1")
	require.NoError(t, err)

	require.Len(t, out, 1, "the good job must still be returned; a poison sibling must not strand it")
	require.Equal(t, good.ID, out[0].ID)

	require.Equal(t, before+1, s.PoisonPayloadDrops(),
		"every undecodable row must increment the counter, or an operator has no number to alert on")
	require.Contains(t, buf.String(), string(poison.ID),
		"the dropped job id must be named in the log; without it the symptom is a job that silently never runs")
	require.NotContains(t, buf.String(), string(good.ID),
		"a job that decoded fine must not be reported")
}

// The row re-fails on every claim, so an unconditional log would emit tens of
// lines per second per row. It must be logged once per id and counted every time.
func TestDecodeClaimedBatch_PoisonPayloadLogsOncePerJobButCountsEvery(t *testing.T) {
	ctx := context.Background()
	s := newTestStorage(t)
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	poison := &core.Job{ID: core.NewID(), Type: "wf", Queue: "default", Status: core.StatusRunning, LockedBy: "w1", Args: []byte(poisonMarker)}
	require.NoError(t, s.db.Create(poison).Error)
	s.codec = poisonCodec{}

	for i := 0; i < 5; i++ {
		_, err := s.decodeClaimedBatch(ctx, []*core.Job{poison}, "w1")
		require.NoError(t, err)
	}

	require.Equal(t, int64(5), s.PoisonPayloadDrops(),
		"every attempt must be counted, or a rising failure rate is invisible")
	// Count LINES mentioning the id, not occurrences: one line carries the id
	// twice (as job_id, and again inside the wrapped decode error).
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, string(poison.ID)) {
			lines++
		}
	}
	require.Equal(t, 1, lines,
		"the id must be logged ONCE per process; a line per claim is tens per second per row and becomes its own outage")
}

const poisonMarker = "UNREADABLE-BY-THIS-KEY"

// poisonCodec models a codec that cannot read certain stored rows — the shape a
// rotated or missing key produces. It sees bytes only, like a real one.
type poisonCodec struct{}

func (poisonCodec) Encode(plaintext []byte) ([]byte, error) { return plaintext, nil }

func (poisonCodec) Decode(stored []byte) ([]byte, error) {
	if bytes.Contains(stored, []byte(poisonMarker)) {
		return nil, errDecodeUnreadable
	}
	return stored, nil
}

var errDecodeUnreadable = errors.New("codec: cannot decrypt with the configured key")
