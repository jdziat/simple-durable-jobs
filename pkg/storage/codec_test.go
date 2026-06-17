package storage

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	payloadcodec "github.com/jdziat/simple-durable-jobs/v3/pkg/codec"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

type markerXORCodec struct{}

func (markerXORCodec) Encode(plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return plaintext, nil
	}
	out := make([]byte, 4, 4+len(plaintext))
	copy(out, []byte("xor:"))
	for _, b := range plaintext {
		out = append(out, b^0xaa)
	}
	return out, nil
}

func (markerXORCodec) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 || !bytes.HasPrefix(stored, []byte("xor:")) {
		return stored, nil
	}
	out := make([]byte, 0, len(stored)-4)
	for _, b := range stored[4:] {
		out = append(out, b^0xaa)
	}
	return out, nil
}

func openCodecTestDBs(t *testing.T) map[string]*gorm.DB {
	t.Helper()

	out := make(map[string]*gorm.DB)
	dbPath := t.TempDir() + "/codec.db"
	sqliteDB, err := gorm.Open(sqlite.Open(dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err, "open sqlite codec test db")
	sqlDB, err := sqliteDB.DB()
	require.NoError(t, err, "get sqlite sql.DB")
	sqlDB.SetMaxOpenConns(2)
	t.Cleanup(func() { _ = sqlDB.Close() })
	out["sqlite"] = sqliteDB

	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		postgresDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open postgres codec test db")
		sqlDB, err := postgresDB.DB()
		require.NoError(t, err, "get postgres sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		cleanupCodecExternalDB(t, postgresDB)
		t.Cleanup(func() {
			cleanupCodecExternalDB(t, postgresDB)
			_ = sqlDB.Close()
		})
		out["postgres"] = postgresDB
	}

	if dsn := os.Getenv("TEST_MYSQL_URL"); dsn != "" {
		mysqlDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		require.NoError(t, err, "open mysql codec test db")
		sqlDB, err := mysqlDB.DB()
		require.NoError(t, err, "get mysql sql.DB")
		sqlDB.SetMaxOpenConns(2)
		sqlDB.SetMaxIdleConns(1)
		cleanupCodecExternalDB(t, mysqlDB)
		t.Cleanup(func() {
			cleanupCodecExternalDB(t, mysqlDB)
			_ = sqlDB.Close()
		})
		out["mysql"] = mysqlDB
	}

	return out
}

func cleanupCodecExternalDB(t testing.TB, db *gorm.DB) {
	t.Helper()
	for _, tbl := range []string{"signals", "checkpoints", "fan_outs", "queue_states", "jobs", "scheduled_fires", "leases"} {
		_ = db.Exec("DELETE FROM " + tbl).Error
	}
}

func TestGormStoragePayloadCodecRoundTrip(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := NewGormStorage(db, WithCodec(markerXORCodec{}))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			args := []byte(`{"msg":"hello"}`)
			job := &core.Job{Type: "codec.args", Args: args}
			require.NoError(t, s.Enqueue(ctx, job))

			rawArgs := rawJobBytes(t, db, job.ID, "args")
			require.NotEqual(t, args, rawArgs)

			dequeued, err := s.Dequeue(ctx, []string{"default"}, "worker-a")
			require.NoError(t, err)
			require.NotNil(t, dequeued)
			assert.Equal(t, args, dequeued.Args)

			result := []byte(`{"ok":true}`)
			require.NoError(t, s.SaveJobResult(ctx, dequeued.ID, "worker-a", result))
			rawResult := rawJobBytes(t, db, dequeued.ID, "result")
			require.NotEqual(t, result, rawResult)
			gotJob, err := s.GetJob(ctx, dequeued.ID)
			require.NoError(t, err)
			require.NotNil(t, gotJob)
			assert.Equal(t, result, gotJob.Result)

			completeJob := &core.Job{Type: "codec.complete", Args: []byte(`{"n":2}`)}
			require.NoError(t, s.Enqueue(ctx, completeJob))
			runningComplete, err := s.Dequeue(ctx, []string{"default"}, "worker-b")
			require.NoError(t, err)
			require.NotNil(t, runningComplete)
			completeResult := []byte(`{"complete":true}`)
			_, err = s.CompleteWithResult(ctx, runningComplete.ID, "worker-b", completeResult)
			require.NoError(t, err)
			rawCompleteResult := rawJobBytes(t, db, runningComplete.ID, "result")
			require.NotEqual(t, completeResult, rawCompleteResult)
			completed, err := s.GetJob(ctx, runningComplete.ID)
			require.NoError(t, err)
			require.NotNil(t, completed)
			assert.Equal(t, completeResult, completed.Result)

			cp := &core.Checkpoint{
				JobID:     dequeued.ID,
				CallIndex: 1,
				CallType:  "activity",
				Result:    []byte(`{"checkpoint":true}`),
			}
			require.NoError(t, s.SaveCheckpoint(ctx, cp))
			rawCheckpoint := rawCheckpointResult(t, db, cp.ID)
			require.NotEqual(t, cp.Result, rawCheckpoint)
			checkpoints, err := s.GetCheckpoints(ctx, dequeued.ID)
			require.NoError(t, err)
			require.Len(t, checkpoints, 1)
			assert.Equal(t, cp.Result, checkpoints[0].Result)

			payload1 := []byte(`{"signal":1}`)
			payload2 := []byte(`{"signal":2}`)
			payload3 := []byte(`{"signal":3}`)
			require.NoError(t, s.SendSignal(ctx, dequeued.ID, "ready", payload1))
			require.NoError(t, s.SendSignal(ctx, dequeued.ID, "ready", payload2))
			require.NoError(t, s.SendSignal(ctx, dequeued.ID, "ready", payload3))
			rawSignal := rawOldestSignalPayload(t, db, dequeued.ID, "ready")
			require.NotEqual(t, payload1, rawSignal)

			consumed, err := s.ConsumeSignal(ctx, dequeued.ID, "ready")
			require.NoError(t, err)
			require.NotNil(t, consumed)
			assert.Equal(t, payload1, consumed.Payload)

			drained, err := s.DrainSignals(ctx, dequeued.ID, "ready")
			require.NoError(t, err)
			require.Len(t, drained, 2)
			assert.Equal(t, payload2, drained[0].Payload)
			assert.Equal(t, payload3, drained[1].Payload)
		})
	}
}

func TestGormStorageSecretboxReadsLegacyPlaintextJob(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			plainStore := NewGormStorage(db)
			require.NoError(t, plainStore.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			args := []byte(`{"legacy":true}`)
			job := &core.Job{Type: "legacy.plaintext", Args: args}
			require.NoError(t, plainStore.Enqueue(ctx, job))

			var key [32]byte
			key[0] = 7
			codec, err := payloadcodec.NewSecretbox(key)
			require.NoError(t, err)
			encryptedStore := NewGormStorage(db, WithCodec(codec))

			dequeued, err := encryptedStore.Dequeue(ctx, []string{"default"}, "worker")
			require.NoError(t, err)
			require.NotNil(t, dequeued)
			assert.Equal(t, args, dequeued.Args)
		})
	}
}

func TestGormStorageTenantMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))

	job := &core.Job{
		Type:     "metadata.roundtrip",
		Args:     []byte(`{"ok":true}`),
		Tenant:   "tenant-a",
		Metadata: map[string]string{"env": "prod", "region": "us"},
	}
	require.NoError(t, s.Enqueue(ctx, job))

	got, err := s.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "tenant-a", got.Tenant)
	assert.Equal(t, map[string]string{"env": "prod", "region": "us"}, got.Metadata)
}

func TestGormStorageSecretboxBypassesTenantAndMetadata(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var key [32]byte
			for i := range key {
				key[i] = byte(i + 1)
			}
			codec, err := payloadcodec.NewSecretbox(key)
			require.NoError(t, err)
			s := NewGormStorage(db, WithCodec(codec))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			args := []byte(`{"secret":"plain-args"}`)
			job := &core.Job{
				Type:     "metadata.codec",
				Args:     args,
				Tenant:   "tenant-plain",
				Metadata: map[string]string{"env": "prod", "team": "billing"},
			}
			require.NoError(t, s.Enqueue(ctx, job))

			rawArgs := rawJobBytes(t, db, job.ID, "args")
			require.NotEqual(t, args, rawArgs)
			assert.NotContains(t, string(rawArgs), "plain-args")
			assert.Equal(t, "tenant-plain", rawJobString(t, db, job.ID, "tenant"))

			rawMetadata := rawJobString(t, db, job.ID, "metadata")
			assert.Contains(t, rawMetadata, `"env":"prod"`)
			assert.Contains(t, rawMetadata, `"team":"billing"`)
			assert.False(t, strings.Contains(rawMetadata, "sdjenc:"))

			found, total, err := s.SearchJobs(ctx, core.JobFilter{
				Tenant:       "tenant-plain",
				MetaContains: &core.MetadataMap{"env": "prod"},
				Limit:        10,
			})
			require.NoError(t, err)
			assert.Equal(t, int64(1), total)
			require.Len(t, found, 1)
			assert.Equal(t, job.ID, found[0].ID)
			assert.Equal(t, args, found[0].Args)
			assert.Equal(t, job.Metadata, found[0].Metadata)
		})
	}
}

// g12: checkpoint Error / ErrorCause must be encrypted at rest under a
// non-identity codec (they previously rode cleartext while job-level
// last_error/dead_letter_reason were encrypted). ErrorKind stays cleartext (it
// is a non-sensitive rehydration discriminator).
func TestGormStorageCodecEncryptsCheckpointError(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := NewGormStorage(db, WithCodec(markerXORCodec{}))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			job := &core.Job{Type: "codec.cp.error", Args: []byte(`{"n":1}`)}
			require.NoError(t, s.Enqueue(ctx, job))

			const (
				errText  = "sensitive: connection refused to 10.0.0.5:5432"
				causeTxt = "dial tcp 10.0.0.5:5432: connect: connection refused"
				kindTxt  = core.CheckpointErrorKindRetryAfter
			)
			cp := &core.Checkpoint{
				JobID:      job.ID,
				CallIndex:  1,
				CallType:   "activity",
				Error:      errText,
				ErrorCause: causeTxt,
				ErrorKind:  kindTxt,
			}
			require.NoError(t, s.SaveCheckpoint(ctx, cp))

			// Raw columns must NOT contain the plaintext (encrypted at rest), and
			// must carry the errTextTag the encrypting path applies.
			rawErr := rawCheckpointColumn(t, db, cp.ID, "error")
			rawCause := rawCheckpointColumn(t, db, cp.ID, "error_cause")
			rawKind := rawCheckpointColumn(t, db, cp.ID, "error_kind")
			assert.NotEqual(t, errText, rawErr)
			assert.NotContains(t, rawErr, "connection refused")
			assert.Contains(t, rawErr, errTextTag)
			assert.NotEqual(t, causeTxt, rawCause)
			assert.NotContains(t, rawCause, "connect")
			assert.Contains(t, rawCause, errTextTag)
			// ErrorKind is a non-sensitive discriminator and stays cleartext.
			assert.Equal(t, kindTxt, rawKind)

			// Normal decode path round-trips to the original plaintext.
			checkpoints, err := s.GetCheckpoints(ctx, job.ID)
			require.NoError(t, err)
			require.Len(t, checkpoints, 1)
			assert.Equal(t, errText, checkpoints[0].Error)
			assert.Equal(t, causeTxt, checkpoints[0].ErrorCause)
			assert.Equal(t, kindTxt, checkpoints[0].ErrorKind)
		})
	}
}

// Identity codec must not corrupt or tag the checkpoint error fields: they
// round-trip unchanged and are stored verbatim (no errTextTag).
func TestGormStorageIdentityCodecCheckpointErrorVerbatim(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := NewGormStorage(db) // identity / default codec
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			job := &core.Job{Type: "codec.cp.identity", Args: []byte(`{"n":2}`)}
			require.NoError(t, s.Enqueue(ctx, job))

			const (
				errText  = "plain error text"
				causeTxt = "plain cause text"
				kindTxt  = core.CheckpointErrorKindNoRetry
			)
			cp := &core.Checkpoint{
				JobID:      job.ID,
				CallIndex:  1,
				CallType:   "activity",
				Error:      errText,
				ErrorCause: causeTxt,
				ErrorKind:  kindTxt,
			}
			require.NoError(t, s.SaveCheckpoint(ctx, cp))

			// Stored verbatim, no tag.
			assert.Equal(t, errText, rawCheckpointColumn(t, db, cp.ID, "error"))
			assert.Equal(t, causeTxt, rawCheckpointColumn(t, db, cp.ID, "error_cause"))
			assert.NotContains(t, rawCheckpointColumn(t, db, cp.ID, "error"), errTextTag)

			// Round-trips unchanged.
			checkpoints, err := s.GetCheckpoints(ctx, job.ID)
			require.NoError(t, err)
			require.Len(t, checkpoints, 1)
			assert.Equal(t, errText, checkpoints[0].Error)
			assert.Equal(t, causeTxt, checkpoints[0].ErrorCause)
			assert.Equal(t, kindTxt, checkpoints[0].ErrorKind)
		})
	}
}

func rawCheckpointColumn(t *testing.T, db *gorm.DB, checkpointID core.UUID, column string) string {
	t.Helper()
	query := ""
	switch column {
	case "error":
		query = "SELECT error FROM checkpoints WHERE id = ?"
	case "error_cause":
		query = "SELECT error_cause FROM checkpoints WHERE id = ?"
	case "error_kind":
		query = "SELECT error_kind FROM checkpoints WHERE id = ?"
	default:
		t.Fatalf("unsupported checkpoints string column %q", column)
	}
	var out string
	require.NoError(t, db.Raw(query, checkpointID).Row().Scan(&out))
	return out
}

func rawJobBytes(t *testing.T, db *gorm.DB, jobID core.UUID, column string) []byte {
	t.Helper()
	query := ""
	switch column {
	case "args":
		query = "SELECT args FROM jobs WHERE id = ?"
	case "result":
		query = "SELECT result FROM jobs WHERE id = ?"
	default:
		t.Fatalf("unsupported jobs payload column %q", column)
	}
	var out []byte
	require.NoError(t, db.Raw(query, jobID).Row().Scan(&out))
	return out
}

func rawJobString(t *testing.T, db *gorm.DB, jobID core.UUID, column string) string {
	t.Helper()
	query := ""
	switch column {
	case "tenant":
		query = "SELECT tenant FROM jobs WHERE id = ?"
	case "metadata":
		query = "SELECT metadata FROM jobs WHERE id = ?"
	default:
		t.Fatalf("unsupported jobs string column %q", column)
	}
	var out string
	require.NoError(t, db.Raw(query, jobID).Row().Scan(&out))
	return out
}

func rawCheckpointResult(t *testing.T, db *gorm.DB, checkpointID core.UUID) []byte {
	t.Helper()
	var out []byte
	require.NoError(t, db.Raw("SELECT result FROM checkpoints WHERE id = ?", checkpointID).Row().Scan(&out))
	return out
}

func rawOldestSignalPayload(t *testing.T, db *gorm.DB, jobID core.UUID, name string) []byte {
	t.Helper()
	var out []byte
	require.NoError(t, db.Raw(
		"SELECT payload FROM signals WHERE job_id = ? AND name = ? ORDER BY created_at ASC LIMIT 1",
		jobID, name,
	).Row().Scan(&out))
	return out
}

func TestGormStorageCodecDoesNotTouchEmptyPayloads(t *testing.T) {
	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db, WithCodec(markerXORCodec{}))
	require.NoError(t, s.Migrate(ctx))

	job := &core.Job{Type: "empty.payload"}
	require.NoError(t, s.Enqueue(ctx, job))
	dequeued, err := s.Dequeue(ctx, []string{"default"}, "worker")
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Len(t, dequeued.Args, 0)

	require.NoError(t, s.SaveJobResult(ctx, dequeued.ID, "worker", nil))
	got, err := s.GetJob(ctx, dequeued.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.Result)

	cp := &core.Checkpoint{JobID: dequeued.ID, CallIndex: 1, CallType: "empty"}
	require.NoError(t, s.SaveCheckpoint(ctx, cp))
	checkpoints, err := s.GetCheckpoints(ctx, dequeued.ID)
	require.NoError(t, err)
	require.Len(t, checkpoints, 1)
	assert.Len(t, checkpoints[0].Result, 0)

	require.NoError(t, s.SendSignal(ctx, dequeued.ID, "empty", nil))
	sig, err := s.PeekSignal(ctx, dequeued.ID, "empty")
	require.NoError(t, err)
	require.NotNil(t, sig)
	assert.Nil(t, sig.Payload)
}

func TestGormStorageDecodeFailureIsWrapped(t *testing.T) {
	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db, WithCodec(failingDecodeCodec{}))
	require.NoError(t, s.Migrate(ctx))

	job := &core.Job{Type: "decode.fail", Args: []byte("plain")}
	require.NoError(t, NewGormStorage(db).Enqueue(ctx, job))

	_, err := s.GetJob(ctx, job.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrPayloadDecode)
	assert.Contains(t, err.Error(), job.ID)
}

type failingDecodeCodec struct{}

func (failingDecodeCodec) Encode(plaintext []byte) ([]byte, error) { return plaintext, nil }

func (failingDecodeCodec) Decode([]byte) ([]byte, error) {
	return nil, assert.AnError
}

func TestGormStorageCodecDecodesUIJobReadPaths(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := NewGormStorage(db, WithCodec(markerXORCodec{}))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			searchArgs := []byte(`{"ui":"search"}`)
			searchJob := &core.Job{Type: "ui.codec.search", Args: searchArgs}
			require.NoError(t, s.Enqueue(ctx, searchJob))
			require.NotEqual(t, searchArgs, rawJobBytes(t, db, searchJob.ID, "args"))

			found, total, err := s.SearchJobs(ctx, core.JobFilter{Type: searchJob.Type, Limit: 10})
			require.NoError(t, err)
			assert.Equal(t, int64(1), total)
			require.Len(t, found, 1)
			assert.Equal(t, searchArgs, found[0].Args)

			rootArgs := []byte(`{"ui":"root"}`)
			rootJob := &core.Job{Type: "ui.codec.root", Args: rootArgs}
			require.NoError(t, s.Enqueue(ctx, rootJob))
			childArgs := []byte(`{"ui":"child"}`)
			childJob := &core.Job{Type: "ui.codec.child", Args: childArgs, ParentJobID: &rootJob.ID}
			require.NoError(t, s.Enqueue(ctx, childJob))

			roots, total, err := s.GetWorkflowRoots(ctx, "", 10, 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, total, int64(1))
			var gotRoot *core.Job
			for _, job := range roots {
				if job.ID == rootJob.ID {
					gotRoot = job
					break
				}
			}
			require.NotNil(t, gotRoot)
			assert.Equal(t, rootArgs, gotRoot.Args)

			fanOut := &core.FanOut{
				ID:          core.NewID(),
				ParentJobID: rootJob.ID,
				TotalCount:  1,
				Strategy:    core.StrategyFailFast,
				Status:      core.FanOutPending,
			}
			require.NoError(t, s.CreateFanOut(ctx, fanOut))
			subArgs := []byte(`{"ui":"subjob"}`)
			subJob := &core.Job{
				Type:        "ui.codec.subjob",
				Args:        subArgs,
				ParentJobID: &rootJob.ID,
				FanOutID:    &fanOut.ID,
			}
			require.NoError(t, s.Enqueue(ctx, subJob))

			subJobs, err := s.GetSubJobsByFanOuts(ctx, []core.UUID{fanOut.ID})
			require.NoError(t, err)
			require.Len(t, subJobs, 1)
			assert.Equal(t, subArgs, subJobs[0].Args)

			retryArgs := []byte(`{"ui":"retry"}`)
			retryJob := &core.Job{Type: "ui.codec.retry", Args: retryArgs, Status: core.StatusFailed}
			require.NoError(t, s.Enqueue(ctx, retryJob))
			require.NotEqual(t, retryArgs, rawJobBytes(t, db, retryJob.ID, "args"))

			retried, err := s.RetryJob(ctx, retryJob.ID)
			require.NoError(t, err)
			require.NotNil(t, retried)
			assert.Equal(t, core.StatusPending, retried.Status)
			assert.Equal(t, retryArgs, retried.Args)
		})
	}
}

// ST-01: free-text args search must not error on Postgres (22021) or silently
// corrupt on MySQL when an encrypting codec stores non-UTF8 ciphertext in args.
// Under encryption the search is gated to exact job-ID; a free-text term matches
// nothing rather than erroring or returning unfiltered rows.
func TestSearchJobs_SecretboxArgsSearchGuarded(t *testing.T) {
	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var key [32]byte
			for i := range key {
				key[i] = byte(i + 1)
			}
			codec, err := payloadcodec.NewSecretbox(key)
			require.NoError(t, err)
			s := NewGormStorage(db, WithCodec(codec))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			job := &core.Job{Type: "secret.job", Queue: "default", Args: []byte(`{"needle":"findme"}`)}
			require.NoError(t, s.Enqueue(ctx, job))
			// A second job so the assert-zero below also catches a future "drop the
			// filter" regression (which would return the unfiltered set of 2), not
			// only the PG 22021 error — on MySQL the cast silently no-matches, so
			// row-count is the only signal there.
			require.NoError(t, s.Enqueue(ctx, &core.Job{Type: "secret.job2", Queue: "default", Args: []byte(`{"needle":"findme"}`)}))

			// Free-text over the (encrypted) args column: must NOT error and must NOT
			// return unfiltered rows — it returns no rows under encryption.
			jobs, total, err := s.SearchJobs(ctx, core.JobFilter{Search: "findme", Limit: 10})
			require.NoError(t, err, "args free-text search under secretbox must not error (PG 22021 / MySQL corruption)")
			assert.Equal(t, int64(0), total, "free-text args search returns nothing under encryption (not the unfiltered set)")
			assert.Empty(t, jobs)

			// Sanity: the jobs exist (an unfiltered list returns both), so the zero
			// above is the guard at work, not an empty table.
			_, allTotal, err := s.SearchJobs(ctx, core.JobFilter{Limit: 10})
			require.NoError(t, err)
			assert.Equal(t, int64(2), allTotal)

			// Exact job-ID search still works under encryption.
			jobs, total, err = s.SearchJobs(ctx, core.JobFilter{Search: job.ID.String(), Limit: 10})
			require.NoError(t, err)
			assert.Equal(t, int64(1), total)
			require.Len(t, jobs, 1)
			assert.Equal(t, job.ID, jobs[0].ID)
		})
	}
}
