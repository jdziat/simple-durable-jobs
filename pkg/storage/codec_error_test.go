package storage

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	payloadcodec "github.com/jdziat/simple-durable-jobs/v4/pkg/codec"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
)

// rawJobErrorText reads last_error / dead_letter_reason directly via SQL as a
// string, bypassing the codec funnel, so tests can assert on the at-rest bytes.
func rawJobErrorText(t *testing.T, db *gorm.DB, jobID core.UUID, column string) string {
	t.Helper()
	query := ""
	switch column {
	case "last_error":
		query = "SELECT last_error FROM jobs WHERE id = ?"
	case "dead_letter_reason":
		query = "SELECT dead_letter_reason FROM jobs WHERE id = ?"
	default:
		t.Fatalf("unsupported jobs error column %q", column)
	}
	var out string
	require.NoError(t, db.Raw(query, jobID).Row().Scan(&out))
	return out
}

// failTerminally enqueues a MaxRetries:1 job, dequeues it, and Fails it once so
// the single attempt exhausts retries → terminal dead-letter write.
func failTerminally(t *testing.T, ctx context.Context, s *GormStorage, errMsg string) core.UUID {
	t.Helper()
	job := &core.Job{Type: "codec.err", Queue: "default", MaxRetries: 1}
	require.NoError(t, s.Enqueue(ctx, job))
	got, err := s.Dequeue(ctx, []string{"default"}, "worker-err")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NoError(t, s.Fail(ctx, got.ID, "worker-err", errMsg, nil))
	return got.ID
}

func TestErrorTextCodecRoundTrip(t *testing.T) {
	const errMsg = "database password=secret exploded"
	// "exploded" survives sanitization (password=secret is redacted), so a raw
	// column that omits it proves the codec actually encrypted the cleartext.
	sanitized := security.SanitizeErrorMessage(errMsg)
	require.Contains(t, sanitized, "exploded")

	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := NewGormStorage(db, WithCodec(markerXORCodec{}))
			require.NoError(t, s.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			id := failTerminally(t, ctx, s, errMsg)

			rawLast := rawJobErrorText(t, db, id, "last_error")
			assert.True(t, strings.HasPrefix(rawLast, errTextTag),
				"raw last_error should begin with the %q tag, got %q", errTextTag, rawLast)
			assert.NotContains(t, rawLast, "exploded")
			assert.NotContains(t, rawLast, "password=secret")

			rawReason := rawJobErrorText(t, db, id, "dead_letter_reason")
			assert.Contains(t, rawReason, "max retries exhausted:")
			assert.Contains(t, rawReason, errTextTag)
			assert.NotContains(t, rawReason, "exploded")
			assert.NotContains(t, rawReason, "password=secret")

			got, err := s.GetJob(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, got)
			// Round-trips to the sanitized form actually stored, not the raw input.
			assert.Equal(t, sanitized, got.LastError)
			assert.Contains(t, got.DeadLetterReason, "max retries exhausted:")
			assert.Contains(t, got.DeadLetterReason, sanitized)
		})
	}
}

func TestErrorTextLegacyPlaintextReadback(t *testing.T) {
	const errMsg = "legacy plaintext boom"
	sanitized := security.SanitizeErrorMessage(errMsg)

	for name, db := range openCodecTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			// Write the failed job as legacy plaintext via the default store.
			plainStore := NewGormStorage(db)
			require.NoError(t, plainStore.Migrate(ctx))
			t.Cleanup(func() { cleanupCodecExternalDB(t, db) })

			id := failTerminally(t, ctx, plainStore, errMsg)

			// Sanity: at rest it is genuinely plaintext (no tag).
			rawLast := rawJobErrorText(t, db, id, "last_error")
			assert.Equal(t, sanitized, rawLast)
			assert.NotContains(t, rawLast, errTextTag)

			var key [32]byte
			key[0] = 11
			codec, err := payloadcodec.NewSecretbox(key)
			require.NoError(t, err)
			encryptedStore := NewGormStorage(db, WithCodec(codec))

			got, err := encryptedStore.GetJob(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, got)
			// idx<0 passthrough: legacy plaintext reads back unchanged.
			assert.Equal(t, sanitized, got.LastError)
			assert.Contains(t, got.DeadLetterReason, "max retries exhausted:")
			assert.Contains(t, got.DeadLetterReason, sanitized)
		})
	}
}

func TestErrorTextIdentityCodecUnchanged(t *testing.T) {
	const errMsg = "identity codec boom"
	sanitized := security.SanitizeErrorMessage(errMsg)

	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))

	id := failTerminally(t, ctx, s, errMsg)

	rawLast := rawJobErrorText(t, db, id, "last_error")
	assert.Equal(t, sanitized, rawLast)
	assert.NotContains(t, rawLast, errTextTag)

	rawReason := rawJobErrorText(t, db, id, "dead_letter_reason")
	assert.Equal(t, "max retries exhausted: "+sanitized, rawReason)
	assert.NotContains(t, rawReason, errTextTag)
}

func TestErrorTextCoincidentalTagPassthrough(t *testing.T) {
	// A genuine plaintext error that merely contains the tag substring but is
	// NOT followed by valid base64 must read back unchanged (soft passthrough).
	const errMsg = "oops sdjenc:not!base64!!"
	sanitized := security.SanitizeErrorMessage(errMsg)
	require.Contains(t, sanitized, errTextTag)

	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))

	id := failTerminally(t, ctx, s, errMsg)

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, sanitized, got.LastError)
}

func TestErrorTextIdentityCodecValidBase64NotCorrupted(t *testing.T) {
	// Regression (teardown g12): a genuine plaintext error that contains the tag
	// followed by a VALID base64 token would, before the fix, be base64-decoded
	// and run through codec.Decode under the identity codec — corrupting the
	// message ("...sdjenc:YWJjZGVm" -> "...abcdef"). The identity codec never
	// emits the tag, so such a value is always literal plaintext and must
	// round-trip verbatim; decodeErrorText now skips the tag scan for it.
	const errMsg = "boom sdjenc:YWJjZGVm"
	sanitized := security.SanitizeErrorMessage(errMsg)

	// Guard that this input actually exercises the dangerous decode path: the
	// sanitized value must still carry the tag with a fully-valid base64 remainder
	// (otherwise the older soft-passthrough branch would mask the bug anyway).
	idx := strings.Index(sanitized, errTextTag)
	require.GreaterOrEqual(t, idx, 0, "sanitized message must retain the tag")
	token := sanitized[idx+len(errTextTag):]
	_, decErr := base64.StdEncoding.DecodeString(token)
	require.NoError(t, decErr, "token after tag must be valid base64 to exercise the decode path")

	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]
	s := NewGormStorage(db)
	require.NoError(t, s.Migrate(ctx))

	id := failTerminally(t, ctx, s, errMsg)

	got, err := s.GetJob(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, sanitized, got.LastError, "identity-codec error text must round-trip verbatim, not be decoded")
}

func TestErrorTextWrongCodecHardFails(t *testing.T) {
	const errMsg = "wrong key boom"

	ctx := context.Background()
	db := openCodecTestDBs(t)["sqlite"]

	// Write tagged ciphertext with markerXORCodec.
	writer := NewGormStorage(db, WithCodec(markerXORCodec{}))
	require.NoError(t, writer.Migrate(ctx))
	id := failTerminally(t, ctx, writer, errMsg)

	// Confirm the value is genuinely tagged (valid base64 token) so the read
	// path reaches codec.Decode rather than the soft-passthrough branch.
	rawLast := rawJobErrorText(t, db, id, "last_error")
	require.True(t, strings.HasPrefix(rawLast, errTextTag))

	// Read with a codec whose Decode rejects the bytes → hard ErrPayloadDecode.
	reader := NewGormStorage(db, WithCodec(failingDecodeCodec{}))
	_, err := reader.GetJob(ctx, id)
	require.Error(t, err)
	assert.ErrorIs(t, err, core.ErrPayloadDecode)
	assert.Contains(t, err.Error(), id)
}
