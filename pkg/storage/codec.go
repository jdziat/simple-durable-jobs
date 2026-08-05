package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// errTextTag marks an encrypted-then-base64 segment inside a plaintext-bearing
// TEXT column (last_error / dead_letter_reason). The trailing colon is NOT in
// the base64 alphabet, so the tag can never appear inside a base64 token. The
// tag may sit AFTER a fixed label (e.g. "max retries exhausted: sdjenc:..."),
// so decode scans for it with strings.Index rather than HasPrefix.
const errTextTag = "sdjenc:"

// codecIsIdentity reports whether the configured codec is the pass-through
// identity codec (or the unset default). Under it, payload bytes and error text
// are stored verbatim and no errTextTag is ever emitted, so the text-decode path
// must treat stored values as literal plaintext. A caller-supplied codec that
// happens to be a different no-op type is not detected here — the same narrow
// assumption argsSearchable already relies on.
func (s *GormStorage) codecIsIdentity() bool {
	return s.codec == nil || s.codec == core.IdentityCodec
}

func (s *GormStorage) encodePayload(kind, id string, b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	encoded, err := s.codec.Encode(b)
	if err != nil {
		return nil, fmt.Errorf("storage: encode %s payload %s: %w", kind, id, err)
	}
	return encoded, nil
}

func (s *GormStorage) decodePayload(kind, id string, b []byte) ([]byte, error) {
	if len(b) == 0 {
		return b, nil
	}
	decoded, err := s.codec.Decode(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s: %w", core.ErrPayloadDecode, kind, id, err)
	}
	return decoded, nil
}

// encodeErrorText encrypts handler error text (last_error / dead_letter_reason
// suffix) for storage in a TEXT column. Secretbox output contains NUL bytes and
// non-UTF8 sequences that Postgres TEXT rejects and MySQL TEXT may mangle, so
// the binary ciphertext is base64-encoded behind the errTextTag. Under the
// default identity codec the bytes are unchanged (bytes.Equal short-circuit) and
// the plaintext is stored verbatim — zero behavior change. Empty input stays
// empty so the Requeue/RetryJob clear-to-"" sites are untouched.
func (s *GormStorage) encodeErrorText(kind, id, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	encoded, err := s.codec.Encode([]byte(plaintext))
	if err != nil {
		return "", fmt.Errorf("storage: encode %s payload %s: %w", kind, id, err)
	}
	if bytes.Equal(encoded, []byte(plaintext)) {
		// Identity / pass-through codec: store readable plaintext, no tag.
		return plaintext, nil
	}
	return errTextTag + base64.StdEncoding.EncodeToString(encoded), nil
}

// decodeErrorText reverses encodeErrorText. Untagged values (legacy plaintext or
// identity codec) are returned verbatim. A tagged value whose token is not valid
// base64 is a coincidental plaintext that merely contains the tag substring and
// is returned untouched (soft pass-through). A tagged value with a valid-base64
// token that the codec cannot open means wrong key / corruption and surfaces as
// core.ErrPayloadDecode, mirroring decodePayload. The tag may follow a fixed
// label, so any plaintext prefix before the tag is preserved.
//
// Under the identity codec encodeErrorText never emits the tag, so any stored
// value containing the tag substring is literal plaintext. Decoding it would
// corrupt a legitimate message that happens to read "...sdjenc:<valid-base64>"
// (the base64 token would be decoded and returned as bytes), so the tag scan is
// skipped entirely for a pass-through codec.
func (s *GormStorage) decodeErrorText(kind, id, stored string) (string, error) {
	if s.codecIsIdentity() {
		return stored, nil
	}
	idx := strings.Index(stored, errTextTag)
	if idx < 0 {
		return stored, nil
	}
	prefix := stored[:idx]
	token := stored[idx+len(errTextTag):]
	raw, berr := base64.StdEncoding.DecodeString(token)
	if berr != nil {
		// Coincidental plaintext containing the tag but not our encoding.
		return stored, nil
	}
	decoded, derr := s.codec.Decode(raw)
	if derr != nil {
		return "", fmt.Errorf("%w: %s %s: %w", core.ErrPayloadDecode, kind, id, derr)
	}
	return prefix + string(decoded), nil
}

// encodedJobForCreate builds the row handed to GORM's Create. It ALWAYS returns
// a fresh struct, never the caller's pointer, and that is load-bearing rather
// than tidiness.
//
// GORM treats the struct it is given as its own scratch space: while building the
// INSERT it substitutes each zero-valued field's declared column default and
// writes the substituted value BACK into that struct — and it leaves it there
// when the statement then FAILS and the transaction rolls back. core.Job declares
// two such defaults that carry caller INTENT in their zero value:
//
//	MaxRetries `default:3`   an explicit jobs.Retries(0) becomes 3
//	DQReady    `default:true` a delayed job's false becomes true
//
// Both are corrected by a follow-up write inside the same transaction
// (applyExplicitZeroRetries, restoreDQReadyFalse), and both of those derive the
// correction from the caller's struct. Every enqueue path is retried on transient
// serialization failures — SQLITE_BUSY, MySQL 1213, Postgres 40001, all of which
// this package documents as EXPECTED — either internally by withSerializationRetry
// or, for the Tx forms, by the caller's documented WithSerializationRetry wrapper,
// which re-enters the function. So if GORM can reach the caller's struct, attempt
// N reads the state attempt N-1 destroyed: the correction never arms and the row
// commits max_retries=3, silently, with a nil error and a do-not-retry handler
// that runs three times.
//
// This used to short-circuit to `return job, nil` whenever the codec left the args
// unchanged — which is the IDENTITY codec, i.e. every unencrypted deployment, i.e.
// the default. Copying unconditionally is what makes the correction read a value
// GORM has never touched, on every attempt, on every path, without each of those
// paths having to remember. Pinned by
// TestExplicitZeroRetriesSurvivesASerializationRetryOnEveryEnqueuePath and
// TestRowForCreateNeverHandsGORMTheCallersJob.
//
// The cost of no longer aliasing is that GORM's post-insert write-back —
// autoCreateTime/autoUpdateTime — lands on the copy, so the caller's job keeps its
// zero CreatedAt/UpdatedAt. That was ALREADY the behaviour under any non-identity
// codec, so this makes it uniform rather than introducing it; ID, Status and Queue
// are filled by fillEnqueueDefaults on the caller's struct before we are called,
// and those are what a caller reads back.
//
// Args is assigned unconditionally rather than only when the codec changed it.
// encodePayload returns its input verbatim for an empty payload and IdentityCodec
// returns it verbatim otherwise, so the two forms are byte-identical on every
// input — a guard here would be a clause no test could distinguish.
func (s *GormStorage) encodedJobForCreate(job *core.Job) (*core.Job, error) {
	encodedArgs, err := s.encodePayload("job args", string(job.ID), job.Args)
	if err != nil {
		return nil, err
	}
	row := *job
	row.Args = encodedArgs
	return &row, nil
}

func (s *GormStorage) encodedJobsForCreate(jobs []*core.Job) ([]*core.Job, error) {
	out := make([]*core.Job, 0, len(jobs))
	for _, job := range jobs {
		encoded, err := s.encodedJobForCreate(job)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
	}
	return out, nil
}

func (s *GormStorage) decodeJobPayloads(job *core.Job) error {
	if job == nil {
		return nil
	}
	args, err := s.decodePayload("job args", string(job.ID), job.Args)
	if err != nil {
		return err
	}
	result, err := s.decodePayload("job result", string(job.ID), job.Result)
	if err != nil {
		return err
	}
	le, err := s.decodeErrorText("job last_error", string(job.ID), job.LastError)
	if err != nil {
		return err
	}
	dr, err := s.decodeErrorText("job dead_letter_reason", string(job.ID), job.DeadLetterReason)
	if err != nil {
		return err
	}
	job.Args = args
	job.Result = result
	job.LastError = le
	job.DeadLetterReason = dr
	return nil
}

func (s *GormStorage) decodeJobListPayloads(jobs []*core.Job) error {
	for _, job := range jobs {
		if err := s.decodeJobPayloads(job); err != nil {
			return err
		}
	}
	return nil
}

func (s *GormStorage) encodedCheckpointForSave(cp *core.Checkpoint) (*core.Checkpoint, error) {
	// Result is binary (codec.Encode output) and rides the bytes column.
	encodedResult, err := s.encodePayload("checkpoint result", string(cp.ID), cp.Result)
	if err != nil {
		return nil, err
	}
	resultChanged := len(cp.Result) != 0 && !bytes.Equal(encodedResult, cp.Result)

	// Error / ErrorCause carry handler error text in TEXT columns, so they use the
	// base64-tagged encodeErrorText path (same as job last_error/dead_letter_reason),
	// not encodePayload. ErrorKind stays cleartext: it is a non-sensitive
	// discriminator (no_retry/retry_after/sentinel-key) that rehydration relies on.
	encodedErr, err := s.encodeErrorText("checkpoint error", string(cp.ID), cp.Error)
	if err != nil {
		return nil, err
	}
	encodedErrCause, err := s.encodeErrorText("checkpoint error_cause", string(cp.ID), cp.ErrorCause)
	if err != nil {
		return nil, err
	}
	errChanged := encodedErr != cp.Error
	errCauseChanged := encodedErrCause != cp.ErrorCause

	// Preserve the no-op-when-unchanged optimization: only allocate the out copy
	// if at least one field actually changed under the codec.
	if !resultChanged && !errChanged && !errCauseChanged {
		return cp, nil
	}
	out := *cp
	if resultChanged {
		out.Result = encodedResult
	}
	out.Error = encodedErr
	out.ErrorCause = encodedErrCause
	return &out, nil
}

func (s *GormStorage) decodeCheckpointPayloads(cps []core.Checkpoint) error {
	for i := range cps {
		result, err := s.decodePayload("checkpoint result", string(cps[i].ID), cps[i].Result)
		if err != nil {
			return err
		}
		errText, err := s.decodeErrorText("checkpoint error", string(cps[i].ID), cps[i].Error)
		if err != nil {
			return err
		}
		errCause, err := s.decodeErrorText("checkpoint error_cause", string(cps[i].ID), cps[i].ErrorCause)
		if err != nil {
			return err
		}
		cps[i].Result = result
		cps[i].Error = errText
		cps[i].ErrorCause = errCause
	}
	return nil
}

func (s *GormStorage) decodeSignalPayload(sig *core.Signal) error {
	if sig == nil {
		return nil
	}
	payload, err := s.decodePayload("signal payload", string(sig.ID), sig.Payload)
	if err != nil {
		return err
	}
	sig.Payload = payload
	return nil
}

func (s *GormStorage) decodeSignalPayloads(sigs []*core.Signal) error {
	for _, sig := range sigs {
		if err := s.decodeSignalPayload(sig); err != nil {
			return err
		}
	}
	return nil
}
