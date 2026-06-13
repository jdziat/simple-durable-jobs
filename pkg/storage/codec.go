package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

// errTextTag marks an encrypted-then-base64 segment inside a plaintext-bearing
// TEXT column (last_error / dead_letter_reason). The trailing colon is NOT in
// the base64 alphabet, so the tag can never appear inside a base64 token. The
// tag may sit AFTER a fixed label (e.g. "max retries exhausted: sdjenc:..."),
// so decode scans for it with strings.Index rather than HasPrefix.
const errTextTag = "sdjenc:"

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
func (s *GormStorage) decodeErrorText(kind, id, stored string) (string, error) {
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

func (s *GormStorage) encodedJobForCreate(job *core.Job) (*core.Job, error) {
	encodedArgs, err := s.encodePayload("job args", job.ID, job.Args)
	if err != nil {
		return nil, err
	}
	if len(job.Args) == 0 || bytes.Equal(encodedArgs, job.Args) {
		return job, nil
	}
	cp := *job
	cp.Args = encodedArgs
	return &cp, nil
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
	args, err := s.decodePayload("job args", job.ID, job.Args)
	if err != nil {
		return err
	}
	result, err := s.decodePayload("job result", job.ID, job.Result)
	if err != nil {
		return err
	}
	le, err := s.decodeErrorText("job last_error", job.ID, job.LastError)
	if err != nil {
		return err
	}
	dr, err := s.decodeErrorText("job dead_letter_reason", job.ID, job.DeadLetterReason)
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
	encoded, err := s.encodePayload("checkpoint result", cp.ID, cp.Result)
	if err != nil {
		return nil, err
	}
	if len(cp.Result) == 0 || bytes.Equal(encoded, cp.Result) {
		return cp, nil
	}
	out := *cp
	out.Result = encoded
	return &out, nil
}

func (s *GormStorage) decodeCheckpointPayloads(cps []core.Checkpoint) error {
	for i := range cps {
		result, err := s.decodePayload("checkpoint result", cps[i].ID, cps[i].Result)
		if err != nil {
			return err
		}
		cps[i].Result = result
	}
	return nil
}

func (s *GormStorage) decodeSignalPayload(sig *core.Signal) error {
	if sig == nil {
		return nil
	}
	payload, err := s.decodePayload("signal payload", sig.ID, sig.Payload)
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
