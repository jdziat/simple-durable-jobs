package storage

import (
	"bytes"
	"fmt"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

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
	job.Args = args
	job.Result = result
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
