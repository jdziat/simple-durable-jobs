package typed_test

import (
	"context"
	"time"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

type exampleStorage struct {
	jobs map[string]*core.Job
}

func newExampleStorage() *exampleStorage {
	return &exampleStorage{jobs: make(map[string]*core.Job)}
}

func (s *exampleStorage) Migrate(context.Context) error { return nil }

func (s *exampleStorage) Enqueue(_ context.Context, job *core.Job) error {
	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *exampleStorage) Dequeue(context.Context, []string, string) (*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) Complete(context.Context, string, string) error { return nil }

func (s *exampleStorage) Fail(context.Context, string, string, string, *time.Time) error {
	return nil
}

func (s *exampleStorage) EnqueueUnique(_ context.Context, job *core.Job, _ string) error {
	return s.Enqueue(context.Background(), job)
}

func (s *exampleStorage) SaveCheckpoint(context.Context, *core.Checkpoint) error { return nil }

func (s *exampleStorage) GetCheckpoints(context.Context, string) ([]core.Checkpoint, error) {
	return nil, nil
}

func (s *exampleStorage) DeleteCheckpoints(context.Context, string) error { return nil }

func (s *exampleStorage) GetDueJobs(context.Context, []string, int) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) ClaimScheduledFire(context.Context, string, time.Time) (bool, error) {
	return false, nil
}

func (s *exampleStorage) Heartbeat(context.Context, string, string) error { return nil }

func (s *exampleStorage) Release(context.Context, string, string) error { return nil }

func (s *exampleStorage) ReleaseStaleLocks(context.Context, time.Duration) ([]string, error) {
	return nil, nil
}

func (s *exampleStorage) FindOrphanedJobs(context.Context, []string, string) ([]string, error) {
	return nil, nil
}

func (s *exampleStorage) GetJob(_ context.Context, jobID string) (*core.Job, error) {
	job := s.jobs[jobID]
	if job == nil {
		return nil, nil
	}
	cp := *job
	return &cp, nil
}

func (s *exampleStorage) GetJobsByStatus(context.Context, core.JobStatus, int) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) CreateFanOut(context.Context, *core.FanOut) error { return nil }

func (s *exampleStorage) GetFanOut(context.Context, string) (*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) IncrementFanOutCompleted(context.Context, string) (*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) IncrementFanOutFailed(context.Context, string) (*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) IncrementFanOutCancelled(context.Context, string) (*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) UpdateFanOutStatus(context.Context, string, core.FanOutStatus) (bool, error) {
	return false, nil
}

func (s *exampleStorage) GetFanOutsByParent(context.Context, string) ([]*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) EnqueueBatch(context.Context, []*core.Job) error { return nil }

func (s *exampleStorage) GetSubJobs(context.Context, string) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) GetSubJobResults(context.Context, string) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) CancelSubJobs(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *exampleStorage) CancelSubJob(context.Context, string) (*core.FanOut, error) {
	return nil, nil
}

func (s *exampleStorage) MarkWaiting(context.Context, string, string) error { return nil }

func (s *exampleStorage) ResumeJob(context.Context, string) (bool, error) { return false, nil }

func (s *exampleStorage) GetWaitingJobsToResume(context.Context) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) GetStalledFanOutParents(context.Context, time.Time) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) SaveJobResult(context.Context, string, string, []byte) error {
	return nil
}

func (s *exampleStorage) PauseJob(context.Context, string) error { return nil }

func (s *exampleStorage) UnpauseJob(context.Context, string) error { return nil }

func (s *exampleStorage) GetPausedJobs(context.Context, string) ([]*core.Job, error) {
	return nil, nil
}

func (s *exampleStorage) IsJobPaused(context.Context, string) (bool, error) { return false, nil }

func (s *exampleStorage) PauseQueue(context.Context, string) error { return nil }

func (s *exampleStorage) UnpauseQueue(context.Context, string) error { return nil }

func (s *exampleStorage) GetPausedQueues(context.Context) ([]string, error) { return nil, nil }

func (s *exampleStorage) IsQueuePaused(context.Context, string) (bool, error) {
	return false, nil
}

func (s *exampleStorage) RefreshQueueStates(context.Context) (map[string]bool, error) {
	return nil, nil
}
