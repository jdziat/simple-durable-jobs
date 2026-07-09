package ui

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
)

// TestGetWorkflow_TruncatedOnParentCycle (PKT-11 / A-05) proves the root-climb's
// cycle detection reports truncation instead of looping or silently presenting a
// non-root as the root.
func TestGetWorkflow_TruncatedOnParentCycle(t *testing.T) {
	a, b := core.NewID(), core.NewID()
	mock := &mockStorage{
		getJobFn: func(_ context.Context, id core.UUID) (*core.Job, error) {
			switch id {
			case a:
				return &core.Job{ID: a, Queue: "q", Type: "work", ParentJobID: &b}, nil
			case b:
				return &core.Job{ID: b, Queue: "q", Type: "work", ParentJobID: &a}, nil
			default:
				return nil, nil
			}
		},
		getFanOutsByParentFn: func(context.Context, core.UUID) ([]*core.FanOut, error) { return nil, nil },
	}
	svc := newServiceWithBaseStorage(mock)

	resp, err := svc.GetWorkflow(context.Background(),
		connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: a.String()}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Truncated, "a cyclic parent chain must be reported truncated")
}

// TestGetWorkflow_TruncatedOnDeepParentChain (PKT-11 / A-05) proves the hop cap
// reports truncation on an over-deep parent chain rather than silently treating
// the 100th ancestor as the root.
func TestGetWorkflow_TruncatedOnDeepParentChain(t *testing.T) {
	mock := &mockStorage{
		getJobFn: func(_ context.Context, id core.UUID) (*core.Job, error) {
			parent := core.NewID() // always another ancestor: an unbounded chain
			return &core.Job{ID: id, Queue: "q", Type: "work", ParentJobID: &parent}, nil
		},
		getFanOutsByParentFn: func(context.Context, core.UUID) ([]*core.FanOut, error) { return nil, nil },
	}
	svc := newServiceWithBaseStorage(mock)

	resp, err := svc.GetWorkflow(context.Background(),
		connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: core.NewID().String()}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Truncated, "a parent chain deeper than the hop cap must be reported truncated")
}

// oversizedSubJobs returns maxWorkflowJobs+1 fresh sub-jobs of the given fan-out
// so the descendant BFS trips the per-request node cap. FanOutID must be set so
// the batched collector groups them under fanOutID.
func oversizedSubJobs(fanOutID core.UUID) []*core.Job {
	jobs := make([]*core.Job, maxWorkflowJobs+1)
	for i := range jobs {
		jobs[i] = &core.Job{ID: core.NewID(), Queue: "q", Type: "work", FanOutID: &fanOutID}
	}
	return jobs
}

// TestGetWorkflow_TruncatedOnOversizedTree_Fallback (PKT-11 / A-06) drives the
// NON-batched collectWorkflowTreeFallback path to > maxWorkflowJobs descendants
// and asserts truncated=true — the tree-cap half of the honesty fix.
func TestGetWorkflow_TruncatedOnOversizedTree_Fallback(t *testing.T) {
	root := core.NewID()
	fanOut := core.NewID()
	mock := &mockStorage{
		getJobFn: func(_ context.Context, id core.UUID) (*core.Job, error) {
			return &core.Job{ID: id, Queue: "q", Type: "work"}, nil // root, no parent
		},
		getFanOutsByParentFn: func(_ context.Context, parentID core.UUID) ([]*core.FanOut, error) {
			if parentID == root {
				return []*core.FanOut{{ID: fanOut, ParentJobID: root}}, nil
			}
			return nil, nil
		},
		getSubJobsFn: func(_ context.Context, foID core.UUID) ([]*core.Job, error) {
			if foID == fanOut {
				return oversizedSubJobs(fanOut), nil
			}
			return nil, nil
		},
	}
	svc := newServiceWithBaseStorage(mock)

	resp, err := svc.GetWorkflow(context.Background(),
		connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: root.String()}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Truncated, "a tree exceeding maxWorkflowJobs must be reported truncated (fallback path)")
}

// TestGetWorkflow_TruncatedOnOversizedTree_Batched drives the same via the
// batched collectWorkflowTreeBatched path (storage implements workflowBatchStorage).
func TestGetWorkflow_TruncatedOnOversizedTree_Batched(t *testing.T) {
	root := core.NewID()
	fanOut := core.NewID()
	mock := &mockUIWorkflowBatchStorage{}
	mock.getJobFn = func(_ context.Context, id core.UUID) (*core.Job, error) {
		return &core.Job{ID: id, Queue: "q", Type: "work"}, nil
	}
	mock.getFanOutsByParentsFn = func(_ context.Context, parentIDs []core.UUID) ([]*core.FanOut, error) {
		for _, p := range parentIDs {
			if p == root {
				return []*core.FanOut{{ID: fanOut, ParentJobID: root}}, nil
			}
		}
		return nil, nil
	}
	mock.getSubJobsByFanOutsFn = func(_ context.Context, fanOutIDs []core.UUID) ([]*core.Job, error) {
		for _, f := range fanOutIDs {
			if f == fanOut {
				return oversizedSubJobs(fanOut), nil
			}
		}
		return nil, nil
	}
	svc := newJobsService(mock, nil, nil)

	resp, err := svc.GetWorkflow(context.Background(),
		connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: root.String()}))
	require.NoError(t, err)
	require.True(t, resp.Msg.Truncated, "a tree exceeding maxWorkflowJobs must be reported truncated (batched path)")
}

// TestGetWorkflow_NotTruncatedForNormalTree guards the happy path: a plain root
// (no parent, no oversized tree) reports truncated=false.
func TestGetWorkflow_NotTruncatedForNormalTree(t *testing.T) {
	root := core.NewID()
	mock := &mockStorage{
		getJobFn: func(_ context.Context, id core.UUID) (*core.Job, error) {
			return &core.Job{ID: id, Queue: "q", Type: "work"}, nil // ParentJobID nil = root
		},
		getFanOutsByParentFn: func(context.Context, core.UUID) ([]*core.FanOut, error) { return nil, nil },
	}
	svc := newServiceWithBaseStorage(mock)

	resp, err := svc.GetWorkflow(context.Background(),
		connect.NewRequest(&jobsv1.GetWorkflowRequest{JobId: root.String()}))
	require.NoError(t, err)
	require.False(t, resp.Msg.Truncated, "a normal, fully-collected workflow must not be marked truncated")
}
