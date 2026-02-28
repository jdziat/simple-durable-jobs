package ui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1/jobsv1connect"
)

const maxWatchStreams = 50

// jobsService implements the JobsService Connect-RPC service.
type jobsService struct {
	jobsv1connect.UnimplementedJobsServiceHandler
	storage      core.Storage
	queue        *queue.Queue
	statsStorage StatsStorage

	activeStreams atomic.Int32
}

func newJobsService(storage core.Storage, q *queue.Queue, statsStorage StatsStorage) *jobsService {
	return &jobsService{
		storage:      storage,
		queue:        q,
		statsStorage: statsStorage,
	}
}

// GetStats returns queue statistics.
func (s *jobsService) GetStats(ctx context.Context, req *connect.Request[jobsv1.GetStatsRequest]) (*connect.Response[jobsv1.GetStatsResponse], error) {
	stats, err := s.getQueueStats(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &jobsv1.GetStatsResponse{
		Queues: stats,
	}

	for _, qs := range stats {
		resp.TotalPending += qs.Pending
		resp.TotalRunning += qs.Running
		resp.TotalCompleted += qs.Completed
		resp.TotalFailed += qs.Failed
	}

	return connect.NewResponse(resp), nil
}

// GetStatsHistory returns historical statistics from the stats table.
func (s *jobsService) GetStatsHistory(ctx context.Context, req *connect.Request[jobsv1.GetStatsHistoryRequest]) (*connect.Response[jobsv1.GetStatsHistoryResponse], error) {
	if s.statsStorage == nil {
		return connect.NewResponse(&jobsv1.GetStatsHistoryResponse{}), nil
	}

	since, until := parsePeriod(req.Msg.Period)

	stats, err := s.statsStorage.GetStatsHistory(ctx, req.Msg.Queue, since, until)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Aggregate completed and failed into DataPoint slices
	completedMap := make(map[int64]int64)
	failedMap := make(map[int64]int64)
	for _, st := range stats {
		ts := st.Timestamp.Unix()
		completedMap[ts] += st.Completed
		failedMap[ts] += st.Failed
	}

	resp := &jobsv1.GetStatsHistoryResponse{}
	for ts, val := range completedMap {
		resp.Completed = append(resp.Completed, &jobsv1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(ts, 0)),
			Value:     val,
		})
	}
	for ts, val := range failedMap {
		resp.Failed = append(resp.Failed, &jobsv1.DataPoint{
			Timestamp: timestamppb.New(time.Unix(ts, 0)),
			Value:     val,
		})
	}

	sort.Slice(resp.Completed, func(i, j int) bool {
		return resp.Completed[i].Timestamp.AsTime().Before(resp.Completed[j].Timestamp.AsTime())
	})
	sort.Slice(resp.Failed, func(i, j int) bool {
		return resp.Failed[i].Timestamp.AsTime().Before(resp.Failed[j].Timestamp.AsTime())
	})

	return connect.NewResponse(resp), nil
}

// ListJobs returns a paginated list of jobs with filters.
func (s *jobsService) ListJobs(ctx context.Context, req *connect.Request[jobsv1.ListJobsRequest]) (*connect.Response[jobsv1.ListJobsResponse], error) {
	limit := int(req.Msg.Limit)
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	page := int(req.Msg.Page)
	if page < 1 {
		page = 1
	}

	jobs, total, err := s.searchJobs(ctx, req.Msg, limit, page)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	pbJobs := make([]*jobsv1.Job, len(jobs))
	for i, j := range jobs {
		pbJobs[i] = jobToProto(j)
	}

	return connect.NewResponse(&jobsv1.ListJobsResponse{
		Jobs:  pbJobs,
		Total: total,
		Page:  int32(page),
	}), nil
}

// GetJob returns a single job with its checkpoints.
func (s *jobsService) GetJob(ctx context.Context, req *connect.Request[jobsv1.GetJobRequest]) (*connect.Response[jobsv1.GetJobResponse], error) {
	job, err := s.storage.GetJob(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	checkpoints, err := s.storage.GetCheckpoints(ctx, job.ID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	pbCheckpoints := make([]*jobsv1.Checkpoint, len(checkpoints))
	for i, cp := range checkpoints {
		pbCheckpoints[i] = checkpointToProto(&cp)
	}

	return connect.NewResponse(&jobsv1.GetJobResponse{
		Job:         jobToProto(job),
		Checkpoints: pbCheckpoints,
	}), nil
}

// RetryJob resets a failed job to pending.
func (s *jobsService) RetryJob(ctx context.Context, req *connect.Request[jobsv1.RetryJobRequest]) (*connect.Response[jobsv1.RetryJobResponse], error) {
	job, err := s.retryJob(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	return connect.NewResponse(&jobsv1.RetryJobResponse{
		Job: jobToProto(job),
	}), nil
}

// DeleteJob removes a job.
func (s *jobsService) DeleteJob(ctx context.Context, req *connect.Request[jobsv1.DeleteJobRequest]) (*connect.Response[jobsv1.DeleteJobResponse], error) {
	if err := s.deleteJob(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobsv1.DeleteJobResponse{}), nil
}

// BulkRetryJobs retries multiple jobs.
func (s *jobsService) BulkRetryJobs(ctx context.Context, req *connect.Request[jobsv1.BulkRetryJobsRequest]) (*connect.Response[jobsv1.BulkRetryJobsResponse], error) {
	count := 0
	for _, id := range req.Msg.Ids {
		if _, err := s.retryJob(ctx, id); err == nil {
			count++
		}
	}
	return connect.NewResponse(&jobsv1.BulkRetryJobsResponse{
		Count: int32(count),
	}), nil
}

// BulkDeleteJobs deletes multiple jobs.
func (s *jobsService) BulkDeleteJobs(ctx context.Context, req *connect.Request[jobsv1.BulkDeleteJobsRequest]) (*connect.Response[jobsv1.BulkDeleteJobsResponse], error) {
	count := 0
	for _, id := range req.Msg.Ids {
		if err := s.deleteJob(ctx, id); err == nil {
			count++
		}
	}
	return connect.NewResponse(&jobsv1.BulkDeleteJobsResponse{
		Count: int32(count),
	}), nil
}

// ListQueues returns all queues with stats.
func (s *jobsService) ListQueues(ctx context.Context, req *connect.Request[jobsv1.ListQueuesRequest]) (*connect.Response[jobsv1.ListQueuesResponse], error) {
	stats, err := s.getQueueStats(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobsv1.ListQueuesResponse{
		Queues: stats,
	}), nil
}

// PurgeQueue deletes jobs from a queue by status.
func (s *jobsService) PurgeQueue(ctx context.Context, req *connect.Request[jobsv1.PurgeQueueRequest]) (*connect.Response[jobsv1.PurgeQueueResponse], error) {
	deleted, err := s.purgeQueue(ctx, req.Msg.Name, req.Msg.Status)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobsv1.PurgeQueueResponse{
		Deleted: deleted,
	}), nil
}

// ListScheduledJobs returns registered scheduled jobs.
func (s *jobsService) ListScheduledJobs(ctx context.Context, req *connect.Request[jobsv1.ListScheduledJobsRequest]) (*connect.Response[jobsv1.ListScheduledJobsResponse], error) {
	var jobsList []*jobsv1.ScheduledJobInfo

	if s.queue != nil {
		scheduled := s.queue.GetScheduledJobs()
		for name, sj := range scheduled {
			// Format schedule - use type assertion or just store the name
			schedStr := name // Fallback to name if no string method
			if stringer, ok := interface{}(sj.Schedule).(interface{ String() string }); ok {
				schedStr = stringer.String()
			}
			jobsList = append(jobsList, &jobsv1.ScheduledJobInfo{
				Name:     name,
				Schedule: schedStr,
				Queue:    sj.Options.Queue,
			})
		}
	}

	return connect.NewResponse(&jobsv1.ListScheduledJobsResponse{
		Jobs: jobsList,
	}), nil
}

// GetWorkflow returns the full workflow tree for a given job ID.
// It walks up the parent chain to find the root, then collects all
// fan-outs and children via BFS, returning everything flattened.
func (s *jobsService) GetWorkflow(ctx context.Context, req *connect.Request[jobsv1.GetWorkflowRequest]) (*connect.Response[jobsv1.GetWorkflowResponse], error) {
	// Load the requested job.
	job, err := s.storage.GetJob(ctx, req.Msg.JobId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	// Walk up the parent chain to find the root (cap at 100 hops to avoid
	// infinite loops from corrupted data).
	root := job
	for i := 0; i < 100 && root.ParentJobID != nil; i++ {
		parent, err := s.storage.GetJob(ctx, *root.ParentJobID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if parent == nil {
			// Parent no longer exists; treat current node as root.
			break
		}
		root = parent
	}

	// BFS to collect all descendants (fan-outs and child jobs).
	// We cap at 1000 jobs to bound memory usage.
	const maxJobs = 1000
	var allFanOuts []*jobsv1.FanOut
	var allChildren []*jobsv1.Job

	queue := []string{root.ID}
	visited := make(map[string]struct{}, maxJobs)
	visited[root.ID] = struct{}{}

	for len(queue) > 0 && len(allChildren) < maxJobs {
		current := queue[0]
		queue = queue[1:]

		fanOuts, err := s.storage.GetFanOutsByParent(ctx, current)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, fo := range fanOuts {
			allFanOuts = append(allFanOuts, fanOutToProto(fo))

			subJobs, err := s.storage.GetSubJobs(ctx, fo.ID)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			for _, sj := range subJobs {
				if _, seen := visited[sj.ID]; seen {
					continue
				}
				visited[sj.ID] = struct{}{}
				allChildren = append(allChildren, jobToProto(sj))
				// Enqueue so we can find deeper fan-outs rooted at this child.
				queue = append(queue, sj.ID)
				if len(allChildren) >= maxJobs {
					break
				}
			}
		}
	}

	return connect.NewResponse(&jobsv1.GetWorkflowResponse{
		Root:     jobToProto(root),
		FanOuts:  allFanOuts,
		Children: allChildren,
	}), nil
}

// ListWorkflows returns a paginated list of workflow root jobs.
// It requires the storage to implement UIStorage with GetWorkflowRoots;
// without that, it returns an unimplemented error.
func (s *jobsService) ListWorkflows(ctx context.Context, req *connect.Request[jobsv1.ListWorkflowsRequest]) (*connect.Response[jobsv1.ListWorkflowsResponse], error) {
	limit := int(req.Msg.Limit)
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	page := int(req.Msg.Page)
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	ui, ok := s.storage.(UIStorage)
	if !ok {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}

	roots, total, err := ui.GetWorkflowRoots(ctx, req.Msg.Status, limit, offset)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// For each root, build a WorkflowSummary by collecting its fan-outs.
	summaries := make([]*jobsv1.WorkflowSummary, 0, len(roots))
	for _, root := range roots {
		summary := &jobsv1.WorkflowSummary{
			RootJob: jobToProto(root),
		}

		fanOuts, err := s.storage.GetFanOutsByParent(ctx, root.ID)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		if len(fanOuts) > 0 {
			// Use strategy from the first fan-out as representative.
			summary.Strategy = string(fanOuts[0].Strategy)

			var totalJobs, completedJobs, failedJobs int32
			for _, fo := range fanOuts {
				totalJobs += int32(fo.TotalCount)
				completedJobs += int32(fo.CompletedCount)
				failedJobs += int32(fo.FailedCount)
			}
			summary.TotalJobs = totalJobs
			summary.CompletedJobs = completedJobs
			summary.FailedJobs = failedJobs
			summary.RunningJobs = totalJobs - completedJobs - failedJobs
			if summary.RunningJobs < 0 {
				summary.RunningJobs = 0
			}
		}

		summaries = append(summaries, summary)
	}

	return connect.NewResponse(&jobsv1.ListWorkflowsResponse{
		Workflows: summaries,
		Total:     total,
		Page:      int32(page),
	}), nil
}

// WatchEvents streams real-time job events.
func (s *jobsService) WatchEvents(ctx context.Context, req *connect.Request[jobsv1.WatchEventsRequest], stream *connect.ServerStream[jobsv1.Event]) error {
	if s.queue == nil {
		<-ctx.Done()
		return ctx.Err()
	}

	// NOTE: This uses atomic Add rather than CAS, so the counter can transiently
	// exceed maxWatchStreams under burst conditions before the rejection decrements
	// it back. This is an acceptable tradeoff — the window is tiny and the worst
	// case is a few extra rejections, not over-admission.
	if s.activeStreams.Add(1) > maxWatchStreams {
		s.activeStreams.Add(-1)
		return connect.NewError(connect.CodeResourceExhausted, fmt.Errorf("too many active watch streams"))
	}
	defer s.activeStreams.Add(-1)

	events := s.queue.Events()
	defer s.queue.Unsubscribe(events)

	filterSet := make(map[string]struct{}, len(req.Msg.Queues))
	for _, q := range req.Msg.Queues {
		filterSet[q] = struct{}{}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-events:
			pbEvent := coreEventToProto(e)
			if pbEvent == nil {
				continue
			}

			if len(filterSet) > 0 {
				eventQueue := ""
				if pbEvent.Job != nil {
					eventQueue = pbEvent.Job.Queue
				}
				if _, ok := filterSet[eventQueue]; !ok {
					continue
				}
			}

			if err := stream.Send(pbEvent); err != nil {
				return err
			}
		}
	}
}

// Helper methods

func (s *jobsService) getQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error) {
	// Check if storage implements UIStorage interface
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.GetQueueStats(ctx)
	}

	// Fallback: query all jobs and aggregate (capped to prevent OOM)
	const maxFallbackJobs = 1000
	stats := make(map[string]*jobsv1.QueueStats)

	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted, core.StatusFailed} {
		jobs, err := s.storage.GetJobsByStatus(ctx, status, maxFallbackJobs)
		if err != nil {
			return nil, err
		}
		for _, job := range jobs {
			qs, ok := stats[job.Queue]
			if !ok {
				qs = &jobsv1.QueueStats{Name: job.Queue}
				stats[job.Queue] = qs
			}
			switch status {
			case core.StatusPending:
				qs.Pending++
			case core.StatusRunning:
				qs.Running++
			case core.StatusCompleted:
				qs.Completed++
			case core.StatusFailed:
				qs.Failed++
			}
		}
	}

	queueStats := make([]*jobsv1.QueueStats, 0, len(stats))
	for _, qs := range stats {
		queueStats = append(queueStats, qs)
	}
	return queueStats, nil
}

func (s *jobsService) searchJobs(ctx context.Context, req *jobsv1.ListJobsRequest, limit, page int) ([]*core.Job, int64, error) {
	// Check if storage implements UIStorage
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.SearchJobs(ctx, JobFilter{
			Status: req.Status,
			Queue:  req.Queue,
			Type:   req.Type,
			Search: req.Search,
			Limit:  limit,
			Offset: (page - 1) * limit,
		})
	}

	// Fallback: basic implementation - fetch jobs from all statuses (capped)
	const maxFetchPerStatus = 1000
	var allJobs []*core.Job

	fetchLimit := limit * page
	if fetchLimit > maxFetchPerStatus {
		fetchLimit = maxFetchPerStatus
	}

	if req.Status != "" {
		// Filter by specific status
		jobs, err := s.storage.GetJobsByStatus(ctx, core.JobStatus(req.Status), fetchLimit)
		if err != nil {
			return nil, 0, err
		}
		allJobs = jobs
	} else {
		// No status filter - fetch from all statuses
		for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted, core.StatusFailed} {
			jobs, err := s.storage.GetJobsByStatus(ctx, status, fetchLimit)
			if err != nil {
				return nil, 0, err
			}
			allJobs = append(allJobs, jobs...)
		}
	}

	// Apply additional filters
	var filtered []*core.Job
	for _, job := range allJobs {
		if req.Queue != "" && job.Queue != req.Queue {
			continue
		}
		if req.Type != "" && job.Type != req.Type {
			continue
		}
		if req.Search != "" && !strings.Contains(job.ID, req.Search) && !strings.Contains(string(job.Args), req.Search) {
			continue
		}
		filtered = append(filtered, job)
	}

	// Sort by created_at DESC (newest first)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})

	// Paginate
	start := (page - 1) * limit
	if start >= len(filtered) {
		return nil, int64(len(filtered)), nil
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}

	return filtered[start:end], int64(len(filtered)), nil
}

func (s *jobsService) retryJob(ctx context.Context, id string) (*core.Job, error) {
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.RetryJob(ctx, id)
	}

	// Fallback: not supported without UIStorage
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}

func (s *jobsService) deleteJob(ctx context.Context, id string) error {
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.DeleteJob(ctx, id)
	}
	return connect.NewError(connect.CodeUnimplemented, nil)
}

func (s *jobsService) purgeQueue(ctx context.Context, queueName, status string) (int64, error) {
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.PurgeJobs(ctx, queueName, core.JobStatus(status))
	}
	return 0, connect.NewError(connect.CodeUnimplemented, nil)
}

// Conversion helpers

func jobToProto(j *core.Job) *jobsv1.Job {
	pb := &jobsv1.Job{
		Id:          j.ID,
		Type:        j.Type,
		Queue:       j.Queue,
		Status:      string(j.Status),
		Priority:    int32(j.Priority),
		Attempt:     int32(j.Attempt),
		MaxRetries:  int32(j.MaxRetries),
		Args:        j.Args,
		LastError:   j.LastError,
		CreatedAt:   timestamppb.New(j.CreatedAt),
		FanOutIndex: int32(j.FanOutIndex),
		Result:      j.Result,
	}
	if j.StartedAt != nil {
		pb.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.CompletedAt != nil {
		pb.CompletedAt = timestamppb.New(*j.CompletedAt)
	}
	if j.RunAt != nil {
		pb.RunAt = timestamppb.New(*j.RunAt)
	}
	if j.ParentJobID != nil {
		pb.ParentJobId = j.ParentJobID
	}
	if j.RootJobID != nil {
		pb.RootJobId = j.RootJobID
	}
	if j.FanOutID != nil {
		pb.FanOutId = j.FanOutID
	}
	return pb
}

func fanOutToProto(f *core.FanOut) *jobsv1.FanOut {
	pb := &jobsv1.FanOut{
		Id:             f.ID,
		ParentJobId:    f.ParentJobID,
		TotalCount:     int32(f.TotalCount),
		CompletedCount: int32(f.CompletedCount),
		FailedCount:    int32(f.FailedCount),
		CancelledCount: int32(f.CancelledCount),
		Strategy:       string(f.Strategy),
		Threshold:      f.Threshold,
		Status:         string(f.Status),
		CancelOnFail:   f.CancelOnFail,
		CreatedAt:      timestamppb.New(f.CreatedAt),
		UpdatedAt:      timestamppb.New(f.UpdatedAt),
	}
	if f.TimeoutAt != nil {
		pb.TimeoutAt = timestamppb.New(*f.TimeoutAt)
	}
	return pb
}

func checkpointToProto(cp *core.Checkpoint) *jobsv1.Checkpoint {
	pb := &jobsv1.Checkpoint{
		Id:        cp.ID,
		JobId:     cp.JobID,
		CallIndex: int32(cp.CallIndex),
		CallType:  cp.CallType,
		Result:    cp.Result,
		Error:     cp.Error,
		CreatedAt: timestamppb.New(cp.CreatedAt),
	}
	return pb
}

// coreEventToProto converts a core.Event to a proto Event.
func coreEventToProto(e core.Event) *jobsv1.Event {
	switch ev := e.(type) {
	case *core.JobStarted:
		return &jobsv1.Event{
			Type:      "job.started",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobCompleted:
		return &jobsv1.Event{
			Type:      "job.completed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobFailed:
		return &jobsv1.Event{
			Type:      "job.failed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobRetrying:
		return &jobsv1.Event{
			Type:      "job.retrying",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobPaused:
		return &jobsv1.Event{
			Type:      "job.paused",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.JobResumed:
		return &jobsv1.Event{
			Type:      "job.resumed",
			Job:       jobToProto(ev.Job),
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.QueuePaused:
		return &jobsv1.Event{
			Type:      "queue.paused",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.QueueResumed:
		return &jobsv1.Event{
			Type:      "queue.resumed",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.WorkerPaused:
		return &jobsv1.Event{
			Type:      "worker.paused",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.WorkerResumed:
		return &jobsv1.Event{
			Type:      "worker.resumed",
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	default:
		return nil
	}
}

func parsePeriod(period string) (since, until time.Time) {
	until = time.Now()
	switch period {
	case "1h":
		since = until.Add(-1 * time.Hour)
	case "24h":
		since = until.Add(-24 * time.Hour)
	case "7d":
		since = until.Add(-7 * 24 * time.Hour)
	default:
		since = until.Add(-1 * time.Hour)
	}
	return
}

// UIStorage is an optional interface for enhanced UI queries.
type UIStorage interface {
	core.Storage
	GetQueueStats(ctx context.Context) ([]*jobsv1.QueueStats, error)
	SearchJobs(ctx context.Context, filter core.JobFilter) ([]*core.Job, int64, error)
	RetryJob(ctx context.Context, jobID string) (*core.Job, error)
	DeleteJob(ctx context.Context, jobID string) error
	PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error)
	// GetWorkflowRoots returns root jobs (ParentJobID == nil) that are workflow
	// parents. status filters by job status when non-empty. limit and offset
	// control pagination. Returns the matching jobs and the total count.
	GetWorkflowRoots(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error)
}

// JobFilter is an alias for core.JobFilter for backward compatibility.
type JobFilter = core.JobFilter
