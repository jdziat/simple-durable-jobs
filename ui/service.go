package ui

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v3/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/v3/ui/gen/jobs/v1/jobsv1connect"
)

const (
	maxWatchStreams      = 50
	maxBulkJobIDs        = 1000
	maxWorkflowJobs      = 1000
	maxInt32Value        = int64(1<<31 - 1)
	statusDeadLetteredUI = "dead-lettered"
)

// jobsService implements the JobsService Connect-RPC service.
type jobsService struct {
	jobsv1connect.UnimplementedJobsServiceHandler
	storage      core.Storage
	queue        *queue.Queue
	statsStorage StatsStorage

	activeStreams     atomic.Int32
	metadataRedaction bool
}

func newJobsService(storage core.Storage, q *queue.Queue, statsStorage StatsStorage) *jobsService {
	return &jobsService{
		storage:           storage,
		queue:             q,
		statsStorage:      statsStorage,
		metadataRedaction: true,
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
		resp.TotalPaused += qs.Paused
	}

	if active, ok := s.storage.(activeWorkersStorage); ok {
		count, err := active.CountActiveWorkers(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if count > maxInt32Value {
			resp.ActiveWorkers = int32(maxInt32Value)
		} else {
			resp.ActiveWorkers = int32(count)
		}
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

	// Bucket the raw per-minute rows into a fixed, evenly-spaced grid that spans
	// the WHOLE requested window. This is what makes the 1h / 24h / 7d selector
	// actually change the chart: each period uses a period-appropriate bucket
	// size, so the returned series both covers the full window (so the time axis
	// reflects 1h vs 7d) and stays bounded (so 7d is not 10k raw points). Empty
	// buckets are emitted as zeros so the index-based chart maps index→time.
	resp := bucketStatsHistory(stats, since, until, periodBucket(req.Msg.Period))

	return connect.NewResponse(resp), nil
}

// periodBucket returns the time-bucket width for a throughput period, chosen so
// each window resolves to a similar, readable number of points (~48-90).
func periodBucket(period string) time.Duration {
	switch period {
	case "24h":
		return 30 * time.Minute // ~48 buckets
	case "7d":
		return 3 * time.Hour // ~56 buckets
	case "30d":
		return 12 * time.Hour // ~60 buckets
	default:
		return time.Minute // 1h -> 60 buckets
	}
}

// bucketStatsHistory aggregates raw stat rows into a contiguous grid of buckets
// of width `bucket` spanning [since, until], emitting one Completed and one
// Failed DataPoint per bucket (zeros included) in ascending time order.
func bucketStatsHistory(stats []JobStat, since, until time.Time, bucket time.Duration) *jobsv1.GetStatsHistoryResponse {
	resp := &jobsv1.GetStatsHistoryResponse{}
	if bucket <= 0 {
		bucket = time.Minute
	}
	// Align the window start down to a bucket boundary so buckets are stable.
	start := since.Truncate(bucket)
	if until.Before(start) {
		until = start
	}
	n := int(until.Sub(start)/bucket) + 1
	if n < 1 {
		n = 1
	}
	completed := make([]int64, n)
	failed := make([]int64, n)
	for _, st := range stats {
		idx := int(st.Timestamp.Sub(start) / bucket)
		if idx < 0 || idx >= n {
			continue
		}
		completed[idx] += st.Completed
		failed[idx] += st.Failed
	}
	resp.Completed = make([]*jobsv1.DataPoint, n)
	resp.Failed = make([]*jobsv1.DataPoint, n)
	for i := 0; i < n; i++ {
		ts := timestamppb.New(start.Add(time.Duration(i) * bucket))
		resp.Completed[i] = &jobsv1.DataPoint{Timestamp: ts, Value: completed[i]}
		resp.Failed[i] = &jobsv1.DataPoint{Timestamp: ts, Value: failed[i]}
	}
	return resp
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
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			return nil, connectErr
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	pbJobs := make([]*jobsv1.Job, len(jobs))
	for i, j := range jobs {
		pbJobs[i] = s.jobToProto(j)
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
		Job:         s.jobToProto(job),
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
		Job: s.jobToProto(job),
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
	if len(req.Msg.Ids) > maxBulkJobIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("too many job IDs: max %d", maxBulkJobIDs))
	}

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
	if len(req.Msg.Ids) > maxBulkJobIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("too many job IDs: max %d", maxBulkJobIDs))
	}

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

// PauseJob pauses a specific job.
func (s *jobsService) PauseJob(ctx context.Context, req *connect.Request[jobsv1.PauseJobRequest]) (*connect.Response[jobsv1.PauseJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.PauseJob(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	job, err := s.storage.GetJob(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&jobsv1.PauseJobResponse{Job: s.jobToProto(job)}), nil
}

// CancelJob cooperatively cancels a running job by aliasing aggressive pause.
func (s *jobsService) CancelJob(ctx context.Context, req *connect.Request[jobsv1.CancelJobRequest]) (*connect.Response[jobsv1.CancelJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.CancelJob(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	job, err := s.storage.GetJob(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&jobsv1.CancelJobResponse{Job: s.jobToProto(job)}), nil
}

// ResumeJob resumes a paused job.
func (s *jobsService) ResumeJob(ctx context.Context, req *connect.Request[jobsv1.ResumeJobRequest]) (*connect.Response[jobsv1.ResumeJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.ResumeJob(ctx, req.Msg.Id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	job, err := s.storage.GetJob(ctx, req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&jobsv1.ResumeJobResponse{Job: s.jobToProto(job)}), nil
}

// PauseQueue pauses an entire queue.
func (s *jobsService) PauseQueue(ctx context.Context, req *connect.Request[jobsv1.PauseQueueRequest]) (*connect.Response[jobsv1.PauseQueueResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.PauseQueue(ctx, req.Msg.Name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobsv1.PauseQueueResponse{}), nil
}

// ResumeQueue resumes a paused queue.
func (s *jobsService) ResumeQueue(ctx context.Context, req *connect.Request[jobsv1.ResumeQueueRequest]) (*connect.Response[jobsv1.ResumeQueueResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.ResumeQueue(ctx, req.Msg.Name); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobsv1.ResumeQueueResponse{}), nil
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
	if req.Msg.Name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("queue name is required"))
	}
	if !isValidPurgeStatus(req.Msg.Status) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid purge status %q", req.Msg.Status))
	}

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
		var lastRuns map[string]time.Time
		if fireTimes, ok := s.storage.(scheduledFireTimesStorage); ok {
			var err error
			lastRuns, err = fireTimes.GetScheduledFireTimes(ctx)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}

		scheduled := s.queue.GetScheduledJobs()
		now := time.Now()
		for name, sj := range scheduled {
			// Format schedule - use type assertion or just store the name
			schedStr := name // Fallback to name if no string method
			if stringer, ok := interface{}(sj.Schedule).(interface{ String() string }); ok {
				schedStr = stringer.String()
			}
			info := &jobsv1.ScheduledJobInfo{
				Name:     name,
				Schedule: schedStr,
				Queue:    sj.Options.Queue,
			}
			if sj.Schedule != nil {
				if nextRun := sj.Schedule.Next(now); !nextRun.IsZero() {
					info.NextRun = timestamppb.New(nextRun)
				}
			}
			if lastRun, ok := lastRuns[name]; ok {
				info.LastRun = timestamppb.New(lastRun)
			}
			jobsList = append(jobsList, info)
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

	allFanOuts, allChildren, err := s.collectWorkflowTree(ctx, root)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&jobsv1.GetWorkflowResponse{
		Root:     s.jobToProto(root),
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

	fanOutsByParent, err := s.getFanOutsForParents(ctx, roots)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	summaries := make([]*jobsv1.WorkflowSummary, 0, len(roots))
	for _, root := range roots {
		summary := &jobsv1.WorkflowSummary{
			RootJob: s.jobToProto(root),
		}
		fanOuts := fanOutsByParent[root.ID]

		if len(fanOuts) > 0 {
			// Use strategy from the first fan-out as representative.
			summary.Strategy = string(fanOuts[0].Strategy)

			var totalJobs, completedJobs, failedJobs, cancelledJobs int32
			for _, fo := range fanOuts {
				totalJobs += int32(fo.TotalCount)
				completedJobs += int32(fo.CompletedCount)
				failedJobs += int32(fo.FailedCount)
				cancelledJobs += int32(fo.CancelledCount)
			}
			summary.TotalJobs = totalJobs
			summary.CompletedJobs = completedJobs
			summary.FailedJobs = failedJobs
			summary.RunningJobs = totalJobs - completedJobs - failedJobs - cancelledJobs
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

// WatchEvents streams real-time job events on a best-effort basis. Queue event
// delivery may drop events for slow consumers; clients should resync via ListJobs.
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
				} else {
					eventQueue = pbEvent.Queue
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
	if depth, ok := s.storage.(queueDepthStatsStorage); ok {
		return depth.GetQueueDepthStats(ctx)
	}

	if ui, ok := s.storage.(UIStorage); ok {
		return ui.GetQueueStats(ctx)
	}

	// Fallback: query all jobs and aggregate (capped to prevent OOM)
	const maxFallbackJobs = 1000
	stats := make(map[string]*jobsv1.QueueStats)

	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted, core.StatusFailed, core.StatusPaused} {
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
			case core.StatusPaused:
				qs.Paused++
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
	if req.Status == statusDeadLetteredUI {
		deadLettered, ok := s.storage.(deadLetterStorage)
		if !ok {
			return nil, 0, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("dead-lettered job filter requires storage with DLQ query support"))
		}
		filter := core.DeadLetterFilter{
			Queue:        req.Queue,
			Type:         req.Type,
			Tenant:       req.Tenant,
			MetaContains: metadataMapFromProto(req.MetaContains),
			Search:       req.Search,
			Limit:        limit,
			Offset:       (page - 1) * limit,
		}
		jobs, err := deadLettered.ListDeadLettered(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		total, err := deadLettered.CountDeadLettered(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		return jobs, total, nil
	}

	// Check if storage implements UIStorage
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.SearchJobs(ctx, JobFilter{
			Status:       req.Status,
			Queue:        req.Queue,
			Type:         req.Type,
			Tenant:       req.Tenant,
			MetaContains: metadataMapFromProto(req.MetaContains),
			Search:       req.Search,
			Limit:        limit,
			Offset:       (page - 1) * limit,
		})
	}

	return nil, 0, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("ListJobs requires storage with UI search support"))
}

func (s *jobsService) collectWorkflowTree(ctx context.Context, root *core.Job) ([]*jobsv1.FanOut, []*jobsv1.Job, error) {
	if batch, ok := s.storage.(workflowBatchStorage); ok {
		return s.collectWorkflowTreeBatched(ctx, root, batch)
	}
	return s.collectWorkflowTreeFallback(ctx, root)
}

func (s *jobsService) collectWorkflowTreeBatched(ctx context.Context, root *core.Job, batch workflowBatchStorage) ([]*jobsv1.FanOut, []*jobsv1.Job, error) {
	var allFanOuts []*jobsv1.FanOut
	var allChildren []*jobsv1.Job

	queue := []string{root.ID}
	visited := make(map[string]struct{}, maxWorkflowJobs)
	visited[root.ID] = struct{}{}

	for len(queue) > 0 && len(allChildren) < maxWorkflowJobs {
		currentParents := queue
		queue = nil

		fanOuts, err := batch.GetFanOutsByParents(ctx, currentParents)
		if err != nil {
			return nil, nil, err
		}
		fanOutsByParent := groupFanOutsByParent(fanOuts)

		fanOutIDs := make([]string, 0, len(fanOuts))
		for _, parentID := range currentParents {
			for _, fo := range fanOutsByParent[parentID] {
				fanOutIDs = append(fanOutIDs, fo.ID)
			}
		}

		subJobs, err := batch.GetSubJobsByFanOuts(ctx, fanOutIDs)
		if err != nil {
			return nil, nil, err
		}
		subJobsByFanOut := groupSubJobsByFanOut(subJobs)

		for _, parentID := range currentParents {
			for _, fo := range fanOutsByParent[parentID] {
				allFanOuts = append(allFanOuts, fanOutToProto(fo))
				for _, sj := range subJobsByFanOut[fo.ID] {
					if _, seen := visited[sj.ID]; seen {
						continue
					}
					visited[sj.ID] = struct{}{}
					allChildren = append(allChildren, s.jobToProto(sj))
					queue = append(queue, sj.ID)
					if len(allChildren) >= maxWorkflowJobs {
						break
					}
				}
				if len(allChildren) >= maxWorkflowJobs {
					break
				}
			}
			if len(allChildren) >= maxWorkflowJobs {
				break
			}
		}
	}

	return allFanOuts, allChildren, nil
}

func (s *jobsService) collectWorkflowTreeFallback(ctx context.Context, root *core.Job) ([]*jobsv1.FanOut, []*jobsv1.Job, error) {
	var allFanOuts []*jobsv1.FanOut
	var allChildren []*jobsv1.Job

	queue := []string{root.ID}
	visited := make(map[string]struct{}, maxWorkflowJobs)
	visited[root.ID] = struct{}{}

	for len(queue) > 0 && len(allChildren) < maxWorkflowJobs {
		current := queue[0]
		queue = queue[1:]

		fanOuts, err := s.storage.GetFanOutsByParent(ctx, current)
		if err != nil {
			return nil, nil, err
		}
		for _, fo := range fanOuts {
			allFanOuts = append(allFanOuts, fanOutToProto(fo))

			subJobs, err := s.storage.GetSubJobs(ctx, fo.ID)
			if err != nil {
				return nil, nil, err
			}
			for _, sj := range subJobs {
				if _, seen := visited[sj.ID]; seen {
					continue
				}
				visited[sj.ID] = struct{}{}
				allChildren = append(allChildren, s.jobToProto(sj))
				queue = append(queue, sj.ID)
				if len(allChildren) >= maxWorkflowJobs {
					break
				}
			}
		}
	}

	return allFanOuts, allChildren, nil
}

func (s *jobsService) getFanOutsForParents(ctx context.Context, roots []*core.Job) (map[string][]*core.FanOut, error) {
	parentIDs := make([]string, 0, len(roots))
	for _, root := range roots {
		parentIDs = append(parentIDs, root.ID)
	}

	if batch, ok := s.storage.(interface {
		GetFanOutsByParents(context.Context, []string) ([]*core.FanOut, error)
	}); ok {
		fanOuts, err := batch.GetFanOutsByParents(ctx, parentIDs)
		if err != nil {
			return nil, err
		}
		return groupFanOutsByParent(fanOuts), nil
	}

	fanOutsByParent := make(map[string][]*core.FanOut, len(parentIDs))
	for _, parentID := range parentIDs {
		fanOuts, err := s.storage.GetFanOutsByParent(ctx, parentID)
		if err != nil {
			return nil, err
		}
		fanOutsByParent[parentID] = fanOuts
	}
	return fanOutsByParent, nil
}

func groupFanOutsByParent(fanOuts []*core.FanOut) map[string][]*core.FanOut {
	grouped := make(map[string][]*core.FanOut)
	for _, fo := range fanOuts {
		grouped[fo.ParentJobID] = append(grouped[fo.ParentJobID], fo)
	}
	return grouped
}

func groupSubJobsByFanOut(jobs []*core.Job) map[string][]*core.Job {
	grouped := make(map[string][]*core.Job)
	for _, job := range jobs {
		if job.FanOutID == nil {
			continue
		}
		grouped[*job.FanOutID] = append(grouped[*job.FanOutID], job)
	}
	return grouped
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

// isValidPurgeStatus reports whether status names a terminal/idle job state that
// is safe to bulk-purge. Running, retrying, and waiting are intentionally
// excluded: purging a running/retrying job deletes a row a worker still holds
// (locked_by/locked_until) so its later Complete/Fail/Heartbeat hits a vanished
// row, and purging a waiting fan-out parent (or its in-flight sub-jobs) corrupts
// fan-out accounting. Empty/unknown values are rejected.
func isValidPurgeStatus(status string) bool {
	switch core.JobStatus(status) {
	case core.StatusPending,
		core.StatusCompleted,
		core.StatusFailed,
		core.StatusCancelled,
		core.StatusPaused:
		return true
	default:
		return false
	}
}

// Conversion helpers

// redactBytes redacts secret-shaped substrings from a payload without
// truncating it (payloads may be large). nil in -> nil out.
func redactBytes(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	return []byte(security.RedactSecrets(string(b)))
}

func redactString(s string) string {
	if s == "" {
		return s
	}
	return security.RedactSecrets(s)
}

func (s *jobsService) jobToProto(j *core.Job) *jobsv1.Job {
	return jobToProtoWithMetadataRedaction(j, s.metadataRedaction)
}

func jobToProto(j *core.Job) *jobsv1.Job {
	return jobToProtoWithMetadataRedaction(j, true)
}

func jobToProtoWithMetadataRedaction(j *core.Job, redactMetadata bool) *jobsv1.Job {
	pb := &jobsv1.Job{
		Id:          j.ID,
		Type:        j.Type,
		Queue:       j.Queue,
		Status:      string(j.Status),
		Priority:    int32(j.Priority),
		Attempt:     int32(j.Attempt),
		MaxRetries:  int32(j.MaxRetries),
		Args:        redactBytes(j.Args),
		LastError:   security.SanitizeErrorMessage(j.LastError),
		CreatedAt:   timestamppb.New(j.CreatedAt),
		FanOutIndex: int32(j.FanOutIndex),
		Result:      redactBytes(j.Result),
		Worker:      j.LockedBy,
		Tenant:      j.Tenant,
		Metadata:    metadataMapToProto(j.Metadata, redactMetadata),
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
	if j.DeadLetteredAt != nil {
		pb.DeadLetteredAt = timestamppb.New(*j.DeadLetteredAt)
	}
	pb.DeadLetterReason = j.DeadLetterReason
	return pb
}

func metadataMapFromProto(metadata map[string]string) *core.MetadataMap {
	if len(metadata) == 0 {
		return nil
	}
	out := make(core.MetadataMap, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return &out
}

func metadataMapToProto(metadata map[string]string, redactValues bool) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if redactValues {
			value = redactString(value)
		}
		out[key] = value
	}
	return out
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
		Result:    redactBytes(cp.Result),
		Error:     security.SanitizeErrorMessage(cp.Error),
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
			Queue:     ev.Queue,
			Timestamp: timestamppb.New(ev.Timestamp),
		}
	case *core.QueueResumed:
		return &jobsv1.Event{
			Type:      "queue.resumed",
			Queue:     ev.Queue,
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
	case *core.JobReclaimed:
		return &jobsv1.Event{
			Type:      "job.reclaimed",
			JobId:     ev.JobID,
			WorkerId:  ev.WorkerID,
			Reason:    ev.Reason,
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
	case "30d":
		since = until.Add(-30 * 24 * time.Hour)
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

type workflowBatchStorage interface {
	GetFanOutsByParents(ctx context.Context, parentJobIDs []string) ([]*core.FanOut, error)
	GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []string) ([]*core.Job, error)
}

type queueDepthStatsStorage interface {
	GetQueueDepthStats(ctx context.Context) ([]*jobsv1.QueueStats, error)
}

type activeWorkersStorage interface {
	CountActiveWorkers(ctx context.Context) (int64, error)
}

type scheduledFireTimesStorage interface {
	GetScheduledFireTimes(ctx context.Context) (map[string]time.Time, error)
}

type deadLetterStorage interface {
	ListDeadLettered(ctx context.Context, filter core.DeadLetterFilter) ([]*core.Job, error)
	CountDeadLettered(ctx context.Context, filter core.DeadLetterFilter) (int64, error)
}

// JobFilter is an alias for core.JobFilter for backward compatibility.
type JobFilter = core.JobFilter
