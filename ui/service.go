package ui

import (
	"context"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	jobsv1 "github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1/jobsv1connect"
)

// jobsService implements the JobsService Connect-RPC service.
type jobsService struct {
	jobsv1connect.UnimplementedJobsServiceHandler
	storage      core.Storage
	queue        *queue.Queue
	statsStorage StatsStorage
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

// WatchEvents streams real-time job events.
func (s *jobsService) WatchEvents(ctx context.Context, req *connect.Request[jobsv1.WatchEventsRequest], stream *connect.ServerStream[jobsv1.Event]) error {
	if s.queue == nil {
		<-ctx.Done()
		return ctx.Err()
	}

	events := s.queue.Events()
	filterQueues := req.Msg.Queues

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-events:
			pbEvent := coreEventToProto(e)
			if pbEvent == nil {
				continue
			}

			if len(filterQueues) > 0 {
				eventQueue := ""
				if pbEvent.Job != nil {
					eventQueue = pbEvent.Job.Queue
				}
				if !containsString(filterQueues, eventQueue) {
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

	// Fallback: query all jobs and aggregate
	stats := make(map[string]*jobsv1.QueueStats)

	for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted, core.StatusFailed} {
		jobs, err := s.storage.GetJobsByStatus(ctx, status, 10000)
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

	// Fallback: basic implementation - fetch jobs from all statuses
	var allJobs []*core.Job

	if req.Status != "" {
		// Filter by specific status
		jobs, err := s.storage.GetJobsByStatus(ctx, core.JobStatus(req.Status), limit*page)
		if err != nil {
			return nil, 0, err
		}
		allJobs = jobs
	} else {
		// No status filter - fetch from all statuses
		for _, status := range []core.JobStatus{core.StatusPending, core.StatusRunning, core.StatusCompleted, core.StatusFailed} {
			jobs, err := s.storage.GetJobsByStatus(ctx, status, limit*page)
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
		Id:         j.ID,
		Type:       j.Type,
		Queue:      j.Queue,
		Status:     string(j.Status),
		Priority:   int32(j.Priority),
		Attempt:    int32(j.Attempt),
		MaxRetries: int32(j.MaxRetries),
		Args:       j.Args,
		LastError:  j.LastError,
		CreatedAt:  timestamppb.New(j.CreatedAt),
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

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
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
	SearchJobs(ctx context.Context, filter JobFilter) ([]*core.Job, int64, error)
	RetryJob(ctx context.Context, jobID string) (*core.Job, error)
	DeleteJob(ctx context.Context, jobID string) error
	PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error)
}

// JobFilter holds search criteria for jobs.
type JobFilter struct {
	Status string
	Queue  string
	Type   string
	Search string
	Since  time.Time
	Until  time.Time
	Limit  int
	Offset int
}
