package ui

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/security"
	jobsv1 "github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1"
	"github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1/jobsv1connect"
)

const (
	maxWatchStreams = 50
	maxBulkJobIDs   = 1000
	maxWorkflowJobs = 1000
	// maxRootHops bounds the parent-chain climb in GetWorkflow so corrupted or
	// cyclic parent_job_id data cannot loop forever; hitting it marks the tree
	// truncated rather than silently returning a non-root as the root.
	maxRootHops          = 100
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
		return nil, errToConnect(err)
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
		resp.TotalRetrying += qs.Retrying
		resp.TotalWaiting += qs.Waiting
		resp.TotalCancelled += qs.Cancelled
	}

	if active, ok := s.storage.(activeWorkersStorage); ok {
		count, err := active.CountActiveWorkers(ctx)
		if err != nil {
			return nil, errToConnect(err)
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
		return nil, errToConnect(err)
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
		return nil, errToConnect(err)
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
	id, err := core.ParseUUID(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	job, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return nil, errToConnect(err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	checkpoints, err := s.storage.GetCheckpoints(ctx, job.ID)
	if err != nil {
		return nil, errToConnect(err)
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
		return nil, errToConnect(err)
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
	// delete_subtree=true is the explicit workflow-aware delete (removes the
	// whole fan-out subtree); otherwise deleting a workflow parent is refused.
	if req.Msg.DeleteSubtree {
		if err := s.deleteWorkflowSubtree(ctx, req.Msg.Id); err != nil {
			return nil, errToConnect(err)
		}
		return connect.NewResponse(&jobsv1.DeleteJobResponse{}), nil
	}
	if err := s.deleteJob(ctx, req.Msg.Id); err != nil {
		// errToConnect maps ErrJobHasChildren -> FailedPrecondition (the actionable
		// "delete the workflow instead") and everything else appropriately.
		return nil, errToConnect(err)
	}
	return connect.NewResponse(&jobsv1.DeleteJobResponse{}), nil
}

// validateBulkIDs enforces the batch-size cap and rejects the whole request with
// InvalidArgument if ANY id is malformed, before any mutation runs — so a bad id
// can't masquerade as a benign "skipped" miss. Returns the parsed ids in order.
func validateBulkIDs(ids []string) ([]core.UUID, error) {
	if len(ids) > maxBulkJobIDs {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("too many job IDs: max %d", maxBulkJobIDs))
	}
	parsed := make([]core.UUID, len(ids))
	for i, id := range ids {
		uid, err := core.ParseUUID(id)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid job id %q: %w", id, err))
		}
		parsed[i] = uid
	}
	return parsed, nil
}

// BulkRetryJobs retries multiple jobs. count is the number retried; every id NOT
// retried is reported in skipped with a reason, so a reduced count is no longer
// the only (ambiguous) signal of loss (F-003).
func (s *jobsService) BulkRetryJobs(ctx context.Context, req *connect.Request[jobsv1.BulkRetryJobsRequest]) (*connect.Response[jobsv1.BulkRetryJobsResponse], error) {
	if _, err := validateBulkIDs(req.Msg.Ids); err != nil {
		return nil, err
	}

	count := 0
	var skipped []*jobsv1.BulkSkip
	for _, id := range req.Msg.Ids {
		if _, err := s.retryJob(ctx, id); err != nil {
			skipped = append(skipped, &jobsv1.BulkSkip{Id: id, Reason: bulkSkipReason(err)})
			continue
		}
		count++
	}
	return connect.NewResponse(&jobsv1.BulkRetryJobsResponse{
		Count:   int32(count),
		Skipped: skipped,
	}), nil
}

// BulkDeleteJobs deletes multiple jobs. See BulkRetryJobs for the count/skipped
// contract (F-003).
func (s *jobsService) BulkDeleteJobs(ctx context.Context, req *connect.Request[jobsv1.BulkDeleteJobsRequest]) (*connect.Response[jobsv1.BulkDeleteJobsResponse], error) {
	if _, err := validateBulkIDs(req.Msg.Ids); err != nil {
		return nil, err
	}

	count := 0
	var skipped []*jobsv1.BulkSkip
	for _, id := range req.Msg.Ids {
		if err := s.deleteJob(ctx, id); err != nil {
			skipped = append(skipped, &jobsv1.BulkSkip{Id: id, Reason: bulkSkipReason(err)})
			continue
		}
		count++
	}
	return connect.NewResponse(&jobsv1.BulkDeleteJobsResponse{
		Count:   int32(count),
		Skipped: skipped,
	}), nil
}

// PauseJob pauses a specific job.
func (s *jobsService) PauseJob(ctx context.Context, req *connect.Request[jobsv1.PauseJobRequest]) (*connect.Response[jobsv1.PauseJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	id, err := core.ParseUUID(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.queue.PauseJob(ctx, id); err != nil {
		return nil, errToConnect(err)
	}
	job, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return nil, errToConnect(err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&jobsv1.PauseJobResponse{Job: s.jobToProto(job)}), nil
}

// CancelJob terminally cancels a job.
func (s *jobsService) CancelJob(ctx context.Context, req *connect.Request[jobsv1.CancelJobRequest]) (*connect.Response[jobsv1.CancelJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	id, err := core.ParseUUID(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.queue.CancelJob(ctx, id); err != nil {
		return nil, errToConnect(err)
	}
	job, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return nil, errToConnect(err)
	}
	if job == nil {
		// The cancel already succeeded; the row just can't be read back (e.g. a
		// retention GC raced the re-read). Return a minimal cancelled job rather
		// than a misleading NotFound for an operation that did complete.
		job = &core.Job{ID: id, Status: core.StatusCancelled}
	}
	return connect.NewResponse(&jobsv1.CancelJobResponse{Job: s.jobToProto(job)}), nil
}

// ResumeJob resumes a paused job.
func (s *jobsService) ResumeJob(ctx context.Context, req *connect.Request[jobsv1.ResumeJobRequest]) (*connect.Response[jobsv1.ResumeJobResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	id, err := core.ParseUUID(req.Msg.Id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.queue.ResumeJob(ctx, id); err != nil {
		return nil, errToConnect(err)
	}
	job, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return nil, errToConnect(err)
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
		return nil, errToConnect(err)
	}
	return connect.NewResponse(&jobsv1.PauseQueueResponse{}), nil
}

// ResumeQueue resumes a paused queue.
func (s *jobsService) ResumeQueue(ctx context.Context, req *connect.Request[jobsv1.ResumeQueueRequest]) (*connect.Response[jobsv1.ResumeQueueResponse], error) {
	if s.queue == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, nil)
	}
	if err := s.queue.ResumeQueue(ctx, req.Msg.Name); err != nil {
		return nil, errToConnect(err)
	}
	return connect.NewResponse(&jobsv1.ResumeQueueResponse{}), nil
}

// ListQueues returns all queues with stats.
func (s *jobsService) ListQueues(ctx context.Context, req *connect.Request[jobsv1.ListQueuesRequest]) (*connect.Response[jobsv1.ListQueuesResponse], error) {
	stats, err := s.getQueueStats(ctx)
	if err != nil {
		return nil, errToConnect(err)
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
		return nil, errToConnect(err)
	}
	return connect.NewResponse(&jobsv1.PurgeQueueResponse{
		Deleted: deleted,
	}), nil
}

// nextRunCatchUpCap bounds the UI's forward walk over missed schedule boundaries
// so a pathologically dense schedule over a long outage cannot spin a dashboard
// request. It mirrors the scheduler's maxCatchUpIterations (worker.go); real
// schedules are far coarser and exit in a handful of steps.
const nextRunCatchUpCap = 100_000

// ListScheduledJobs returns registered scheduled jobs.
func (s *jobsService) ListScheduledJobs(ctx context.Context, req *connect.Request[jobsv1.ListScheduledJobsRequest]) (*connect.Response[jobsv1.ListScheduledJobsResponse], error) {
	var jobsList []*jobsv1.ScheduledJobInfo

	if s.queue != nil {
		var lastRuns map[string]time.Time
		if fireTimes, ok := s.storage.(scheduledFireTimesStorage); ok {
			var err error
			lastRuns, err = fireTimes.GetScheduledFireTimes(ctx)
			if err != nil {
				return nil, errToConnect(err)
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
			// Anchor NextRun on the persisted last run, then walk forward to the
			// first fire still in the future. The scheduler computes the next fire
			// from the last run (worker.go), but a single Schedule.Next(lastRun)
			// step returns a boundary already in the PAST when the last run is more
			// than one interval behind now (worker down / schedule paused). The
			// scheduler resolves this with a catch-up walk (seedLastRun); the
			// read-only UI mirrors just enough of it to show the next FUTURE fire,
			// bounded identically so a pathological schedule cannot spin the
			// request. Walking (rather than anchoring on max(lastRun, now))
			// preserves the schedule's phase for anchor-relative schedules, exactly
			// as the scheduler does. It still does not replicate the durable
			// fleet-anchor seeding (establishScheduleBase/SeedScheduledFire), so a
			// never-fired interval schedule's NextRun is a best-effort estimate
			// until the first real fire.
			anchor := now
			if lastRun, ok := lastRuns[name]; ok {
				anchor = lastRun
				info.LastRun = timestamppb.New(lastRun)
			}
			if sj.Schedule != nil {
				nextRun := sj.Schedule.Next(anchor)
				for iter := 0; !nextRun.IsZero() && !nextRun.After(now); iter++ {
					if iter >= nextRunCatchUpCap {
						// Pathologically dense schedule over a long gap: resume from
						// now (mirrors seedLastRun's capped fallback). Unreachable
						// for real, coarser schedules.
						nextRun = sj.Schedule.Next(now)
						break
					}
					nextRun = sj.Schedule.Next(nextRun)
				}
				if !nextRun.IsZero() {
					info.NextRun = timestamppb.New(nextRun)
				}
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
	id, err := core.ParseUUID(req.Msg.JobId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	job, err := s.storage.GetJob(ctx, id)
	if err != nil {
		return nil, errToConnect(err)
	}
	if job == nil {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	// Walk up the parent chain to find the root. Bounded by maxRootHops AND a
	// cycle-detecting seen-set so corrupted/cyclic parent_job_id data cannot loop
	// forever; if we stop with a parent still pending (cap or cycle), the returned
	// "root" is NOT the true root, so mark the tree truncated rather than lie.
	root := job
	rootTruncated := false
	seen := map[core.UUID]struct{}{root.ID: {}}
	for hops := 0; root.ParentJobID != nil; hops++ {
		if hops >= maxRootHops {
			rootTruncated = true
			break
		}
		if _, cycle := seen[*root.ParentJobID]; cycle {
			rootTruncated = true
			break
		}
		parent, err := s.storage.GetJob(ctx, *root.ParentJobID)
		if err != nil {
			return nil, errToConnect(err)
		}
		if parent == nil {
			// Parent no longer exists; treat current node as root.
			break
		}
		seen[parent.ID] = struct{}{}
		root = parent
	}

	allFanOuts, allChildren, treeTruncated, err := s.collectWorkflowTree(ctx, root)
	if err != nil {
		return nil, errToConnect(err)
	}

	return connect.NewResponse(&jobsv1.GetWorkflowResponse{
		Root:      s.jobToProto(root),
		FanOuts:   allFanOuts,
		Children:  allChildren,
		Truncated: rootTruncated || treeTruncated,
	}), nil
}

// strategyMixed is the display-only aggregate label for a workflow whose fan-outs
// do not all share one strategy. It is intentionally NOT a core.FanOutStrategy and
// NOT in core.AllFanOutStrategies (which the DB CHECK constraint enforces) — it
// only ever appears in the read-only UI workflow summary.
const strategyMixed = "mixed"

// aggregateStrategy reports the single strategy shared by every fan-out in a
// workflow, or strategyMixed when they differ (a workflow can hold multiple
// fan-outs with different strategies; reporting only the first was misleading).
// Callers pass a non-empty slice.
func aggregateStrategy(fanOuts []*core.FanOut) string {
	first := string(fanOuts[0].Strategy)
	for _, fo := range fanOuts[1:] {
		if string(fo.Strategy) != first {
			return strategyMixed
		}
	}
	return first
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
		return nil, errToConnect(err)
	}

	fanOutsByParent, err := s.getFanOutsForParents(ctx, roots)
	if err != nil {
		return nil, errToConnect(err)
	}

	summaries := make([]*jobsv1.WorkflowSummary, 0, len(roots))
	for _, root := range roots {
		summary := &jobsv1.WorkflowSummary{
			RootJob: s.jobToProto(root),
		}
		fanOuts := fanOutsByParent[root.ID]

		if len(fanOuts) > 0 {
			summary.Strategy = aggregateStrategy(fanOuts)

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

	for _, status := range []core.JobStatus{
		core.StatusPending,
		core.StatusRunning,
		core.StatusCompleted,
		core.StatusFailed,
		core.StatusPaused,
		core.StatusRetrying,
		core.StatusWaiting,
		core.StatusCancelled,
	} {
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
			case core.StatusRetrying:
				qs.Retrying++
			case core.StatusWaiting:
				qs.Waiting++
			case core.StatusCancelled:
				qs.Cancelled++
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
			SortKey:      req.SortKey,
			SortDir:      req.SortDir,
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
			SortKey:      req.SortKey,
			SortDir:      req.SortDir,
		})
	}

	return nil, 0, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("ListJobs requires storage with UI search support"))
}

// collectWorkflowTree returns the fan-outs and children of a workflow root. The
// truncated return is true when the BFS stopped at maxWorkflowJobs with more of
// the tree still unvisited — the caller must surface that so a partial tree is
// never presented as complete.
func (s *jobsService) collectWorkflowTree(ctx context.Context, root *core.Job) ([]*jobsv1.FanOut, []*jobsv1.Job, bool, error) {
	if batch, ok := s.storage.(workflowBatchStorage); ok {
		return s.collectWorkflowTreeBatched(ctx, root, batch)
	}
	return s.collectWorkflowTreeFallback(ctx, root)
}

func (s *jobsService) collectWorkflowTreeBatched(ctx context.Context, root *core.Job, batch workflowBatchStorage) ([]*jobsv1.FanOut, []*jobsv1.Job, bool, error) {
	var allFanOuts []*jobsv1.FanOut
	var allChildren []*jobsv1.Job

	queue := []core.UUID{root.ID}
	visited := make(map[core.UUID]struct{}, maxWorkflowJobs)
	visited[root.ID] = struct{}{}

	for len(queue) > 0 && len(allChildren) < maxWorkflowJobs {
		currentParents := queue
		queue = nil

		fanOuts, err := batch.GetFanOutsByParents(ctx, currentParents)
		if err != nil {
			return nil, nil, false, err
		}
		fanOutsByParent := groupFanOutsByParent(fanOuts)

		fanOutIDs := make([]core.UUID, 0, len(fanOuts))
		for _, parentID := range currentParents {
			for _, fo := range fanOutsByParent[parentID] {
				fanOutIDs = append(fanOutIDs, fo.ID)
			}
		}

		subJobs, err := batch.GetSubJobsByFanOuts(ctx, fanOutIDs)
		if err != nil {
			return nil, nil, false, err
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

	// queue non-empty here means the BFS stopped at maxWorkflowJobs with tree left.
	return allFanOuts, allChildren, len(queue) > 0, nil
}

func (s *jobsService) collectWorkflowTreeFallback(ctx context.Context, root *core.Job) ([]*jobsv1.FanOut, []*jobsv1.Job, bool, error) {
	var allFanOuts []*jobsv1.FanOut
	var allChildren []*jobsv1.Job

	queue := []core.UUID{root.ID}
	visited := make(map[core.UUID]struct{}, maxWorkflowJobs)
	visited[root.ID] = struct{}{}

	for len(queue) > 0 && len(allChildren) < maxWorkflowJobs {
		current := queue[0]
		queue = queue[1:]

		fanOuts, err := s.storage.GetFanOutsByParent(ctx, current)
		if err != nil {
			return nil, nil, false, err
		}
		for _, fo := range fanOuts {
			allFanOuts = append(allFanOuts, fanOutToProto(fo))

			subJobs, err := s.storage.GetSubJobs(ctx, fo.ID)
			if err != nil {
				return nil, nil, false, err
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

	// queue non-empty here means the BFS stopped at maxWorkflowJobs with tree left.
	return allFanOuts, allChildren, len(queue) > 0, nil
}

func (s *jobsService) getFanOutsForParents(ctx context.Context, roots []*core.Job) (map[core.UUID][]*core.FanOut, error) {
	parentIDs := make([]core.UUID, 0, len(roots))
	for _, root := range roots {
		parentIDs = append(parentIDs, root.ID)
	}

	if batch, ok := s.storage.(interface {
		GetFanOutsByParents(context.Context, []core.UUID) ([]*core.FanOut, error)
	}); ok {
		fanOuts, err := batch.GetFanOutsByParents(ctx, parentIDs)
		if err != nil {
			return nil, err
		}
		return groupFanOutsByParent(fanOuts), nil
	}

	fanOutsByParent := make(map[core.UUID][]*core.FanOut, len(parentIDs))
	for _, parentID := range parentIDs {
		fanOuts, err := s.storage.GetFanOutsByParent(ctx, parentID)
		if err != nil {
			return nil, err
		}
		fanOutsByParent[parentID] = fanOuts
	}
	return fanOutsByParent, nil
}

func groupFanOutsByParent(fanOuts []*core.FanOut) map[core.UUID][]*core.FanOut {
	grouped := make(map[core.UUID][]*core.FanOut)
	for _, fo := range fanOuts {
		grouped[fo.ParentJobID] = append(grouped[fo.ParentJobID], fo)
	}
	return grouped
}

func groupSubJobsByFanOut(jobs []*core.Job) map[core.UUID][]*core.Job {
	grouped := make(map[core.UUID][]*core.Job)
	for _, job := range jobs {
		if job.FanOutID == nil {
			continue
		}
		grouped[*job.FanOutID] = append(grouped[*job.FanOutID], job)
	}
	return grouped
}

func (s *jobsService) retryJob(ctx context.Context, id string) (*core.Job, error) {
	jobID, err := core.ParseUUID(id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.RetryJob(ctx, jobID)
	}

	// Fallback: not supported without UIStorage
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}

func (s *jobsService) deleteJob(ctx context.Context, id string) error {
	jobID, err := core.ParseUUID(id)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if ui, ok := s.storage.(UIStorage); ok {
		return ui.DeleteJob(ctx, jobID)
	}
	return connect.NewError(connect.CodeUnimplemented, nil)
}

// workflowSubtreeDeleter is an optional storage capability (asserted, not part of
// the UIStorage interface, so the interface stays api-compatible) for the
// workflow-aware delete-subtree path.
type workflowSubtreeDeleter interface {
	DeleteWorkflowSubtree(ctx context.Context, rootJobID core.UUID) error
}

func (s *jobsService) deleteWorkflowSubtree(ctx context.Context, id string) error {
	jobID, err := core.ParseUUID(id)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if d, ok := s.storage.(workflowSubtreeDeleter); ok {
		return d.DeleteWorkflowSubtree(ctx, jobID)
	}
	return connect.NewError(connect.CodeUnimplemented, nil)
}

// errToConnect maps a storage/queue error to a connect.Error with an accurate,
// non-leaky code. It is the single funnel every dashboard handler uses for errors
// returned by storage/queue calls, replacing the old blanket
// connect.NewError(CodeInternal, err):
//
//   - An existing *connect.Error is returned unchanged, so codes set by inner
//     helpers (CodeInvalidArgument for a bad UUID, CodeUnimplemented for a
//     non-UI storage) survive instead of collapsing to Internal (F-002).
//   - Known, user-actionable sentinels map to their semantic code, preserving
//     their safe descriptive message (these carry no backend internals).
//   - Anything else is logged in full server-side and returned as a generic
//     Internal, so raw backend/SQL error text never reaches the dashboard
//     client (SEC-3).
func errToConnect(err error) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce
	}
	switch {
	case errors.Is(err, core.ErrJobNotFound), errors.Is(err, gorm.ErrRecordNotFound):
		return connect.NewError(connect.CodeNotFound, errors.New("job not found"))
	case errors.Is(err, core.ErrJobHasChildren),
		errors.Is(err, core.ErrJobAlreadyPaused),
		errors.Is(err, core.ErrJobNotPaused),
		errors.Is(err, core.ErrJobNotCancellable),
		errors.Is(err, core.ErrCannotPauseStatus),
		errors.Is(err, core.ErrQueueAlreadyPaused),
		errors.Is(err, core.ErrQueueNotPaused),
		errors.Is(err, core.ErrCannotRequeueSubJob):
		// User-actionable state errors; their sentinel text is safe to surface.
		return connect.NewError(connect.CodeFailedPrecondition, err)
	}
	slog.Default().Error("dashboard rpc storage error", "error", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}

// bulkSkipReason classifies a per-item bulk-mutation error (the ORIGINAL error
// from retryJob/deleteJob) into a short, non-leaky reason for BulkSkip.reason.
// It never echoes raw backend text; an unclassified error is logged server-side
// (as errToConnect does for single calls) and reported generically.
func bulkSkipReason(err error) string {
	switch {
	case errors.Is(err, core.ErrJobNotFound), errors.Is(err, gorm.ErrRecordNotFound):
		return "not found"
	case errors.Is(err, core.ErrJobHasChildren):
		return "workflow parent (delete the workflow instead)"
	case errors.Is(err, core.ErrCannotRequeueSubJob):
		return "fan-out sub-job (retry its parent)"
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeUnimplemented:
			return "unsupported by storage"
		case connect.CodeInvalidArgument:
			return "invalid id"
		case connect.CodeNotFound:
			return "not found"
		case connect.CodeFailedPrecondition:
			return ce.Message() // our own safe sentinel text
		}
	}
	slog.Default().Error("bulk mutation item failed", "error", err)
	return "internal error"
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
		Id:          string(j.ID),
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
		parentJobID := string(*j.ParentJobID)
		pb.ParentJobId = &parentJobID
	}
	if j.RootJobID != nil {
		rootJobID := string(*j.RootJobID)
		pb.RootJobId = &rootJobID
	}
	if j.FanOutID != nil {
		fanOutID := string(*j.FanOutID)
		pb.FanOutId = &fanOutID
	}
	if j.DeadLetteredAt != nil {
		pb.DeadLetteredAt = timestamppb.New(*j.DeadLetteredAt)
	}
	pb.DeadLetterReason = security.SanitizeErrorMessage(j.DeadLetterReason)
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
		Id:             string(f.ID),
		ParentJobId:    string(f.ParentJobID),
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
		Id:        string(cp.ID),
		JobId:     string(cp.JobID),
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
	case *core.JobCancelled:
		return &jobsv1.Event{
			Type:      "job.cancelled",
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
			JobId:     string(ev.JobID),
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
	RetryJob(ctx context.Context, jobID core.UUID) (*core.Job, error)
	DeleteJob(ctx context.Context, jobID core.UUID) error
	PurgeJobs(ctx context.Context, queue string, status core.JobStatus) (int64, error)
	// GetWorkflowRoots returns root jobs (ParentJobID == nil) that are workflow
	// parents. status filters by job status when non-empty. limit and offset
	// control pagination. Returns the matching jobs and the total count.
	GetWorkflowRoots(ctx context.Context, status string, limit, offset int) ([]*core.Job, int64, error)
}

type workflowBatchStorage interface {
	GetFanOutsByParents(ctx context.Context, parentJobIDs []core.UUID) ([]*core.FanOut, error)
	GetSubJobsByFanOuts(ctx context.Context, fanOutIDs []core.UUID) ([]*core.Job, error)
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
