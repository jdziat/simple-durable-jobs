<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'

  let workflows = $state<Array<{
    rootJobId: string
    rootJobType: string
    rootJobStatus: string
    totalJobs: number
    completedJobs: number
    failedJobs: number
    runningJobs: number
    strategy: string
    createdAt: Date | null
  }>>([])
  let loading = $state(true)
  let error = $state<string | null>(null)

  async function loadWorkflows() {
    try {
      loading = true
      const response = await jobsClient.listWorkflows({ page: 1, limit: 50 })
      workflows = response.workflows.map(wf => ({
        rootJobId: wf.rootJob?.id ?? '',
        rootJobType: wf.rootJob?.type ?? '',
        rootJobStatus: wf.rootJob?.status ?? '',
        totalJobs: wf.totalJobs,
        completedJobs: wf.completedJobs,
        failedJobs: wf.failedJobs,
        runningJobs: wf.runningJobs,
        strategy: wf.strategy,
        createdAt: wf.rootJob?.createdAt?.toDate() ?? null,
      }))
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load workflows'
    } finally {
      loading = false
    }
  }

  onMount(() => {
    loadWorkflows()
    const interval = setInterval(loadWorkflows, 5000)
    return () => clearInterval(interval)
  })

  function progressPct(wf: typeof workflows[0]): number {
    if (wf.totalJobs === 0) return 0
    return Math.round((wf.completedJobs / wf.totalJobs) * 100)
  }

  function failedPct(wf: typeof workflows[0]): number {
    if (wf.totalJobs === 0) return 0
    return Math.round((wf.failedJobs / wf.totalJobs) * 100)
  }
</script>

<div class="workflows-page">
  <h2>Workflows</h2>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if workflows.length === 0}
    <p class="empty">No workflows found</p>
  {:else}
    <div class="workflow-list">
      {#each workflows as wf}
        <a
          href="#/workflows/{wf.rootJobId}"
          class="workflow-card"
          aria-label="View workflow {wf.rootJobType}"
        >
          <div class="card-header">
            <div class="card-title">
              <span class="job-type">{wf.rootJobType}</span>
              {#if wf.strategy}
                <span class="strategy-badge">{wf.strategy}</span>
              {/if}
            </div>
            <span class="status status-{wf.rootJobStatus}">{wf.rootJobStatus}</span>
          </div>

          <div class="progress-section">
            <div class="progress-bar" role="progressbar" aria-valuenow={progressPct(wf)} aria-valuemin={0} aria-valuemax={100}>
              <div class="progress-fill progress-completed" style="width: {progressPct(wf)}%"></div>
              <div class="progress-fill progress-failed" style="width: {failedPct(wf)}%"></div>
            </div>
            <div class="progress-text">
              <span>{wf.completedJobs}/{wf.totalJobs} completed</span>
              {#if wf.failedJobs > 0}
                <span class="failed-count">{wf.failedJobs} failed</span>
              {/if}
            </div>
          </div>

          <div class="card-footer">
            <span class="created-time">
              {wf.createdAt ? wf.createdAt.toLocaleString() : '-'}
            </span>
          </div>
        </a>
      {/each}
    </div>
  {/if}
</div>

<style>
  .workflows-page h2 {
    margin-bottom: 24px;
  }

  .workflow-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .workflow-card {
    display: block;
    background: white;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    padding: 20px;
    text-decoration: none;
    color: inherit;
    transition: box-shadow 0.15s, transform 0.1s;
  }

  .workflow-card:hover {
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
    transform: translateY(-1px);
  }

  .card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
  }

  .card-title {
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .job-type {
    font-weight: 600;
    font-size: 15px;
    color: #111827;
  }

  .strategy-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 500;
    background: #1a1a2e;
    color: white;
    text-transform: lowercase;
  }

  .status {
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 500;
    white-space: nowrap;
  }

  .status-pending  { background: #fef3c7; color: #92400e; }
  .status-running  { background: #dbeafe; color: #1e40af; }
  .status-completed { background: #d1fae5; color: #065f46; }
  .status-failed   { background: #fee2e2; color: #991b1b; }

  .progress-section {
    margin-bottom: 12px;
  }

  .progress-bar {
    position: relative;
    display: flex;
    height: 8px;
    background: #e5e7eb;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 6px;
  }

  .progress-fill {
    height: 100%;
    transition: width 0.3s ease;
  }

  .progress-completed {
    background: #22c55e;
  }

  .progress-failed {
    background: #ef4444;
  }

  .progress-text {
    display: flex;
    gap: 12px;
    font-size: 13px;
    color: #6b7280;
  }

  .failed-count {
    color: #991b1b;
    font-weight: 500;
  }

  .card-footer {
    font-size: 12px;
    color: #9ca3af;
  }

  .empty {
    color: #9ca3af;
    font-size: 15px;
  }

  .loading { color: #666; }
  .error   { color: #ef4444; }
</style>
