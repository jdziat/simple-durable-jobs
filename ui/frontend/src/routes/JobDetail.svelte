<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import WaterfallChart from '../components/WaterfallChart.svelte'
  import type { Job as ProtoJob, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

  let { id, navigate }: { id: string; navigate: (path: string) => void } = $props()

  let job = $state<{
    id: string
    type: string
    queue: string
    status: string
    priority: number
    attempt: number
    maxRetries: number
    args: string
    lastError: string
    createdAt: Date | null
    startedAt: Date | null
    completedAt: Date | null
  } | null>(null)

  let checkpoints = $state<Array<{
    id: string
    callIndex: number
    callType: string
    result: string
    error: string
    createdAt: Date | null
  }>>([])

  let loading = $state(true)
  let error = $state<string | null>(null)

  let workflowRoot = $state<ProtoJob | null>(null)
  let workflowFanOuts = $state<FanOut[]>([])
  let workflowChildren = $state<ProtoJob[]>([])

  async function loadWorkflow(jobData: { parentJobId?: string; rootJobId?: string; fanOutId?: string; id: string }) {
    if (!jobData.parentJobId && !jobData.fanOutId) return
    try {
      const rootId = jobData.rootJobId || jobData.id
      const response = await jobsClient.getWorkflow({ jobId: rootId })
      if (response.root) {
        workflowRoot = response.root
        workflowFanOuts = response.fanOuts
        workflowChildren = response.children
      }
    } catch {
      // Silently ignore - workflow is optional
    }
  }

  async function loadJob() {
    try {
      const response = await jobsClient.getJob({ id })
      if (response.job) {
        const j = response.job
        job = {
          id: j.id,
          type: j.type,
          queue: j.queue,
          status: j.status,
          priority: j.priority,
          attempt: j.attempt,
          maxRetries: j.maxRetries,
          args: formatJson(j.args),
          lastError: j.lastError,
          createdAt: j.createdAt?.toDate() ?? null,
          startedAt: j.startedAt?.toDate() ?? null,
          completedAt: j.completedAt?.toDate() ?? null,
        }
        checkpoints = response.checkpoints.map(cp => ({
          id: cp.id,
          callIndex: cp.callIndex,
          callType: cp.callType,
          result: formatJson(cp.result),
          error: cp.error,
          createdAt: cp.createdAt?.toDate() ?? null,
        }))
        // Check if this job is part of a workflow
        if (j.parentJobId || j.fanOutId) {
          loadWorkflow({
            parentJobId: j.parentJobId ?? undefined,
            rootJobId: j.rootJobId ?? undefined,
            fanOutId: j.fanOutId ?? undefined,
            id: j.id,
          })
        }
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load job'
    } finally {
      loading = false
    }
  }

  function formatJson(bytes: Uint8Array): string {
    try {
      const text = new TextDecoder().decode(bytes)
      return JSON.stringify(JSON.parse(text), null, 2)
    } catch {
      return new TextDecoder().decode(bytes)
    }
  }

  async function retryJob() {
    try {
      await jobsClient.retryJob({ id })
      loadJob()
    } catch (e) {
      alert('Failed to retry job')
    }
  }

  async function deleteJob() {
    if (!confirm('Are you sure you want to delete this job?')) return
    try {
      await jobsClient.deleteJob({ id })
      navigate('#/jobs')
    } catch (e) {
      alert('Failed to delete job')
    }
  }

  onMount(() => {
    loadJob()
  })
</script>

<div class="job-detail">
  <a href="#/jobs" class="back-link">&larr; Back to Jobs</a>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if job}
    <div class="header">
      <h2>{job.type}</h2>
      <span class="status status-{job.status}">{job.status}</span>
    </div>

    <div class="meta">
      <div class="meta-item">
        <span class="label">ID</span>
        <span class="value mono">{job.id}</span>
      </div>
      <div class="meta-item">
        <span class="label">Queue</span>
        <span class="value">{job.queue}</span>
      </div>
      <div class="meta-item">
        <span class="label">Priority</span>
        <span class="value">{job.priority}</span>
      </div>
      <div class="meta-item">
        <span class="label">Attempts</span>
        <span class="value">{job.attempt}/{job.maxRetries}</span>
      </div>
    </div>

    <div class="timestamps">
      <div class="ts-item">
        <span class="label">Created</span>
        <span class="value">{job.createdAt?.toLocaleString() ?? '-'}</span>
      </div>
      <div class="ts-item">
        <span class="label">Started</span>
        <span class="value">{job.startedAt?.toLocaleString() ?? '-'}</span>
      </div>
      <div class="ts-item">
        <span class="label">Completed</span>
        <span class="value">{job.completedAt?.toLocaleString() ?? '-'}</span>
      </div>
    </div>

    {#if job.lastError}
      <div class="error-box">
        <h4>Last Error</h4>
        <pre>{job.lastError}</pre>
      </div>
    {/if}

    <div class="args-box">
      <h4>Arguments</h4>
      <pre>{job.args}</pre>
    </div>

    {#if checkpoints.length > 0}
      <div class="checkpoints">
        <h4>Checkpoints</h4>
        <table>
          <thead>
            <tr>
              <th>Index</th>
              <th>Type</th>
              <th>Result/Error</th>
              <th>Created</th>
            </tr>
          </thead>
          <tbody>
            {#each checkpoints as cp}
              <tr>
                <td>{cp.callIndex}</td>
                <td>{cp.callType}</td>
                <td>
                  {#if cp.error}
                    <span class="cp-error">{cp.error}</span>
                  {:else}
                    <pre class="cp-result">{cp.result}</pre>
                  {/if}
                </td>
                <td>{cp.createdAt?.toLocaleString() ?? '-'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}

    {#if workflowRoot}
      <div class="workflow-section">
        <h4>Workflow Timeline</h4>
        <WaterfallChart
          root={workflowRoot}
          fanOuts={workflowFanOuts}
          children={workflowChildren}
          onJobClick={(jobId) => navigate(`#/jobs/${jobId}`)}
        />
      </div>
    {/if}

    <div class="actions">
      {#if job.status === 'failed'}
        <button class="btn-retry" onclick={retryJob}>Retry Job</button>
      {/if}
      <button class="btn-delete" onclick={deleteJob}>Delete Job</button>
    </div>
  {/if}
</div>

<style>
  .job-detail {
    max-width: 900px;
  }

  .back-link {
    display: inline-block;
    margin-bottom: 16px;
    color: #3b82f6;
    text-decoration: none;
  }

  .header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 24px;
  }

  .header h2 {
    margin: 0;
  }

  .status {
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 14px;
    font-weight: 500;
  }

  .status-pending { background: #fef3c7; color: #92400e; }
  .status-running { background: #dbeafe; color: #1e40af; }
  .status-completed { background: #d1fae5; color: #065f46; }
  .status-failed { background: #fee2e2; color: #991b1b; }

  .meta, .timestamps {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  .meta-item, .ts-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .label {
    font-size: 12px;
    color: #666;
    text-transform: uppercase;
  }

  .value {
    font-size: 16px;
    font-weight: 500;
  }

  .mono {
    font-family: monospace;
    font-size: 14px;
  }

  .error-box, .args-box, .checkpoints {
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    margin-bottom: 24px;
  }

  .error-box {
    border-left: 4px solid #ef4444;
  }

  .error-box h4, .args-box h4, .checkpoints h4 {
    margin: 0 0 12px;
    font-size: 14px;
    color: #666;
  }

  pre {
    background: #f5f7fa;
    padding: 12px;
    border-radius: 4px;
    overflow-x: auto;
    font-size: 13px;
    margin: 0;
  }

  .checkpoints table {
    width: 100%;
    border-collapse: collapse;
  }

  .checkpoints th,
  .checkpoints td {
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid #eee;
  }

  .checkpoints th {
    font-weight: 600;
    font-size: 12px;
    color: #666;
  }

  .cp-error {
    color: #ef4444;
  }

  .cp-result {
    font-size: 12px;
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .actions {
    display: flex;
    gap: 12px;
    margin-top: 24px;
  }

  .btn-retry, .btn-delete {
    padding: 10px 20px;
    border: none;
    border-radius: 6px;
    font-size: 14px;
    cursor: pointer;
  }

  .btn-retry { background: #3b82f6; color: white; }
  .btn-delete { background: #ef4444; color: white; }

  .workflow-section {
    margin-bottom: 24px;
  }

  .workflow-section h4 {
    margin: 0 0 12px;
    font-size: 14px;
    color: #666;
  }

  .loading { color: #666; }
  .error { color: #ef4444; }
</style>
