<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import { deltaFlash } from '../lib/actions/deltaFlash'
  import WaterfallChart from '../components/WaterfallChart.svelte'
  import Button from '../lib/components/Button.svelte'
  import ConfirmDialog from '../lib/components/ConfirmDialog.svelte'
  import CopyButton from '../lib/components/CopyButton.svelte'
  import Duration from '../lib/components/Duration.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import RelativeTime from '../lib/components/RelativeTime.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { toast } from '../lib/stores/toast.svelte'
  import type { Job as ProtoJob, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

  let { id, navigate }: { id: string; navigate: (path: string) => void } = $props()

  type TimeValue = Date | null

  type JobDetail = {
    id: string
    type: string
    queue: string
    tenant: string
    metadata: Record<string, string>
    status: string
    priority: number
    attempt: number
    maxRetries: number
    worker: string
    args: string
    lastError: string
    createdAt: TimeValue
    startedAt: TimeValue
    completedAt: TimeValue
    deadLetteredAt: TimeValue
    deadLetterReason: string
  }

  type CheckpointItem = {
    id: string
    callIndex: number
    callType: string
    result: string
    error: string
    createdAt: TimeValue
  }

  type ConfirmState =
    | { kind: 'cancel'; id: string }
    | { kind: 'delete'; id: string }
    | { kind: 'requeue'; id: string }
    | null

  let job = $state<JobDetail | null>(null)
  let checkpoints = $state<CheckpointItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let confirmState = $state<ConfirmState>(null)
  let expandedCheckpoints = $state<Set<string>>(new Set())

  let workflowRoot = $state<ProtoJob | null>(null)
  let workflowFanOuts = $state<FanOut[]>([])
  let workflowChildren = $state<ProtoJob[]>([])

  let mounted = false
  let pollTimer: ReturnType<typeof setInterval> | null = null
  let inFlight = false

  let displayStatus = $derived(job?.deadLetteredAt ? 'dead-lettered' : (job?.status ?? ''))
  let isRunning = $derived(job?.status === 'running' && !job.deadLetteredAt)
  let metadataEntries = $derived(Object.entries(job?.metadata ?? {}).sort(([a], [b]) => a.localeCompare(b)))

  function toDate(value: { toDate?: () => Date } | undefined): Date | null {
    if (!value?.toDate) return null
    const date = value.toDate()
    return Number.isNaN(date.getTime()) ? null : date
  }

  function formatJson(bytes: Uint8Array): string {
    try {
      const text = new TextDecoder().decode(bytes)
      return JSON.stringify(JSON.parse(text), null, 2)
    } catch {
      return new TextDecoder().decode(bytes)
    }
  }

  function checkpointPayload(cp: CheckpointItem): string {
    return cp.error || cp.result || ''
  }

  function toggleCheckpoint(cpId: string) {
    const next = new Set(expandedCheckpoints)
    if (next.has(cpId)) {
      next.delete(cpId)
    } else {
      next.add(cpId)
    }
    expandedCheckpoints = next
  }

  function stopPoll() {
    if (pollTimer) clearInterval(pollTimer)
    pollTimer = null
  }

  function reconcilePoll() {
    if (isRunning) {
      if (!pollTimer) {
        pollTimer = setInterval(() => {
          loadJob({ silent: true })
        }, 3000)
      }
    } else {
      stopPoll()
    }
  }

  function clearWorkflow() {
    workflowRoot = null
    workflowFanOuts = []
    workflowChildren = []
  }

  async function loadWorkflow(jobData: { parentJobId?: string; rootJobId?: string; fanOutId?: string; id: string }) {
    try {
      const rootId = jobData.rootJobId || jobData.id
      const response = await jobsClient.getWorkflow({ jobId: rootId })
      if (!mounted) return
      const hasWorkflowContext = response.children.length > 0 || response.fanOuts.length > 0 || !!jobData.parentJobId || !!jobData.fanOutId
      if (response.root && hasWorkflowContext) {
        workflowRoot = response.root
        workflowFanOuts = response.fanOuts
        workflowChildren = response.children
      } else {
        clearWorkflow()
      }
    } catch {
      // Workflow context is optional for standalone jobs.
      if (mounted) clearWorkflow()
    }
  }

  async function loadJob({ silent = false }: { silent?: boolean } = {}) {
    if (inFlight) return
    inFlight = true
    if (!silent) {
      loading = true
      error = null
    }
    try {
      const response = await jobsClient.getJob({ id })
      if (!mounted) return
      if (!response.job) {
        job = null
        checkpoints = []
        error = 'Job not found'
        return
      }

      const j = response.job
      job = {
        id: j.id,
        type: j.type,
        queue: j.queue,
        tenant: j.tenant,
        metadata: { ...j.metadata },
        status: j.status,
        priority: j.priority,
        attempt: j.attempt,
        maxRetries: j.maxRetries,
        worker: j.worker,
        args: formatJson(j.args),
        lastError: j.lastError,
        createdAt: toDate(j.createdAt),
        startedAt: toDate(j.startedAt),
        completedAt: toDate(j.completedAt),
        deadLetteredAt: toDate(j.deadLetteredAt),
        deadLetterReason: j.deadLetterReason,
      }
      checkpoints = response.checkpoints.map(cp => ({
        id: cp.id,
        callIndex: cp.callIndex,
        callType: cp.callType,
        result: formatJson(cp.result),
        error: cp.error,
        createdAt: toDate(cp.createdAt),
      }))
      expandedCheckpoints = new Set([...expandedCheckpoints].filter(cpId => checkpoints.some(cp => cp.id === cpId)))
      error = null
      reconcilePoll()
      loadWorkflow({
        parentJobId: j.parentJobId ?? undefined,
        rootJobId: j.rootJobId ?? undefined,
        fanOutId: j.fanOutId ?? undefined,
        id: j.id,
      })
    } catch (e) {
      if (!mounted) return
      error = e instanceof Error ? e.message : 'Failed to load job'
      toast.push({ kind: 'err', msg: error })
      stopPoll()
    } finally {
      if (mounted) loading = false
      inFlight = false
    }
  }

  async function retryJob() {
    try {
      await jobsClient.retryJob({ id })
      toast.push({ kind: 'ok', msg: 'job queued for retry' })
      loadJob()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to retry job' })
    }
  }

  async function confirmRequeue() {
    if (confirmState?.kind !== 'requeue') return
    confirmState = null
    try {
      await jobsClient.retryJob({ id })
      toast.push({ kind: 'ok', msg: 'job requeued' })
      loadJob()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to requeue job' })
    }
  }

  async function pauseJob() {
    try {
      await jobsClient.pauseJob({ id })
      toast.push({ kind: 'ok', msg: 'job paused' })
      loadJob()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to pause job' })
    }
  }

  async function resumeJob() {
    try {
      await jobsClient.resumeJob({ id })
      toast.push({ kind: 'ok', msg: 'job resumed' })
      loadJob()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to resume job' })
    }
  }

  async function confirmCancel() {
    if (confirmState?.kind !== 'cancel') return
    const jobId = confirmState.id
    confirmState = null
    try {
      await jobsClient.cancelJob({ id: jobId })
      toast.push({ kind: 'ok', msg: 'job cancellation requested' })
      loadJob()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to cancel job' })
    }
  }

  async function confirmDelete() {
    if (confirmState?.kind !== 'delete') return
    const jobId = confirmState.id
    confirmState = null
    try {
      await jobsClient.deleteJob({ id: jobId })
      toast.push({ kind: 'ok', msg: 'job deleted' })
      navigate('#/jobs')
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to delete job' })
    }
  }

  function openCancel() {
    if (!job) return
    confirmState = { kind: 'cancel', id: job.id }
  }

  function openDelete() {
    if (!job) return
    confirmState = { kind: 'delete', id: job.id }
  }

  function openRequeue() {
    if (!job) return
    confirmState = { kind: 'requeue', id: job.id }
  }

  onMount(() => {
    mounted = true
    loadJob()

    return () => {
      mounted = false
      stopPoll()
    }
  })
</script>

<div class="job-detail">
  <a href="#/jobs" class="back-link">&larr; Back to Jobs</a>

  {#if loading}
    <div class="detail-layout loading-layout" aria-label="Loading job detail">
      <div class="left-pane">
        <div class="skeleton panel-skeleton"></div>
        <div class="skeleton panel-skeleton tall"></div>
      </div>
      <div class="right-pane">
        <div class="skeleton panel-skeleton wide"></div>
        <div class="skeleton panel-skeleton wide tall"></div>
      </div>
    </div>
  {:else if error}
    <div class="error-banner" role="alert">{error}</div>
    <EmptyState title="Job unavailable" hint="The job could not be loaded from the current backend." />
  {:else if job}
    <div class="header">
      <div class="title-block">
        <h2 use:deltaFlash={job.type}>{job.type || '—'}</h2>
        <div class="header-id">
          <span class="full-id">{job.id}</span>
          <CopyButton text={job.id} />
        </div>
      </div>
      <StatusBadge status={displayStatus} size="md" />
    </div>

    <div class="detail-layout">
      <div class="left-pane">
        <section class="meta panel">
          <div class="section-heading">Metadata</div>
          <div class="meta-item">
            <span class="label">ID</span>
            <span class="value mono">{job.id}</span>
            <CopyButton text={job.id} />
          </div>
          <div class="meta-item">
            <span class="label">Queue</span>
            <span class="value mono">{job.queue || '—'}</span>
          </div>
          <div class="meta-item">
            <span class="label">Tenant</span>
            <span class="value mono">{job.tenant || '—'}</span>
          </div>
          <div class="meta-item">
            <span class="label">Attempts/Max</span>
            <span class="value mono" use:deltaFlash={`${job.attempt}/${job.maxRetries}`}>{job.attempt}/{job.maxRetries}</span>
          </div>
          <div class="meta-item">
            <span class="label">Priority</span>
            <span class="value mono">{job.priority}</span>
          </div>
          <div class="meta-item">
            <span class="label">Worker</span>
            <span class="value mono">{job.worker || '—'}</span>
            {#if job.worker}
              <CopyButton text={job.worker} />
            {/if}
          </div>
        </section>

        <section class="job-metadata panel">
          <div class="section-heading">Job Metadata</div>
          {#if metadataEntries.length > 0}
            <table>
              <tbody>
                {#each metadataEntries as [key, value]}
                  <tr>
                    <th>{key}</th>
                    <td>{value}</td>
                  </tr>
                {/each}
              </tbody>
            </table>
          {:else}
            <span class="muted">—</span>
          {/if}
        </section>

        <section class="timestamps panel">
          <div class="section-heading">Timestamps</div>
          <div class="ts-item">
            <span class="label">Created</span>
            <span class="value"><RelativeTime ts={job.createdAt} mode="both" /></span>
          </div>
          <div class="ts-item">
            <span class="label">Started</span>
            <span class="value"><RelativeTime ts={job.startedAt} mode="both" /></span>
          </div>
          <div class="ts-item">
            <span class="label">Completed</span>
            <span class="value">
              {#if job.completedAt}
                <RelativeTime ts={job.completedAt} mode="both" />
              {:else if job.status === 'running'}
                <span class="running-indicator">running</span>
              {:else}
                <span class="muted">—</span>
              {/if}
            </span>
          </div>
          <div class="ts-item duration-row">
            <span class="label">Wait</span>
            <span class="value">
              {#if job.createdAt && job.startedAt}
                <Duration from={job.createdAt} to={job.startedAt} />
              {:else if job.createdAt && job.status === 'pending'}
                <Duration from={job.createdAt} />
              {:else}
                <span class="muted">—</span>
              {/if}
            </span>
          </div>
          <div class="ts-item duration-row">
            <span class="label">Run</span>
            <span class="value">
              {#if job.startedAt}
                <Duration from={job.startedAt} to={job.completedAt} />
              {:else}
                <span class="muted">—</span>
              {/if}
            </span>
          </div>
        </section>

      </div>

      <div class="right-pane">
        {#if job.lastError}
          <section class="error-box panel">
            <div class="panel-header">
              <h4>Last Error</h4>
              <CopyButton text={job.lastError} />
            </div>
            <pre>{job.lastError}</pre>
          </section>
        {/if}

        {#if job.deadLetteredAt}
          <section class="dead-letter-box panel">
            <div class="panel-header">
              <h4>Dead-letter</h4>
              <div class="panel-header-actions">
                <Button variant="secondary" size="sm" class="btn-requeue" onclick={openRequeue}>Requeue</Button>
                <CopyButton text={job.deadLetterReason || job.lastError} />
              </div>
            </div>
            <div class="dead-letter-meta">
              <span class="label">Dead-lettered</span>
              <span class="value"><RelativeTime ts={job.deadLetteredAt} mode="both" /></span>
            </div>
            <pre>{job.deadLetterReason || job.lastError || '—'}</pre>
          </section>
        {/if}

        <section class="args-box panel">
          <div class="panel-header">
            <h4>Arguments</h4>
            <CopyButton text={job.args} />
          </div>
          <pre>{job.args || '—'}</pre>
        </section>

        {#if checkpoints.length > 0}
          <section class="checkpoints panel">
            <h4>Checkpoints</h4>
            <div class="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Index</th>
                    <th>Type</th>
                    <th>Result</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {#each checkpoints as cp}
                    <tr>
                      <td class="mono">{cp.callIndex}</td>
                      <td class="mono">{cp.callType || '—'}</td>
                      <td>
                        <div class="checkpoint-result">
                          <button
                            type="button"
                            class:error-result={!!cp.error}
                            class="checkpoint-toggle"
                            aria-expanded={expandedCheckpoints.has(cp.id)}
                            onclick={() => toggleCheckpoint(cp.id)}
                          >
                            {cp.error || cp.result || '—'}
                          </button>
                          <CopyButton text={checkpointPayload(cp)} />
                          {#if expandedCheckpoints.has(cp.id)}
                            <pre>{checkpointPayload(cp) || '—'}</pre>
                          {/if}
                        </div>
                      </td>
                      <td><RelativeTime ts={cp.createdAt} mode="both" /></td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          </section>
        {/if}

        <div class="actions">
          {#if job.deadLetteredAt}
            <Button variant="secondary" class="btn-requeue" onclick={openRequeue}>Requeue</Button>
          {:else if job.status === 'failed'}
            <Button variant="secondary" class="btn-retry" onclick={retryJob}>Retry</Button>
          {/if}
          {#if (job.status === 'pending' || job.status === 'running') && !job.deadLetteredAt}
            <Button variant="secondary" class="btn-pause" onclick={pauseJob}>Pause</Button>
          {/if}
          {#if job.status === 'running' && !job.deadLetteredAt}
            <Button variant="destructive" class="btn-cancel" onclick={openCancel}>Cancel</Button>
          {/if}
          {#if job.status === 'paused' && !job.deadLetteredAt}
            <Button variant="secondary" class="btn-resume" onclick={resumeJob}>Resume</Button>
          {/if}
          <Button variant="destructive" class="btn-delete" onclick={openDelete}>Delete</Button>
        </div>
      </div>
    </div>

    {#if workflowRoot}
      <section class="workflow-section panel">
        <WaterfallChart
          root={workflowRoot}
          fanOuts={workflowFanOuts}
          children={workflowChildren}
          onJobClick={(jobId) => navigate(`#/jobs/${jobId}`)}
        />
      </section>
    {/if}
  {/if}

  {#if confirmState?.kind === 'cancel'}
    <ConfirmDialog
      title="Cancel running job"
      body="Cancel this running job? This interrupts the handler cooperatively by cancelling its context; handlers that ignore context are not force-killed."
      blastRadius={`Requests cooperative cancellation for ${confirmState.id}. The handler may continue if it ignores context cancellation.`}
      confirmWord="CANCEL"
      confirmLabel="Cancel job"
      onConfirm={confirmCancel}
      onCancel={() => { confirmState = null }}
    />
  {:else if confirmState?.kind === 'delete'}
    <ConfirmDialog
      title="Delete job"
      body="Delete this job permanently?"
      blastRadius={`Permanently deletes job ${confirmState.id}. This cannot be undone.`}
      confirmWord="DELETE"
      confirmLabel="Delete job"
      onConfirm={confirmDelete}
      onCancel={() => { confirmState = null }}
    />
  {:else if confirmState?.kind === 'requeue'}
    <ConfirmDialog
      title="Requeue dead-lettered job"
      body="Requeue this dead-lettered job? This clears the dead-letter state and queues it again while preserving existing checkpoints. The CLI sdj dlq requeue command replays from scratch; this dashboard action resumes from checkpoints."
      blastRadius={`Requeues ${confirmState.id}. Previously saved checkpoints remain available for replay.`}
      confirmWord="REQUEUE"
      confirmLabel="Requeue"
      onConfirm={confirmRequeue}
      onCancel={() => { confirmState = null }}
    />
  {/if}
</div>

<style>
  .job-detail {
    display: grid;
    gap: var(--sp-4);
    min-width: 0;
  }

  .back-link {
    width: fit-content;
    color: var(--accent);
    font-size: var(--fs-body);
    text-decoration: none;
  }

  .back-link:hover {
    color: var(--fg-primary);
  }

  .header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--sp-4);
    padding-bottom: var(--sp-4);
    border-bottom: var(--border-strong);
  }

  .title-block {
    display: grid;
    gap: var(--sp-2);
    min-width: 0;
  }

  .header h2 {
    margin: 0;
    color: var(--fg-primary);
    font-size: var(--fs-title);
    line-height: var(--lh-dense);
  }

  .header-id,
  .meta-item {
    display: flex;
    align-items: center;
    gap: var(--sp-2);
    min-width: 0;
  }

  .full-id,
  .mono {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .full-id {
    overflow-wrap: anywhere;
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  .detail-layout {
    display: grid;
    grid-template-columns: minmax(280px, 360px) minmax(0, 1fr);
    gap: var(--sp-4);
    align-items: start;
  }

  .left-pane,
  .right-pane {
    display: grid;
    gap: var(--sp-4);
    min-width: 0;
  }

  .left-pane {
    position: sticky;
    top: var(--sp-4);
  }

  .panel {
    padding: var(--sp-4);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .section-heading,
  .panel-header h4,
  .checkpoints h4 {
    margin: 0;
    color: var(--fg-secondary);
    font-size: var(--fs-label);
    font-weight: var(--fw-head);
  }

  .meta,
  .timestamps,
  .job-metadata {
    display: grid;
    gap: var(--sp-3);
  }

  .meta-item,
  .ts-item {
    display: grid;
    grid-template-columns: 104px minmax(0, 1fr) auto;
    gap: var(--sp-2);
    align-items: center;
  }

  .ts-item {
    grid-template-columns: 104px minmax(0, 1fr);
  }

  .label {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
    line-height: var(--lh-dense);
    text-transform: uppercase;
  }

  .value {
    min-width: 0;
    color: var(--fg-primary);
    font-size: var(--fs-body);
    overflow-wrap: anywhere;
  }

  .muted {
    color: var(--fg-secondary);
    font-family: var(--font-mono);
  }

  .running-indicator {
    color: var(--sig-info);
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .duration-row .label {
    color: var(--fg-primary);
  }

  .panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--sp-3);
    margin-bottom: var(--sp-3);
  }

  .panel-header-actions {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
  }

  .job-metadata table {
    width: 100%;
    border-collapse: collapse;
  }

  .job-metadata th,
  .job-metadata td {
    padding: var(--sp-2) 0;
    border-bottom: var(--border);
    text-align: left;
    vertical-align: top;
  }

  .job-metadata th {
    width: 38%;
    padding-right: var(--sp-3);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-weight: var(--fw-body);
    overflow-wrap: anywhere;
  }

  .job-metadata td {
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    overflow-wrap: anywhere;
  }

  .error-box {
    border-left: 4px solid var(--sig-danger);
  }

  .dead-letter-box {
    border: 2px dashed var(--sig-danger);
    background: var(--sig-danger-bg);
  }

  .dead-letter-meta {
    display: grid;
    grid-template-columns: 112px minmax(0, 1fr);
    gap: var(--sp-2);
    align-items: center;
    margin-bottom: var(--sp-3);
  }

  pre {
    margin: 0;
    padding: var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    line-height: var(--lh-prose);
    overflow-wrap: anywhere;
    white-space: pre-wrap;
  }

  .error-box pre {
    color: var(--sig-danger);
  }

  .table-scroll {
    overflow-x: auto;
  }

  .checkpoints {
    display: grid;
    gap: var(--sp-3);
  }

  .checkpoints table {
    width: 100%;
    min-width: 680px;
    border-collapse: collapse;
  }

  .checkpoints th,
  .checkpoints td {
    padding: var(--sp-2) var(--sp-3);
    border-bottom: var(--border);
    text-align: left;
    vertical-align: top;
  }

  .checkpoints th {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
    font-weight: var(--fw-head);
  }

  .checkpoint-result {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: var(--sp-2);
    align-items: start;
  }

  .checkpoint-result pre {
    grid-column: 1 / -1;
  }

  .checkpoint-toggle {
    overflow: hidden;
    padding: 0;
    border: 0;
    background: transparent;
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    text-align: left;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
  }

  .checkpoint-toggle:hover,
  .checkpoint-toggle:focus-visible {
    color: var(--accent);
  }

  .checkpoint-toggle.error-result {
    color: var(--sig-danger);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: var(--sp-2);
    justify-content: flex-end;
  }

  .workflow-section {
    min-width: 0;
    width: 100%;
    margin-top: var(--sp-4);
  }

  .workflow-section :global(.waterfall-chart) {
    border: 0;
    background: transparent;
    box-shadow: none;
  }

  .error-banner {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  .loading-layout {
    min-height: 360px;
  }

  .skeleton {
    border: var(--border);
    border-radius: var(--radius-panel);
    background:
      linear-gradient(90deg, transparent, color-mix(in srgb, var(--fg-secondary) 12%, transparent), transparent),
      var(--bg-raised);
    background-size: 220px 100%, 100% 100%;
    animation: skeletonSweep 1200ms linear infinite;
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .panel-skeleton {
    height: 180px;
  }

  .panel-skeleton.tall {
    height: 260px;
  }

  .panel-skeleton.wide {
    height: 220px;
  }

  :global(.job-detail .flash) {
    animation: cellFlash var(--dur-quick) var(--ease) 1;
  }

  @keyframes skeletonSweep {
    from { background-position: -220px 0, 0 0; }
    to { background-position: calc(100% + 220px) 0, 0 0; }
  }

  @keyframes cellFlash {
    from { background: var(--accent-ring); }
    to { background: transparent; }
  }

  @media (max-width: 720px) {
    .header,
    .actions {
      align-items: flex-start;
      flex-direction: column;
    }

    .detail-layout {
      grid-template-columns: 1fr;
    }

    .left-pane {
      position: static;
    }

    .meta-item,
    .ts-item,
    .dead-letter-meta {
      grid-template-columns: 1fr;
    }
  }
</style>
