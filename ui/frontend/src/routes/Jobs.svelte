<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import { deltaFlash } from '../lib/actions/deltaFlash'
  import AgeHeat from '../lib/components/AgeHeat.svelte'
  import Button from '../lib/components/Button.svelte'
  import ConfirmDialog from '../lib/components/ConfirmDialog.svelte'
  import CopyButton from '../lib/components/CopyButton.svelte'
  import DataTable, { type Column } from '../lib/components/DataTable.svelte'
  import Duration from '../lib/components/Duration.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import RelativeTime from '../lib/components/RelativeTime.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { toast } from '../lib/stores/toast.svelte'

  let { navigate, initialStatus = '', initialQueue = '' }: { navigate: (path: string) => void; initialStatus?: string; initialQueue?: string } = $props()

  type TimeValue = Date | null

  type JobItem = {
    id: string
    type: string
    queue: string
    status: string
    attempt: number
    maxRetries: number
    createdAt: TimeValue
    startedAt: TimeValue
    completedAt: TimeValue
    deadLetteredAt: TimeValue
    lastError: string
  }

  type ConfirmState =
    | { kind: 'cancel'; id: string }
    | { kind: 'delete'; id: string }
    | { kind: 'requeue'; id: string }
    | null

  const limit = 20
  const statusOptions = [
    ['pending', 'Pending'],
    ['running', 'Running'],
    ['completed', 'Completed'],
    ['failed', 'Failed'],
    ['dead-lettered', 'Dead-lettered'],
    ['paused', 'Paused'],
    ['cancelled', 'Cancelled'],
    ['waiting', 'Waiting'],
    ['retrying', 'Retrying'],
  ] as const

  const columns: Column[] = [
    { key: 'id', label: 'ID', width: '150px' },
    { key: 'type', label: 'Type', sortable: true },
    { key: 'queue', label: 'Queue', sortable: true },
    { key: 'status', label: 'Status', sortable: true },
    { key: 'attempt', label: 'Attempts', align: 'right', sortable: true },
    // duration/age are computed (no single DB column), so they are not
    // server-sortable; the Created column gives the same age ordering.
    { key: 'duration', label: 'Duration', align: 'right' },
    { key: 'age', label: 'Age', align: 'right' },
    { key: 'createdAt', label: 'Created', sortable: true },
    { key: 'actions', label: 'Actions' },
  ]

  let jobs = $state<JobItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let total = $state(0)
  let page = $state(1)
  let statusFilter = $state('')
  let queueFilter = $state('')
  let tenantFilter = $state('')
  let typeFilter = $state('')
  let searchQuery = $state('')
  let sortKey = $state('createdAt')
  let sortDir = $state<'asc' | 'desc'>('desc')
  let filterTimer: ReturnType<typeof setTimeout> | null = null
  let transitionTimer: ReturnType<typeof setTimeout> | null = null
  let confirmState = $state<ConfirmState>(null)
  let overflowJobId = $state<string | null>(null)
  let transitionIds = $state<Set<string>>(new Set())
  let previousStatuses = new Map<string, string>()

  // Jobs arrive already sorted by the server (full-dataset sort, not page-local).
  let sortedJobs = $derived(jobs)

  // Maps a sortable table column to its server sort_key. Columns absent here
  // (id, computed duration/age, actions) are not server-sortable.
  const SORT_KEY_MAP: Record<string, string> = {
    type: 'type',
    queue: 'queue',
    status: 'status',
    attempt: 'attempt',
    createdAt: 'created_at',
  }

  let rangeStart = $derived(total === 0 ? 0 : Math.min((page - 1) * limit + 1, total))
  let rangeEnd = $derived(Math.min(page * limit, total))
  let totalPages = $derived(Math.max(1, Math.ceil(total / limit)))
  let activeFilters = $derived([
    ...(statusFilter ? [{ key: 'status', label: `status: ${statusFilter}` }] : []),
    ...(queueFilter ? [{ key: 'queue', label: `queue: ${queueFilter}` }] : []),
    ...(tenantFilter ? [{ key: 'tenant', label: `tenant: ${tenantFilter}` }] : []),
    ...(typeFilter ? [{ key: 'type', label: `type: ${typeFilter}` }] : []),
    ...(searchQuery ? [{ key: 'search', label: `search: ${searchQuery}` }] : []),
  ])

  function toDate(value: { toDate?: () => Date } | undefined): Date | null {
    if (!value?.toDate) return null
    const date = value.toDate()
    return Number.isNaN(date.getTime()) ? null : date
  }

  function displayStatus(job: JobItem): string {
    return job.deadLetteredAt ? 'dead-lettered' : job.status
  }

  function isFailedStatus(job: JobItem): boolean {
    const status = displayStatus(job)
    return status === 'failed' || status === 'dead-lettered'
  }

  function truncate(value: string, max = 88): string {
    if (value.length <= max) return value
    return `${value.slice(0, max - 1)}…`
  }

  function durationMs(job: JobItem): number | null {
    if (!job.startedAt) return null
    return (job.completedAt?.getTime() ?? Date.now()) - job.startedAt.getTime()
  }

  function handleSort(key: string) {
    if (!(key in SORT_KEY_MAP)) return // not a server-sortable column
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = key
      sortDir = key === 'createdAt' ? 'desc' : 'asc'
    }
    // Server-side sort applies across the whole result set, so re-query from
    // page 1 rather than re-ordering the current page in the browser.
    page = 1
    void loadJobs()
  }

  function syncHash() {
    const params = new URLSearchParams()
    if (statusFilter) params.set('status', statusFilter)
    if (queueFilter) params.set('queue', queueFilter)
    if (tenantFilter) params.set('tenant', tenantFilter)
    if (typeFilter) params.set('type', typeFilter)
    if (searchQuery) params.set('search', searchQuery)
    if (page > 1) params.set('page', String(page))
    const query = params.toString()
    window.location.hash = query ? `#/jobs?${query}` : '#/jobs'
  }

  function seedFromHash() {
    const query = window.location.hash.split('?')[1] ?? ''
    const params = new URLSearchParams(query)
    statusFilter = initialStatus || params.get('status') || ''
    queueFilter = initialQueue || params.get('queue') || ''
    tenantFilter = params.get('tenant') || ''
    typeFilter = params.get('type') || ''
    searchQuery = params.get('search') || ''
    page = Math.max(1, Number(params.get('page') || 1) || 1)
  }

  function scheduleApply(resetPage = true) {
    if (filterTimer) clearTimeout(filterTimer)
    filterTimer = setTimeout(() => {
      if (resetPage) page = 1
      syncHash()
      loadJobs()
    }, 250)
  }

  function removeFilter(key: string) {
    if (key === 'status') statusFilter = ''
    if (key === 'queue') queueFilter = ''
    if (key === 'tenant') tenantFilter = ''
    if (key === 'type') typeFilter = ''
    if (key === 'search') searchQuery = ''
    scheduleApply()
  }

  function clearFilters() {
    statusFilter = ''
    queueFilter = ''
    tenantFilter = ''
    typeFilter = ''
    searchQuery = ''
    page = 1
    if (filterTimer) clearTimeout(filterTimer)
    syncHash()
    loadJobs()
  }

  function noteTransitions(nextJobs: JobItem[]) {
    const nextTransitions = new Set<string>()
    for (const job of nextJobs) {
      const status = displayStatus(job)
      const previous = previousStatuses.get(job.id)
      if (previous && previous !== status) nextTransitions.add(job.id)
      previousStatuses.set(job.id, status)
    }
    transitionIds = nextTransitions
    if (nextTransitions.size > 0) {
      if (transitionTimer) clearTimeout(transitionTimer)
      transitionTimer = setTimeout(() => {
        transitionIds = new Set()
        transitionTimer = null
      }, 420)
    }
  }

  async function loadJobs() {
    loading = true
    error = null
    try {
      const response = await jobsClient.listJobs({
        status: statusFilter,
        queue: queueFilter,
        tenant: tenantFilter,
        type: typeFilter,
        search: searchQuery,
        page,
        limit,
        sortKey: SORT_KEY_MAP[sortKey] ?? '',
        sortDir,
      })
      const nextJobs = response.jobs.map(j => ({
        id: j.id,
        type: j.type,
        queue: j.queue,
        status: j.status,
        attempt: j.attempt,
        maxRetries: j.maxRetries,
        createdAt: toDate(j.createdAt),
        startedAt: toDate(j.startedAt),
        completedAt: toDate(j.completedAt),
        deadLetteredAt: toDate(j.deadLetteredAt),
        lastError: j.lastError || '',
      }))
      noteTransitions(nextJobs)
      jobs = nextJobs
      total = Number(response.total)
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load jobs'
      toast.push({ kind: 'err', msg: error })
    } finally {
      loading = false
    }
  }

  async function retryJob(id: string) {
    try {
      await jobsClient.retryJob({ id })
      toast.push({ kind: 'ok', msg: 'job queued for retry' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to retry job' })
    }
  }

  async function confirmRequeue() {
    if (confirmState?.kind !== 'requeue') return
    const id = confirmState.id
    confirmState = null
    try {
      await jobsClient.retryJob({ id })
      toast.push({ kind: 'ok', msg: 'job requeued' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to requeue job' })
    }
  }

  async function pauseJob(id: string) {
    try {
      await jobsClient.pauseJob({ id })
      toast.push({ kind: 'ok', msg: 'job paused' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to pause job' })
    }
  }

  async function resumeJob(id: string) {
    try {
      await jobsClient.resumeJob({ id })
      toast.push({ kind: 'ok', msg: 'job resumed' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to resume job' })
    }
  }

  async function confirmCancel() {
    if (confirmState?.kind !== 'cancel') return
    const id = confirmState.id
    confirmState = null
    try {
      await jobsClient.cancelJob({ id })
      toast.push({ kind: 'ok', msg: 'job cancellation requested' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to cancel job' })
    }
  }

  async function confirmDelete() {
    if (confirmState?.kind !== 'delete') return
    const id = confirmState.id
    confirmState = null
    try {
      await jobsClient.deleteJob({ id })
      toast.push({ kind: 'ok', msg: 'job deleted' })
      loadJobs()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to delete job' })
    }
  }

  function openDelete(event: MouseEvent, id: string) {
    event.stopPropagation()
    overflowJobId = null
    confirmState = { kind: 'delete', id }
  }

  function viewJob(job: JobItem) {
    navigate(`#/jobs/${job.id}`)
  }

  function nextPage() {
    if (page * limit >= total) return
    page += 1
    syncHash()
    loadJobs()
  }

  function previousPage() {
    if (page <= 1) return
    page -= 1
    syncHash()
    loadJobs()
  }

  onMount(() => {
    seedFromHash()
    loadJobs()

    // Clear pending timers on unmount: a leaked debounce firing after route
    // change would syncHash() — a GLOBAL side effect that yanks the user back
    // to /jobs from wherever they navigated — and write torn-down $state.
    return () => {
      if (filterTimer) clearTimeout(filterTimer)
      if (transitionTimer) clearTimeout(transitionTimer)
    }
  })
</script>

<div class="jobs-page">
  <h2>Jobs</h2>

  <div class="filters" aria-label="Job filters">
    <input
      name="search"
      type="search"
      placeholder="Search by ID..."
      bind:value={searchQuery}
      oninput={() => scheduleApply()}
    />
    <select bind:value={statusFilter} onchange={() => scheduleApply()}>
      <option value="">All Statuses</option>
      {#each statusOptions as [value, label]}
        <option {value}>{label}</option>
      {/each}
    </select>
    <input
      type="text"
      placeholder="Queue..."
      bind:value={queueFilter}
      oninput={() => scheduleApply()}
    />
    <input
      type="text"
      placeholder="Tenant..."
      bind:value={tenantFilter}
      oninput={() => scheduleApply()}
    />
    <input
      type="text"
      placeholder="Type..."
      bind:value={typeFilter}
      oninput={() => scheduleApply()}
    />
    <Button variant="ghost" class="btn-clear" onclick={clearFilters}>Clear</Button>
  </div>

  {#if activeFilters.length > 0}
    <div class="chip-bar" aria-label="Active filters">
      {#each activeFilters as filter}
        <button type="button" class="filter-chip" onclick={() => removeFilter(filter.key)} aria-label={`Remove ${filter.label}`}>
          <span>{filter.label}</span>
          <span aria-hidden="true">×</span>
        </button>
      {/each}
    </div>
  {/if}

  <div class="table-meta">
    <span>Sortable columns sort the full result set (Duration and Age are computed and not sortable).</span>
  </div>

  {#if error}
    <div class="error" role="alert">{error}</div>
  {/if}

  {#snippet emptyState()}
    <EmptyState title="No jobs" hint="No jobs match the current filters." />
  {/snippet}

  {#snippet cell(job: JobItem, column: Column)}
    {#if column.key === 'id'}
      <div class="id-cell" role="presentation" onclick={(event) => event.stopPropagation()} onkeydown={(event) => event.stopPropagation()}>
        <span class="id" title={job.id}>{job.id.slice(0, 8)}…</span>
        <CopyButton text={job.id} />
      </div>
    {:else if column.key === 'type'}
      <div class="type-cell">
        <span>{job.type || '—'}</span>
        {#if job.lastError && isFailedStatus(job)}
          <div class="error-peek" role="presentation" onclick={(event) => event.stopPropagation()} onkeydown={(event) => event.stopPropagation()}>
            <button type="button" class="error-peek-toggle" aria-label="Toggle full error" onclick={(event) => event.stopPropagation()}>
              {truncate(job.lastError)}
            </button>
            <div class="error-peek-full">
              <pre>{job.lastError}</pre>
              <CopyButton text={job.lastError} />
            </div>
          </div>
        {/if}
      </div>
    {:else if column.key === 'queue'}
      <span class="mono">{job.queue || '—'}</span>
    {:else if column.key === 'status'}
      <span
        class:status-transition={transitionIds.has(job.id)}
        class="status-cell"
        use:deltaFlash={displayStatus(job)}
      >
        <StatusBadge status={displayStatus(job)} />
      </span>
    {:else if column.key === 'attempt'}
      <span class="mono" use:deltaFlash={`${job.attempt}/${job.maxRetries}`}>{job.attempt}/{job.maxRetries}</span>
    {:else if column.key === 'duration'}
      {#if job.startedAt}
        <Duration from={job.startedAt} to={job.completedAt} />
      {:else}
        <span class="muted">—</span>
      {/if}
    {:else if column.key === 'age'}
      {#if job.createdAt}
        <AgeHeat ts={job.createdAt} />
      {:else}
        <span class="muted">—</span>
      {/if}
    {:else if column.key === 'createdAt'}
      <RelativeTime ts={job.createdAt} mode="both" />
    {:else if column.key === 'actions'}
      <div class="actions" role="presentation" onclick={(event) => event.stopPropagation()} onkeydown={(event) => event.stopPropagation()}>
        {#if job.deadLetteredAt}
          <Button variant="secondary" size="sm" class="btn-requeue" onclick={() => { confirmState = { kind: 'requeue', id: job.id } }}>Requeue</Button>
        {:else if job.status === 'failed'}
          <Button variant="secondary" size="sm" class="btn-retry" onclick={() => retryJob(job.id)}>Retry</Button>
        {/if}
        {#if job.status === 'pending' || job.status === 'running'}
          <Button variant="secondary" size="sm" class="btn-pause" onclick={() => pauseJob(job.id)}>Pause</Button>
        {/if}
        {#if job.status === 'running'}
          <Button variant="destructive" size="sm" class="btn-cancel" onclick={() => { confirmState = { kind: 'cancel', id: job.id } }}>Cancel</Button>
        {/if}
        {#if job.status === 'paused'}
          <Button variant="secondary" size="sm" class="btn-resume" onclick={() => resumeJob(job.id)}>Resume</Button>
        {/if}
        <div
          class="overflow"
          role="presentation"
          onkeydown={(event) => { if (event.key === 'Escape' && overflowJobId === job.id) { overflowJobId = null } }}
          onfocusout={(event) => {
            // Close when focus leaves the overflow cluster entirely.
            const next = event.relatedTarget
            if (overflowJobId === job.id && (!(next instanceof Node) || !event.currentTarget.contains(next))) {
              overflowJobId = null
            }
          }}
        >
          <button
            type="button"
            class="overflow-trigger"
            aria-label="More actions"
            aria-expanded={overflowJobId === job.id}
            onclick={() => { overflowJobId = overflowJobId === job.id ? null : job.id }}
          >•••</button>
          {#if overflowJobId === job.id}
            <div class="overflow-menu">
              <Button variant="destructive" size="sm" class="btn-delete" onclick={(event) => openDelete(event, job.id)}>Delete</Button>
            </div>
          {/if}
        </div>
      </div>
    {/if}
  {/snippet}

  <DataTable
    class="jobs-table"
    {columns}
    rows={sortedJobs}
    {loading}
    rowKey={(job) => job.id}
    onRowClick={viewJob}
    sort={{ key: sortKey, dir: sortDir }}
    onSort={handleSort}
    {emptyState}
    {cell}
  />

  <div class="pagination">
    <span>Showing {rangeStart}-{rangeEnd} of {total}</span>
    <div class="pagination-buttons">
      <Button variant="secondary" disabled={page <= 1} onclick={previousPage}>Previous</Button>
      <span class="page-info">Page {page} of {totalPages}</span>
      <Button variant="secondary" disabled={page * limit >= total} onclick={nextPage}>Next</Button>
    </div>
  </div>

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
  .jobs-page {
    display: grid;
    gap: var(--sp-4);
  }

  h2 {
    font-size: var(--fs-title);
    line-height: var(--lh-dense);
  }

  .filters {
    display: flex;
    gap: var(--sp-2);
    flex-wrap: wrap;
    align-items: center;
  }

  .filters input,
  .filters select {
    min-height: 32px;
    padding: 0 var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-size: var(--fs-body);
  }

  .filters input {
    width: 168px;
  }

  .chip-bar {
    display: flex;
    flex-wrap: wrap;
    gap: var(--sp-2);
  }

  .filter-chip {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
    min-height: 26px;
    padding: 0 var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-chip);
    background: var(--bg-raised);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    cursor: pointer;
  }

  .table-meta {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  .error {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  .id-cell,
  .actions {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
  }

  .id,
  .mono {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .id {
    font-size: var(--fs-label);
  }

  .muted {
    color: var(--fg-secondary);
    font-family: var(--font-mono);
  }

  .type-cell {
    display: grid;
    gap: var(--sp-1);
    min-width: 180px;
  }

  .error-peek {
    max-width: 420px;
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
  }

  .error-peek-toggle {
    display: block;
    max-width: 100%;
    overflow: hidden;
    padding: 0;
    border: 0;
    background: transparent;
    color: inherit;
    font: inherit;
    text-align: left;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
  }

  .error-peek-full {
    display: none;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: var(--sp-2);
    align-items: start;
    margin-top: var(--sp-2);
    padding: var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
  }

  .error-peek:hover .error-peek-full,
  .error-peek:focus-within .error-peek-full {
    display: grid;
  }

  .error-peek pre {
    margin: 0;
    overflow: auto;
    white-space: pre-wrap;
  }

  .status-cell {
    display: inline-block;
  }

  :global(.jobs-table .flash) {
    animation: cellFlash var(--dur-quick) var(--ease) 1;
  }

  :global(.jobs-table tr:has(.status-transition)) {
    animation: rowSweep var(--dur-sweep) var(--ease) 1;
  }

  .overflow {
    position: relative;
  }

  .overflow-trigger {
    min-width: 30px;
    min-height: 26px;
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    cursor: pointer;
  }

  .overflow-menu {
    position: absolute;
    right: 0;
    z-index: 5;
    display: grid;
    gap: var(--sp-2);
    min-width: 120px;
    padding: var(--sp-2);
    border: var(--border-strong);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    box-shadow: var(--shadow-overlay);
  }

  .pagination {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: var(--sp-3);
    color: var(--fg-secondary);
  }

  .pagination-buttons {
    display: flex;
    gap: var(--sp-2);
    align-items: center;
  }

  .page-info {
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
  }

  @keyframes cellFlash {
    from { background: var(--accent-ring); }
    to { background: transparent; }
  }

  @keyframes rowSweep {
    from { outline: 2px solid var(--accent); outline-offset: -2px; }
    to { outline: 2px solid transparent; outline-offset: -2px; }
  }

  @media (max-width: 767px) {
    .filters input {
      width: 100%;
    }

    .pagination {
      align-items: flex-start;
      flex-direction: column;
    }
  }
</style>
