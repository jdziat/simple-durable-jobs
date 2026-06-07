<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import Button from '../lib/components/Button.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import RelativeTime from '../lib/components/RelativeTime.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { statusToken } from '../lib/status'

  type WorkflowItem = {
    rootJobId: string
    rootJobType: string
    rootJobStatus: string
    totalJobs: number
    completedJobs: number
    failedJobs: number
    runningJobs: number
    segmentRunningJobs: number
    pendingJobs: number
    strategy: string
    createdAt: Date | null
  }

  type SortKey = 'age' | 'failures' | 'progress'

  const limit = 20
  const statusOptions = [
    ['running', 'Running'],
    ['pending', 'Pending'],
    ['completed', 'Completed'],
    ['failed', 'Failed'],
    ['cancelled', 'Cancelled'],
    ['paused', 'Paused'],
    ['waiting', 'Waiting'],
  ] as const

  let workflows = $state<WorkflowItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let total = $state(0)
  let page = $state(1)
  let searchQuery = $state('')
  let statusFilter = $state('')
  let sortKey = $state<SortKey>('age')
  let filterTimer: ReturnType<typeof setTimeout> | null = null

  let totalPages = $derived(Math.max(1, Math.ceil(total / limit)))
  let rangeStart = $derived(total === 0 ? 0 : Math.min((page - 1) * limit + 1, total))
  let rangeEnd = $derived(Math.min(page * limit, total))
  let filteredWorkflows = $derived.by(() => {
    const query = searchQuery.trim().toLowerCase()
    const rows = query
      ? workflows.filter(wf =>
          wf.rootJobId.toLowerCase().includes(query) ||
          wf.rootJobType.toLowerCase().includes(query) ||
          wf.strategy.toLowerCase().includes(query)
        )
      : workflows

    return rows.slice().sort((a, b) => {
      if (sortKey === 'failures') return b.failedJobs - a.failedJobs || compareAge(a, b)
      if (sortKey === 'progress') return progressPct(b) - progressPct(a) || compareAge(a, b)
      return compareAge(a, b)
    })
  })

  function toDate(value: { toDate?: () => Date } | undefined): Date | null {
    if (!value?.toDate) return null
    const date = value.toDate()
    return Number.isNaN(date.getTime()) ? null : date
  }

  function compareAge(a: WorkflowItem, b: WorkflowItem): number {
    return (b.createdAt?.getTime() ?? 0) - (a.createdAt?.getTime() ?? 0)
  }

  function syncHash() {
    const params = new URLSearchParams()
    if (statusFilter) params.set('status', statusFilter)
    if (searchQuery) params.set('search', searchQuery)
    if (sortKey !== 'age') params.set('sort', sortKey)
    if (page > 1) params.set('page', String(page))
    window.location.hash = params.toString() ? `#/workflows?${params.toString()}` : '#/workflows'
  }

  function seedFromHash() {
    const query = window.location.hash.split('?')[1] ?? ''
    const params = new URLSearchParams(query)
    statusFilter = params.get('status') || ''
    searchQuery = params.get('search') || ''
    const nextSort = params.get('sort')
    sortKey = nextSort === 'failures' || nextSort === 'progress' ? nextSort : 'age'
    page = Math.max(1, Number(params.get('page') || 1) || 1)
  }

  function scheduleApply(resetPage = true) {
    if (filterTimer) clearTimeout(filterTimer)
    filterTimer = setTimeout(() => {
      if (resetPage) page = 1
      syncHash()
      void loadWorkflows()
    }, 250)
  }

  function clearFilters() {
    if (filterTimer) clearTimeout(filterTimer)
    searchQuery = ''
    statusFilter = ''
    sortKey = 'age'
    page = 1
    syncHash()
    void loadWorkflows()
  }

  async function loadWorkflows() {
    try {
      loading = true
      error = null
      const response = await jobsClient.listWorkflows({ page, limit, status: statusFilter })
      workflows = response.workflows.map(wf => {
        const totalJobs = Math.max(0, wf.totalJobs)
        // Clamp every segment against a remaining-track budget so the four
        // widths can never sum past 100%, even if a buggy/racing backend
        // rollup reports completed+failed > total (invariant violation).
        let budget = totalJobs
        const completedJobs = Math.min(Math.max(0, wf.completedJobs), budget)
        budget -= completedJobs
        const failedJobs = Math.min(Math.max(0, wf.failedJobs), budget)
        budget -= failedJobs
        const segmentRunningJobs = Math.min(Math.max(0, wf.runningJobs), budget)
        const runningJobs = Math.max(0, wf.runningJobs)
        const pendingJobs = Math.max(0, totalJobs - completedJobs - failedJobs - segmentRunningJobs)
        return {
          rootJobId: wf.rootJob?.id ?? '',
          rootJobType: wf.rootJob?.type ?? '',
          rootJobStatus: wf.rootJob?.status ?? '',
          totalJobs,
          completedJobs,
          failedJobs,
          runningJobs,
          segmentRunningJobs,
          pendingJobs,
          strategy: wf.strategy,
          createdAt: toDate(wf.rootJob?.createdAt),
        }
      })
      total = Number(response.total)
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load workflows'
    } finally {
      loading = false
    }
  }

  function progressPct(wf: WorkflowItem): number {
    if (wf.totalJobs === 0) return 0
    // Clamp: aria-valuenow must never exceed aria-valuemax (100).
    return Math.min(100, Math.round((wf.completedJobs / wf.totalJobs) * 100))
  }

  function segmentPct(count: number, totalJobs: number): number {
    if (totalJobs <= 0) return 0
    return Math.max(0, Math.min(100, (count / totalJobs) * 100))
  }

  function segmentStyle(status: string, count: number, totalJobs: number): string {
    return `width:${segmentPct(count, totalJobs)}%;background:${statusToken(status).color};`
  }

  function nextPage() {
    if (page >= totalPages) return
    page += 1
    syncHash()
    void loadWorkflows()
  }

  function previousPage() {
    if (page <= 1) return
    page -= 1
    syncHash()
    void loadWorkflows()
  }

  onMount(() => {
    seedFromHash()
    void loadWorkflows()
    const interval = setInterval(() => void loadWorkflows(), 5000)
    return () => {
      clearInterval(interval)
      if (filterTimer) clearTimeout(filterTimer)
    }
  })
</script>

<div class="workflows-page">
  <h2>Workflows</h2>

  <div class="filters" aria-label="Workflow filters">
    <input
      name="search"
      type="search"
      placeholder="Search workflows..."
      bind:value={searchQuery}
      oninput={() => scheduleApply()}
    />
    <select bind:value={statusFilter} onchange={() => scheduleApply()}>
      <option value="">All Statuses</option>
      {#each statusOptions as [value, label]}
        <option {value}>{label}</option>
      {/each}
    </select>
    <select bind:value={sortKey} onchange={() => scheduleApply(false)} aria-label="Sort workflows">
      <option value="age">Newest first</option>
      <option value="failures">Most failures</option>
      <option value="progress">Most progress</option>
    </select>
    <Button variant="ghost" class="btn-clear" onclick={clearFilters}>Clear</Button>
  </div>

  {#if error}
    <div class="error" role="alert">{error}</div>
  {/if}

  {#if loading}
    <div class="workflow-list" aria-label="Loading workflows">
      {#each Array.from({ length: 3 }) as _}
        <div class="workflow-card skeleton-card"></div>
      {/each}
    </div>
  {:else if filteredWorkflows.length === 0}
    <EmptyState title="No workflows" hint="No workflows match the current filters." />
  {:else}
    <div class="workflow-list">
      {#each filteredWorkflows as wf}
        <a
          href="#/workflows/{wf.rootJobId}"
          class="workflow-card"
          class:has-failures={wf.failedJobs > 0}
          aria-label="View workflow {wf.rootJobType}"
        >
          <div class="card-header">
            <div class="card-title">
              <span class="job-type">{wf.rootJobType || 'unknown'}</span>
              {#if wf.strategy}
                <span class="strategy-badge">{wf.strategy}</span>
              {/if}
            </div>
            <StatusBadge status={wf.rootJobStatus} />
          </div>

          <div class="progress-section">
            <div
              class="progress-bar"
              role="progressbar"
              aria-valuenow={progressPct(wf)}
              aria-valuemin={0}
              aria-valuemax={100}
              aria-label={`${wf.completedJobs} of ${wf.totalJobs} workflow jobs completed`}
            >
              <div class="bar bar-completed" style={segmentStyle('completed', wf.completedJobs, wf.totalJobs)}></div>
              <div class="bar bar-failed" style={segmentStyle('failed', wf.failedJobs, wf.totalJobs)}></div>
              <div class="bar bar-running" style={segmentStyle('running', wf.segmentRunningJobs, wf.totalJobs)}></div>
              <div class="bar bar-pending" style={segmentStyle('pending', wf.pendingJobs, wf.totalJobs)}></div>
            </div>
            <div class="progress-text">
              <span>{wf.completedJobs}/{wf.totalJobs} completed</span>
              {#if wf.failedJobs > 0}
                <span class="failed-count">{wf.failedJobs} failed</span>
              {/if}
            </div>
          </div>

          <div class="card-footer">
            <span class="created-time"><RelativeTime ts={wf.createdAt} mode="rel" /></span>
          </div>
        </a>
      {/each}
    </div>
  {/if}

  <div class="pagination">
    <span>Showing {rangeStart}-{rangeEnd} of {total}</span>
    <div class="pagination-buttons">
      <Button variant="secondary" disabled={page <= 1} onclick={previousPage}>Previous</Button>
      <span class="page-info">Page {page} of {totalPages}</span>
      <Button variant="secondary" disabled={page >= totalPages} onclick={nextPage}>Next</Button>
    </div>
  </div>
</div>

<style>
  .workflows-page {
    display: grid;
    gap: var(--sp-4);
  }

  h2 {
    font-size: var(--fs-title);
    line-height: var(--lh-dense);
  }

  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: var(--sp-2);
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
    width: 220px;
  }

  .workflow-list {
    display: grid;
    gap: var(--sp-3);
  }

  .workflow-card {
    display: block;
    padding: var(--sp-4);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: inherit;
    text-decoration: none;
    box-shadow: inset 0 1px 0 var(--inset-sheen);
    transition: border-color var(--dur-instant) var(--ease), background var(--dur-instant) var(--ease);
  }

  .workflow-card:hover,
  .workflow-card:focus-visible {
    border-color: var(--hairline-strong);
    background: color-mix(in srgb, var(--bg-raised) 86%, var(--bg-sunken));
  }

  .workflow-card.has-failures {
    border-color: color-mix(in srgb, var(--sig-danger) 45%, var(--hairline));
  }

  .card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--sp-3);
    margin-bottom: var(--sp-3);
  }

  .card-title {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--sp-2);
    min-width: 0;
  }

  .job-type {
    color: var(--fg-primary);
    font-size: var(--fs-emph);
    font-weight: var(--fw-head);
  }

  .strategy-badge {
    display: inline-flex;
    align-items: center;
    min-height: 20px;
    padding: 0 var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-chip);
    background: var(--bg-sunken);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    text-transform: lowercase;
  }

  .progress-section {
    display: grid;
    gap: var(--sp-2);
    margin-bottom: var(--sp-3);
  }

  .progress-bar {
    display: flex;
    width: 100%;
    height: 8px;
    overflow: hidden;
    border: var(--border);
    border-radius: var(--radius-chip);
    background: var(--bg-sunken);
  }

  .bar {
    height: 100%;
    min-width: 0;
    transition: width var(--dur-settle) var(--ease);
  }

  .progress-text {
    display: flex;
    flex-wrap: wrap;
    gap: var(--sp-3);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
  }

  .failed-count {
    color: var(--sig-danger);
    font-weight: var(--fw-head);
  }

  .card-footer {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  .created-time {
    color: var(--fg-secondary);
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

  .skeleton-card {
    min-height: 116px;
    background: linear-gradient(90deg, var(--bg-raised), var(--bg-sunken), var(--bg-raised));
    animation: shimmer var(--dur-sweep) var(--ease) infinite alternate;
  }

  .error {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  @keyframes shimmer {
    from { opacity: 0.45; }
    to { opacity: 1; }
  }

  @media (max-width: 767px) {
    .filters input,
    .filters select {
      width: 100%;
    }

    .card-header,
    .pagination {
      align-items: flex-start;
      flex-direction: column;
    }
  }
</style>
