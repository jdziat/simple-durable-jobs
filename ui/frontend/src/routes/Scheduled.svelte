<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import DataTable, { type Column } from '../lib/components/DataTable.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import RelativeTime from '../lib/components/RelativeTime.svelte'
  import { cronToProse } from '../lib/cron'
  import { absolute, countdown } from '../lib/time'
  import { toast } from '../lib/stores/toast.svelte'

  type TimeValue = Date | null
  type SortableScheduleKey = 'name' | 'schedule' | 'queue' | 'nextRun' | 'lastRun'
  type ProtoTimestamp = { toDate?: () => Date }

  type ScheduledJob = {
    name: string
    schedule: string
    scheduleProse: string
    queue: string
    nextRun: TimeValue
    lastRun: TimeValue
    overdue: boolean
    missedFires: number
    expectedLastRun: TimeValue
  }

  const columns: Column[] = [
    { key: 'name', label: 'Name', sortable: true },
    { key: 'schedule', label: 'Cron', sortable: true },
    { key: 'queue', label: 'Queue', sortable: true },
    { key: 'nextRun', label: 'Next Run', sortable: true },
    { key: 'lastRun', label: 'Last Run', sortable: true },
    { key: 'health', label: 'Health' },
  ]

  let scheduledJobs = $state<ScheduledJob[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let sortKey = $state<SortableScheduleKey>('nextRun')
  let sortDir = $state<'asc' | 'desc'>('asc')
  let filterName = $state('')
  let filterQueue = $state('')
  let now = $state(Date.now())
  let tickTimer: ReturnType<typeof setInterval> | null = null
  let pollTimer: ReturnType<typeof setInterval> | null = null

  let filteredJobs = $derived(
    scheduledJobs.filter(job => {
      if (filterName && !job.name.toLowerCase().includes(filterName.toLowerCase())) return false
      if (filterQueue && !job.queue.toLowerCase().includes(filterQueue.toLowerCase())) return false
      return true
    })
  )

  let sortedJobs = $derived(
    filteredJobs.slice().sort((a, b) => compareValue(sortValue(a, sortKey), sortValue(b, sortKey)))
  )

  function toDate(value: ProtoTimestamp | undefined): Date | null {
    if (!value?.toDate) return null
    const date = value.toDate()
    return Number.isNaN(date.getTime()) ? null : date
  }

  function sortValue(job: ScheduledJob, key: SortableScheduleKey): string | number | null {
    if (key === 'nextRun' || key === 'lastRun') return job[key]?.getTime() ?? null
    return job[key]
  }

  function compareValue(aVal: string | number | null, bVal: string | number | null): number {
    if (aVal === null) return sortDir === 'asc' ? 1 : -1
    if (bVal === null) return sortDir === 'asc' ? -1 : 1
    if (typeof aVal === 'string' && typeof bVal === 'string') {
      return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
    }
    return sortDir === 'asc' ? Number(aVal) - Number(bVal) : Number(bVal) - Number(aVal)
  }

  function handleSort(key: string) {
    if (!['name', 'schedule', 'queue', 'nextRun', 'lastRun'].includes(key)) return
    const nextKey = key as SortableScheduleKey
    if (sortKey === nextKey) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = nextKey
      sortDir = 'asc'
    }
  }

  async function loadScheduledJobs(showLoading = false) {
    if (showLoading) loading = true
    error = null
    try {
      const response = await jobsClient.listScheduledJobs({})
      scheduledJobs = response.jobs.map(job => ({
        name: job.name,
        schedule: job.schedule,
        scheduleProse: cronToProse(job.schedule),
        queue: job.queue,
        nextRun: toDate(job.nextRun),
        lastRun: toDate(job.lastRun),
        overdue: job.overdue,
        missedFires: Number(job.missedFires),
        expectedLastRun: toDate(job.expectedLastRun),
      }))
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load scheduled jobs'
      toast.push({ kind: 'err', msg: error })
    } finally {
      loading = false
    }
  }

  function nextRunLabel(ts: TimeValue): string {
    now
    if (!ts) return '—'
    return `${countdown(ts)} · ${absolute(ts)}`
  }

  function missedFireLabel(count: number): string {
    return `${count} missed`
  }

  function healthTitle(job: ScheduledJob): string | undefined {
    if (!job.overdue) return undefined
    const expected = job.expectedLastRun ? `Expected ${absolute(job.expectedLastRun)} · ` : ''
    return `${expected}${missedFireLabel(job.missedFires)} — worker may be down or the schedule paused`
  }

  onMount(() => {
    loadScheduledJobs(true)
    tickTimer = setInterval(() => {
      now = Date.now()
    }, 1000)
    pollTimer = setInterval(() => {
      loadScheduledJobs()
    }, 5000)
  })

  onDestroy(() => {
    if (tickTimer) clearInterval(tickTimer)
    if (pollTimer) clearInterval(pollTimer)
  })
</script>

<div class="scheduled-page">
  <h2>Scheduled</h2>

  <div class="filters" aria-label="Scheduled job filters">
    <input
      name="name"
      type="search"
      placeholder="Filter by name..."
      bind:value={filterName}
    />
    <input
      name="queue"
      type="search"
      placeholder="Filter by queue..."
      bind:value={filterQueue}
    />
  </div>

  {#if error}
    <div class="error" role="alert">{error}</div>
  {/if}

  {#snippet emptyState()}
    <EmptyState
      title={scheduledJobs.length === 0 ? 'No scheduled jobs' : 'No scheduled jobs match'}
      hint={scheduledJobs.length === 0 ? 'Register recurring work with the queue scheduling API.' : 'Adjust the name or queue filters.'}
      code={scheduledJobs.length === 0 ? 'queue.Schedule()' : undefined}
    />
  {/snippet}

  {#snippet cell(job: ScheduledJob, column: Column)}
    {#if column.key === 'name'}
      <span class="job-name">{job.name}</span>
    {:else if column.key === 'schedule'}
      <div class="cron-cell">
        <code>{job.schedule}</code>
        <span>{job.scheduleProse}</span>
      </div>
    {:else if column.key === 'queue'}
      <span class="mono">{job.queue || '—'}</span>
    {:else if column.key === 'nextRun'}
      <span class="time-cell">{nextRunLabel(job.nextRun)}</span>
    {:else if column.key === 'lastRun'}
      {#if job.lastRun}
        <RelativeTime ts={job.lastRun} mode="both" live />
      {:else}
        <span class="muted">—</span>
      {/if}
    {:else if column.key === 'health'}
      {#if job.overdue}
        <span class="health-badge health-overdue" title={healthTitle(job)}>
          {job.missedFires > 0 ? `Overdue · ${missedFireLabel(job.missedFires)}` : 'Overdue'}
        </span>
      {:else}
        <span class="health-badge health-ok">On track</span>
      {/if}
    {/if}
  {/snippet}

  <DataTable
    class="scheduled-table"
    {columns}
    rows={sortedJobs}
    {loading}
    rowKey={(job) => job.name}
    sort={{ key: sortKey, dir: sortDir }}
    onSort={handleSort}
    {emptyState}
    {cell}
  />

  {#if !loading && scheduledJobs.length > 0}
    <div class="summary">
      Showing {sortedJobs.length} of {scheduledJobs.length} scheduled jobs
    </div>
  {/if}
</div>

<style>
  .scheduled-page {
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

  .filters input {
    width: 200px;
    min-height: 32px;
    padding: 0 var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-size: var(--fs-body);
  }

  .error {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  .job-name {
    font-weight: var(--fw-head);
  }

  .cron-cell {
    display: grid;
    gap: var(--sp-1);
  }

  .cron-cell code,
  .mono,
  .time-cell,
  .muted,
  .health-badge {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .cron-cell code {
    width: max-content;
    padding: 1px var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-size: var(--fs-label);
  }

  .cron-cell span,
  .muted,
  .summary {
    color: var(--fg-secondary);
  }

  .time-cell {
    white-space: nowrap;
  }

  .health-badge {
    display: inline-flex;
    align-items: center;
    min-height: 20px;
    padding: 0 var(--sp-2);
    border-radius: var(--radius-chip);
    font-size: var(--fs-label);
    line-height: 1;
    white-space: nowrap;
  }

  .health-ok {
    color: var(--fg-secondary);
  }

  .health-overdue {
    border: 1px solid color-mix(in srgb, var(--sig-warn) 55%, transparent);
    background: var(--sig-warn-bg);
    color: var(--sig-warn);
  }

  .summary {
    font-size: var(--fs-label);
  }

  @media (max-width: 767px) {
    .filters input {
      width: 100%;
    }
  }
</style>
