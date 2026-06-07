<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import { deltaFlash } from '../lib/actions/deltaFlash'
  import Button from '../lib/components/Button.svelte'
  import ConfirmDialog from '../lib/components/ConfirmDialog.svelte'
  import DataTable, { type Column } from '../lib/components/DataTable.svelte'
  import EmptyState from '../lib/components/EmptyState.svelte'
  import Sparkline from '../lib/components/Sparkline.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { statusToken } from '../lib/status'
  import { toast } from '../lib/stores/toast.svelte'

  type QueueCountKey = 'pending' | 'running' | 'completed' | 'failed' | 'paused' | 'total'
  type SortableQueueKey = 'name' | QueueCountKey
  type PurgeStatus = 'failed' | 'completed'

  type QueueItem = {
    name: string
    pending: number
    running: number
    completed: number
    failed: number
    paused: number
    total: number
    isPaused: boolean
    composition: number[]
  }

  type ConfirmState = {
    queue: QueueItem
    status: PurgeStatus
    count: number
  } | null

  const columns: Column[] = [
    { key: 'name', label: 'Queue', sortable: true },
    { key: 'pending', label: 'Pending', align: 'right', sortable: true },
    { key: 'running', label: 'Running', align: 'right', sortable: true },
    { key: 'completed', label: 'Completed', align: 'right', sortable: true },
    { key: 'failed', label: 'Failed', align: 'right', sortable: true },
    { key: 'paused', label: 'Paused', align: 'right', sortable: true },
    { key: 'total', label: 'Total', align: 'right', sortable: true },
    { key: 'backlogAge', label: 'Backlog Age', align: 'right' },
    { key: 'composition', label: 'Composition' },
    { key: 'actions', label: 'Actions' },
  ]

  let queues = $state<QueueItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let sortKey = $state<SortableQueueKey>('name')
  let sortDir = $state<'asc' | 'desc'>('asc')
  let filterName = $state('')
  let confirmState = $state<ConfirmState>(null)
  let pollTimer: ReturnType<typeof setInterval> | null = null

  let filteredQueues = $derived(
    queues.filter(q => filterName === '' || q.name.toLowerCase().includes(filterName.toLowerCase()))
  )

  let sortedQueues = $derived(
    filteredQueues.slice().sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDir === 'asc' ? Number(aVal) - Number(bVal) : Number(bVal) - Number(aVal)
    })
  )

  let totals = $derived(
    queues.reduce(
      (acc, queue) => ({
        pending: acc.pending + queue.pending,
        running: acc.running + queue.running,
        completed: acc.completed + queue.completed,
        failed: acc.failed + queue.failed,
        paused: acc.paused + queue.paused,
        total: acc.total + queue.total,
      }),
      { pending: 0, running: 0, completed: 0, failed: 0, paused: 0, total: 0 }
    )
  )

  function numberValue(value: unknown): number {
    return Number(value ?? 0)
  }

  function handleSort(key: string) {
    if (!['name', 'pending', 'running', 'completed', 'failed', 'paused', 'total'].includes(key)) return
    const nextKey = key as SortableQueueKey
    if (sortKey === nextKey) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = nextKey
      sortDir = nextKey === 'name' ? 'asc' : 'desc'
    }
  }

  async function loadQueues(showLoading = false) {
    if (showLoading) loading = true
    error = null
    try {
      const response = await jobsClient.listQueues({})
      queues = response.queues.map(q => {
        const pending = numberValue(q.pending)
        const running = numberValue(q.running)
        const completed = numberValue(q.completed)
        const failed = numberValue(q.failed)
        const paused = numberValue(q.paused)
        return {
          name: q.name,
          pending,
          running,
          completed,
          failed,
          paused,
          total: pending + running + completed + failed + paused,
          isPaused: q.isPaused,
          // Honest micro-chart: QueueStats exposes only a point-in-time status
          // distribution, not throughput history. This snapshot is labeled as
          // composition so it cannot be mistaken for fabricated time-series data.
          composition: [pending, running, completed, failed, paused],
        }
      })
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load queues'
      toast.push({ kind: 'err', msg: error })
    } finally {
      loading = false
    }
  }

  async function pauseQueue(name: string) {
    try {
      await jobsClient.pauseQueue({ name })
      toast.push({ kind: 'ok', msg: `queue "${name}" paused` })
      await loadQueues()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to pause queue' })
    }
  }

  async function resumeQueue(name: string) {
    try {
      await jobsClient.resumeQueue({ name })
      toast.push({ kind: 'ok', msg: `queue "${name}" resumed` })
      await loadQueues()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to resume queue' })
    }
  }

  function openPurge(event: MouseEvent, queue: QueueItem, status: PurgeStatus) {
    event.stopPropagation()
    confirmState = {
      queue,
      status,
      count: status === 'failed' ? queue.failed : queue.completed,
    }
  }

  async function confirmPurge() {
    if (!confirmState) return
    const { queue, status } = confirmState
    confirmState = null
    try {
      const response = await jobsClient.purgeQueue({ name: queue.name, status })
      const deleted = numberValue(response.deleted)
      toast.push({ kind: 'ok', msg: `deleted ${deleted} ${status} jobs` })
      await loadQueues()
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to purge queue' })
    }
  }

  function drillIntoQueue(queue: QueueItem) {
    window.location.hash = `#/jobs?queue=${encodeURIComponent(queue.name)}`
  }

  function countStyle(key: QueueCountKey, value: number): string {
    if (value === 0 || key === 'total' || key === 'completed') return ''
    return `color:${statusToken(key).color};`
  }

  onMount(() => {
    loadQueues(true)
    pollTimer = setInterval(() => {
      loadQueues()
    }, 5000)
  })

  onDestroy(() => {
    if (pollTimer) clearInterval(pollTimer)
  })
</script>

<div class="queues-page">
  <h2>Queues</h2>

  <div class="filters" aria-label="Queue filters">
    <input
      name="filter"
      type="search"
      placeholder="Filter by name..."
      bind:value={filterName}
    />
  </div>

  {#if error}
    <div class="error" role="alert">{error}</div>
  {/if}

  {#snippet emptyState()}
    <EmptyState title="No queues" hint="No queues match the current filter." />
  {/snippet}

  {#snippet cell(queue: QueueItem, column: Column)}
    {#if column.key === 'name'}
      <div class="queue-name">
        <span class="mono">{queue.name}</span>
        {#if queue.isPaused}
          <StatusBadge status="paused" class="badge-paused" />
        {/if}
      </div>
    {:else if column.key === 'pending' || column.key === 'running' || column.key === 'completed' || column.key === 'failed' || column.key === 'paused' || column.key === 'total'}
      {@const value = queue[column.key]}
      <span
        class:total={column.key === 'total'}
        class="num"
        style={countStyle(column.key, value)}
        use:deltaFlash={value}
      >{value}</span>
    {:else if column.key === 'backlogAge'}
      <span class="data-gap" title="QueueStats does not expose oldest queued job age.">—</span>
    {:else if column.key === 'composition'}
      <div class="spark-cell" title="Snapshot of pending, running, completed, failed, paused counts.">
        <Sparkline data={queue.composition} color={queue.failed > 0 ? 'var(--sig-danger)' : 'var(--fg-secondary)'} label={`Status composition for ${queue.name}`} />
      </div>
    {:else if column.key === 'actions'}
      <div class="actions" role="presentation" onclick={(event) => event.stopPropagation()} onkeydown={(event) => event.stopPropagation()}>
        {#if queue.isPaused}
          <Button variant="secondary" size="sm" class="btn-resume-queue" onclick={() => resumeQueue(queue.name)}>Resume Queue</Button>
        {:else}
          <Button variant="secondary" size="sm" class="btn-pause-queue" onclick={() => pauseQueue(queue.name)}>Pause Queue</Button>
        {/if}
        {#if queue.failed > 0}
          <Button variant="destructive" size="sm" class="btn-purge" onclick={(event) => openPurge(event, queue, 'failed')}>Purge Failed</Button>
        {/if}
        {#if queue.completed > 0}
          <Button variant="destructive" size="sm" class="btn-purge-secondary" onclick={(event) => openPurge(event, queue, 'completed')}>Purge Completed</Button>
        {/if}
      </div>
    {/if}
  {/snippet}

  <DataTable
    class="queues-table"
    {columns}
    rows={sortedQueues}
    {loading}
    rowKey={(queue) => queue.name}
    onRowClick={drillIntoQueue}
    sort={{ key: sortKey, dir: sortDir }}
    onSort={handleSort}
    {emptyState}
    {cell}
  />

  {#if !loading && queues.length > 0}
    <!-- Fleet-wide sums (all queues), deliberately NOT the filtered subset:
         labeled so a filtered view can't read as a mismatch. -->
    <div class="totals-footer" aria-label="All-queue totals">
      <span class="totals-label">All queues</span>
      <span><span class="num" use:deltaFlash={totals.pending}>{totals.pending}</span> pending</span>
      <span><span class="num" use:deltaFlash={totals.running}>{totals.running}</span> running</span>
      <span><span class="num" use:deltaFlash={totals.completed}>{totals.completed}</span> completed</span>
      <span><span class="num" style={countStyle('failed', totals.failed)} use:deltaFlash={totals.failed}>{totals.failed}</span> failed</span>
      <span><span class="num" style={countStyle('paused', totals.paused)} use:deltaFlash={totals.paused}>{totals.paused}</span> paused</span>
      <span><span class="num total" use:deltaFlash={totals.total}>{totals.total}</span> total</span>
    </div>
  {/if}

  <div class="summary">
    Showing {sortedQueues.length} of {queues.length} queues
  </div>

  {#if confirmState}
    <ConfirmDialog
      title={`Purge ${confirmState.status} jobs`}
      body={`Purge ${confirmState.status} jobs from "${confirmState.queue.name}"?`}
      blastRadius={`This permanently deletes ${confirmState.count} ${confirmState.status} jobs from "${confirmState.queue.name}". This cannot be undone.`}
      confirmWord={confirmState.queue.name}
      confirmLabel={`Purge ${confirmState.status}`}
      onConfirm={confirmPurge}
      onCancel={() => { confirmState = null }}
    />
  {/if}
</div>

<style>
  .queues-page {
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
    width: 220px;
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

  .queue-name,
  .actions {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
  }

  .actions {
    flex-wrap: wrap;
  }

  .mono,
  .num,
  .data-gap {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .num {
    display: inline-block;
    min-width: 3ch;
    color: var(--fg-primary);
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .total {
    font-weight: var(--fw-head);
  }

  .data-gap {
    color: var(--fg-secondary);
  }

  .spark-cell {
    display: inline-block;
  }

  :global(.queues-table .flash),
  .totals-footer :global(.flash) {
    animation: cellFlash var(--dur-quick) var(--ease) 1;
  }

  .totals-footer {
    display: flex;
    gap: var(--sp-4);
    flex-wrap: wrap;
    align-items: center;
    padding: var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: var(--fg-secondary);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
    font-size: var(--fs-label);
  }

  .totals-label {
    color: var(--fg-primary);
    font-weight: var(--fw-head);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .summary {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  @keyframes cellFlash {
    from { background: var(--accent-ring); }
    to { background: transparent; }
  }

  @media (max-width: 767px) {
    .filters input {
      width: 100%;
    }
  }
</style>
