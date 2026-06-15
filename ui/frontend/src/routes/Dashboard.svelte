<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import { deltaFlash } from '../lib/actions/deltaFlash'
  import { mergeHistory, type HistorySeries, type Period } from '../lib/dashboardHistory'
  import AgeHeat from '../lib/components/AgeHeat.svelte'
  import AreaChart from '../lib/components/AreaChart.svelte'
  import MetricCard from '../lib/components/MetricCard.svelte'
  import SegmentedControl from '../lib/components/SegmentedControl.svelte'
  import Sparkline from '../lib/components/Sparkline.svelte'
  import { error as statsError, start, stats, stop, type QueueStat } from '../lib/stores/stats.svelte'

  type ProtoTimestamp = { toDate?: () => Date }

  const periods: Period[] = ['1h', '24h', '7d', '30d']

  let chartPeriod = $state<Period>('1h')
  let chartQueue = $state('')
  let history = $state<HistorySeries>({ completed: [], failed: [], labels: [] })
  let queueSparklines = $state<Record<string, number[]>>({})
  let queueOldestPendingAt = $state<Record<string, Date>>({})
  let historyError = $state<string | null>(null)
  let failedGlow = $state(false)
  let queueFailureGlow = $state<Set<string>>(new Set())

  let previousFailed: number | null = null
  let previousQueueFailed = new Map<string, number>()
  let glowTimer: ReturnType<typeof setTimeout> | null = null

  let snapshot = $derived(stats.value)
  let topQueues = $derived((snapshot?.queues ?? []).slice().sort(compareQueues).slice(0, 5))
  let terminalTotal = $derived((snapshot?.totalCompleted ?? 0) + (snapshot?.totalFailed ?? 0) + (snapshot?.totalCancelled ?? 0))
  let failureRate = $derived(terminalTotal > 0 && snapshot ? (snapshot.totalFailed / terminalTotal) * 100 : 0)
  let totalBacklog = $derived(snapshot ? snapshot.totalPending + snapshot.totalRunning + snapshot.totalPaused + snapshot.totalRetrying + snapshot.totalWaiting : 0)
  let throughputPerMin = $derived(terminalThroughputPerMinute(history, chartPeriod))
  let oldestPendingAt = $derived(oldestDate(Object.values(queueOldestPendingAt)))

  $effect(() => {
    if (!snapshot) return

    const nextQueueGlow = new Set<string>()
    if (previousFailed !== null && snapshot.totalFailed > previousFailed) {
      failedGlow = true
    }
    previousFailed = snapshot.totalFailed

    for (const queue of snapshot.queues) {
      const previous = previousQueueFailed.get(queue.name)
      if (previous !== undefined && queue.failed > previous) {
        nextQueueGlow.add(queue.name)
      }
      previousQueueFailed.set(queue.name, queue.failed)
    }

    if (nextQueueGlow.size > 0) {
      queueFailureGlow = nextQueueGlow
      failedGlow = true
    }

    if (failedGlow || nextQueueGlow.size > 0) {
      if (glowTimer) clearTimeout(glowTimer)
      glowTimer = setTimeout(() => {
        failedGlow = false
        queueFailureGlow = new Set()
        glowTimer = null
      }, 420)
    }
  })

  function compareQueues(a: QueueStat, b: QueueStat): number {
    const aBacklog = a.pending + a.running + a.paused + a.retrying + a.waiting
    const bBacklog = b.pending + b.running + b.paused + b.retrying + b.waiting
    if (bBacklog !== aBacklog) return bBacklog - aBacklog
    if (b.failed !== a.failed) return b.failed - a.failed
    return a.name.localeCompare(b.name)
  }

  function terminalThroughputPerMinute(series: HistorySeries, period: Period): number {
    const total = series.completed.reduce((sum, value) => sum + value, 0) + series.failed.reduce((sum, value) => sum + value, 0)
    const minutes = period === '1h' ? 60 : period === '24h' ? 1440 : period === '7d' ? 10080 : 43200
    return total / minutes
  }

  function formatNumber(value: number): string {
    return value.toLocaleString()
  }

  function formatRate(value: number): string {
    return `${value.toFixed(value >= 10 ? 1 : 2)}%`
  }

  function formatThroughput(value: number): string {
    return value >= 10 ? value.toFixed(1) : value.toFixed(2)
  }

  function toDate(value: ProtoTimestamp | undefined): Date | null {
    if (!value?.toDate) return null
    const date = value.toDate()
    return Number.isNaN(date.getTime()) ? null : date
  }

  function oldestDate(values: Date[]): Date | null {
    if (values.length === 0) return null
    return values.reduce((oldest, value) => value.getTime() < oldest.getTime() ? value : oldest)
  }

  async function loadQueueOldestPendingAt() {
    try {
      const response = await jobsClient.listQueues({})
      queueOldestPendingAt = Object.fromEntries(response.queues.map(queue => {
        const oldest = toDate(queue.oldestPendingAt)
        return oldest ? [queue.name, oldest] : null
      }).filter((entry): entry is [string, Date] => entry !== null))
    } catch {
      queueOldestPendingAt = {}
    }
  }

  async function loadStatsHistory() {
    try {
      const [main, ...queueHistories] = await Promise.all([
        jobsClient.getStatsHistory({ period: chartPeriod, queue: chartQueue }),
        ...topQueues.map(queue => jobsClient.getStatsHistory({ period: chartPeriod, queue: queue.name })),
      ])

      history = mergeHistory(main.completed, main.failed)

      queueSparklines = Object.fromEntries(queueHistories.map((queueHistory, index) => {
        const merged = mergeHistory(queueHistory.completed, queueHistory.failed)
        return [
          topQueues[index]?.name ?? '',
          merged.completed.map((value, pointIndex) => value + merged.failed[pointIndex]),
        ]
      }).filter(([name]) => name !== ''))
      historyError = null
    } catch (e) {
      historyError = e instanceof Error ? e.message : 'Failed to load stats history'
    }
  }

  function switchPeriod(period: string) {
    chartPeriod = period as Period
    void loadStatsHistory()
  }

  function switchQueue(queue: string) {
    chartQueue = queue
    void loadStatsHistory()
  }

  function openQueue(queue: string) {
    window.location.hash = `#/jobs?queue=${encodeURIComponent(queue)}`
  }

  onMount(() => {
    start()
    void loadStatsHistory()
    void loadQueueOldestPendingAt()
    const interval = setInterval(() => {
      void loadStatsHistory()
      void loadQueueOldestPendingAt()
    }, 5000)

    return () => {
      clearInterval(interval)
      if (glowTimer) clearTimeout(glowTimer)
      stop()
    }
  })
</script>

<div class="dashboard">
  <h2>Dashboard</h2>

  {#if statsError.value}
    <div class="error-banner" role="alert">{statsError.value}</div>
  {/if}

  {#if !snapshot}
    <div class="stats-grid skeleton-grid" aria-label="Loading metrics">
      {#each Array.from({ length: 6 }) as _}
        <div class="card metric-skeleton"></div>
      {/each}
    </div>

    <section class="chart-section">
      <div class="section-header">
        <div>
          <h3>Throughput</h3>
          <p>Loading history</p>
        </div>
      </div>
      <div class="chart-skeleton"></div>
    </section>
  {:else}
    <div class="stats-grid">
      <div class:fail-glow={failedGlow}>
        <!-- dominant is the only danger gate here; the href carries filtering. -->
        <MetricCard label="Failed" value={formatNumber(snapshot.totalFailed)} href="#/jobs?status=failed" dominant={snapshot.totalFailed > 0} />
      </div>
      <MetricCard label="Pending" value={formatNumber(snapshot.totalPending)} status="pending" href="#/jobs?status=pending" />
      <MetricCard label="Running" value={formatNumber(snapshot.totalRunning)} status="running" href="#/jobs?status=running" />
      <MetricCard label="Completed" value={formatNumber(snapshot.totalCompleted)} status="completed" href="#/jobs?status=completed" />
      <MetricCard label="Retrying" value={formatNumber(snapshot.totalRetrying)} status="retrying" href="#/jobs?status=retrying" />
      <MetricCard label="Waiting" value={formatNumber(snapshot.totalWaiting)} status="waiting" href="#/jobs?status=waiting" />
      <MetricCard label="Paused" value={formatNumber(snapshot.totalPaused)} status="paused" href="#/jobs?status=paused" />
      <MetricCard label="Cancelled" value={formatNumber(snapshot.totalCancelled)} status="cancelled" href="#/jobs?status=cancelled" />
      <!-- Distinct workers currently holding running-job locks; idle workers are not counted. -->
      <MetricCard label="Active workers" value={formatNumber(snapshot.activeWorkers)} />
    </div>

    <div class="ops-row" aria-label="Operational numbers">
      <div class="ops-cell">
        <span>Throughput/min</span>
        <strong use:deltaFlash={throughputPerMin}>{formatThroughput(throughputPerMin)}</strong>
      </div>
      <div class="ops-cell">
        <span>Failure rate</span>
        <strong class:danger={snapshot.totalFailed > 0} use:deltaFlash={failureRate}>{formatRate(failureRate)}</strong>
      </div>
      <div class="ops-cell">
        <span>Total backlog</span>
        <strong use:deltaFlash={totalBacklog}>{formatNumber(totalBacklog)}</strong>
      </div>
      <div class="ops-cell">
        <span>Oldest pending age</span>
        <strong use:deltaFlash={oldestPendingAt?.getTime() ?? 0}>
          {#if oldestPendingAt}
            <AgeHeat ts={oldestPendingAt} />
          {:else}
            —
          {/if}
        </strong>
      </div>
    </div>

    <section class="chart-section">
      <div class="section-header chart-header">
        <div>
          <h3>Throughput {chartQueue ? `- ${chartQueue}` : '- all queues'}</h3>
          <p>Completed and failed terminal jobs over {chartPeriod}</p>
        </div>
        <div class="chart-controls">
          <select class="queue-selector" value={chartQueue} onchange={(event) => switchQueue(event.currentTarget.value)} aria-label="Filter throughput by queue">
            <option value="">All queues</option>
            {#each snapshot.queues as queue}
              <option value={queue.name}>{queue.name}</option>
            {/each}
          </select>
          <SegmentedControl options={periods} value={chartPeriod} onChange={switchPeriod} />
        </div>
      </div>
      {#if historyError}
        <div class="error-banner" role="alert">{historyError}</div>
      {/if}
      <AreaChart completed={history.completed} failed={history.failed} labels={history.labels} period={chartPeriod} />
    </section>

    <section class="top-queues">
      <div class="section-header">
        <div>
          <h3>Top queues</h3>
          <p>Highest live backlog first</p>
        </div>
        <a class="view-all" href="#/queues">view all -></a>
      </div>

      <table class="queues-table">
        <thead>
          <tr>
            <th>Queue</th>
            <th class="right">Backlog</th>
            <th class="right">Failed</th>
            <th>Throughput</th>
          </tr>
        </thead>
        <tbody>
          {#each topQueues as queue}
            <tr class="clickable" onclick={() => openQueue(queue.name)}>
              <td class="queue-name">{queue.name}</td>
              <td class="num" use:deltaFlash={queue.pending + queue.running + queue.paused + queue.retrying + queue.waiting}>{formatNumber(queue.pending + queue.running + queue.paused + queue.retrying + queue.waiting)}</td>
              <td
                class="num failed-cell"
                class:has-failures={queue.failed > 0}
                class:fail-glow={queueFailureGlow.has(queue.name)}
                use:deltaFlash={queue.failed}
              >{formatNumber(queue.failed)}</td>
              <td class="spark-cell" use:deltaFlash={queueSparklines[queue.name]?.join(',') ?? ''}>
                <Sparkline data={queueSparklines[queue.name] ?? []} color="var(--fg-secondary)" label={`${queue.name} throughput`} />
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </section>
  {/if}
</div>

<style>
  .dashboard {
    display: grid;
    gap: var(--sp-6);
  }

  .dashboard h2 {
    font-size: var(--fs-title);
    font-weight: var(--fw-head);
  }

  .dashboard h3 {
    color: var(--fg-primary);
    font-size: var(--fs-section);
    font-weight: var(--fw-head);
  }

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: var(--sp-3);
  }

  .stats-grid > :first-child {
    min-width: 0;
  }

  .ops-row {
    display: grid;
    /* 4 tracks for 4 cells — a 3-track grid orphaned the 4th cell on its
       own row (gate-caught); the 1180px breakpoint folds to a 2x2. */
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: var(--sp-3);
  }

  .ops-cell {
    display: grid;
    gap: var(--sp-2);
    min-height: 72px;
    padding: var(--sp-4);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .ops-cell span,
  .section-header p {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  .ops-cell span {
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .ops-cell strong,
  .num,
  .queue-name,
  .view-all,
  .queue-selector {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .ops-cell strong {
    align-self: end;
    color: var(--fg-primary);
    font-size: var(--fs-title);
    line-height: 1;
  }

  .ops-cell strong :global(.age-heat) {
    font-size: inherit;
  }

  /* Specificity must beat the base `.ops-cell strong` and `.queues-table td`
     color rules, or the danger tint silently never renders. */
  .ops-cell strong.danger,
  .queues-table td.has-failures {
    color: var(--sig-danger);
  }

  .chart-section,
  .top-queues {
    display: grid;
    gap: var(--sp-4);
  }

  .section-header {
    display: flex;
    align-items: end;
    justify-content: space-between;
    gap: var(--sp-4);
  }

  .section-header > div {
    display: grid;
    gap: var(--sp-1);
  }

  .chart-controls {
    display: flex;
    align-items: center;
    gap: var(--sp-3);
  }

  .queue-selector {
    min-height: 32px;
    padding: 0 var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-primary);
    font-size: var(--fs-label);
    cursor: pointer;
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .queue-selector:focus-visible {
    border-color: var(--accent);
  }

  .queues-table {
    width: 100%;
    border-collapse: collapse;
    border: var(--border);
    border-radius: var(--radius-panel);
    overflow: hidden;
    background: var(--bg-raised);
    color: var(--fg-primary);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .queues-table th,
  .queues-table td {
    height: var(--row-h);
    padding: 0 var(--sp-3);
    border-bottom: var(--border);
    text-align: left;
    vertical-align: middle;
  }

  .queues-table th {
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
    font-weight: var(--fw-label);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .queues-table tbody tr:last-child td {
    border-bottom: 0;
  }

  .queues-table tr.clickable {
    cursor: pointer;
  }

  .queues-table tr.clickable:hover,
  .queues-table tr.clickable:focus-visible {
    background: var(--bg-sunken);
  }

  .queues-table td {
    color: var(--fg-primary);
    font-size: var(--fs-body);
  }

  .right,
  .num {
    text-align: right;
  }

  .spark-cell {
    width: 132px;
    color: var(--fg-secondary);
  }

  .view-all {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
    text-decoration: none;
  }

  .view-all:hover {
    color: var(--fg-primary);
  }

  .error-banner {
    padding: var(--sp-3) var(--sp-4);
    border: var(--border);
    border-color: var(--sig-danger);
    border-radius: var(--radius-panel);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
  }

  .metric-skeleton,
  .chart-skeleton {
    border: var(--border);
    border-radius: var(--radius-panel);
    background: linear-gradient(90deg, var(--bg-raised), var(--bg-sunken), var(--bg-raised));
    box-shadow: inset 0 1px 0 var(--inset-sheen);
    animation: shimmer var(--dur-sweep) var(--ease) infinite alternate;
  }

  .metric-skeleton {
    min-height: 96px;
  }

  .chart-skeleton {
    min-height: 260px;
  }

  .fail-glow {
    animation: failGlow var(--dur-sweep) var(--ease) 1;
    outline: 1px solid var(--sig-danger-glow);
    background: var(--sig-danger-bg);
  }

  :global(.flash) {
    background: var(--sig-info-bg);
    color: var(--accent);
  }

  @keyframes failGlow {
    from {
      outline-color: var(--sig-danger-glow);
      background: var(--sig-danger-bg);
    }
    to {
      outline-color: transparent;
      background: transparent;
    }
  }

  @keyframes shimmer {
    from { opacity: 0.45; }
    to { opacity: 1; }
  }

  @media (max-width: 1180px) {
    .ops-row {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }

  @media (max-width: 760px) {
    .stats-grid,
    .ops-row {
      grid-template-columns: 1fr;
    }

    .section-header,
    .chart-controls {
      align-items: stretch;
      flex-direction: column;
    }

    .queues-table {
      min-width: 520px;
    }

    .top-queues {
      overflow-x: auto;
    }
  }
</style>
