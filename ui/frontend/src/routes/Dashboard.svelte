<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import StatsCard from '../lib/components/StatsCard.svelte'
  import StatsChart from '../lib/components/StatsChart.svelte'

  type QueueStat = { name: string; pending: number; running: number; completed: number; failed: number; total: number }

  let stats = $state<{
    totalPending: number
    totalRunning: number
    totalCompleted: number
    totalFailed: number
    queues: QueueStat[]
  } | null>(null)

  let chartCompleted = $state<{ timestamp: Date; value: number }[]>([])
  let chartFailed = $state<{ timestamp: Date; value: number }[]>([])
  let chartPeriod = $state('1h')
  let chartQueue = $state('')

  let error = $state<string | null>(null)
  let loading = $state(true)
  let sortKey = $state<keyof QueueStat>('name')
  let sortDir = $state<'asc' | 'desc'>('asc')

  let sortedQueues = $derived(
    stats?.queues.slice().sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDir === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number)
    }) ?? []
  )

  function toggleSort(key: keyof QueueStat) {
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = key
      sortDir = 'asc'
    }
  }

  async function loadStats() {
    try {
      const response = await jobsClient.getStats({})
      stats = {
        totalPending: Number(response.totalPending),
        totalRunning: Number(response.totalRunning),
        totalCompleted: Number(response.totalCompleted),
        totalFailed: Number(response.totalFailed),
        queues: response.queues.map(q => ({
          name: q.name,
          pending: Number(q.pending),
          running: Number(q.running),
          completed: Number(q.completed),
          failed: Number(q.failed),
          total: Number(q.pending) + Number(q.running) + Number(q.completed) + Number(q.failed),
        })),
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load stats'
    } finally {
      loading = false
    }
  }

  async function loadStatsHistory() {
    try {
      const response = await jobsClient.getStatsHistory({ period: chartPeriod, queue: chartQueue })
      chartCompleted = (response.completed ?? []).map(d => ({
        timestamp: d.timestamp?.toDate() ?? new Date(),
        value: Number(d.value),
      }))
      chartFailed = (response.failed ?? []).map(d => ({
        timestamp: d.timestamp?.toDate() ?? new Date(),
        value: Number(d.value),
      }))
    } catch {
      // Stats history not available yet - that's okay
    }
  }

  function switchPeriod(p: string) {
    chartPeriod = p
    loadStatsHistory()
  }

  function switchQueue(q: string) {
    chartQueue = q
    loadStatsHistory()
  }

  onMount(() => {
    loadStats()
    loadStatsHistory()
    const interval = setInterval(() => {
      loadStats()
      loadStatsHistory()
    }, 5000)
    return () => clearInterval(interval)
  })
</script>

<div class="dashboard">
  <h2>Dashboard</h2>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if stats}
    <div class="stats-grid">
      <StatsCard title="Pending" value={stats.totalPending} color="#f59e0b" href="#/jobs?status=pending" />
      <StatsCard title="Running" value={stats.totalRunning} color="#3b82f6" href="#/jobs?status=running" />
      <StatsCard title="Completed" value={stats.totalCompleted} color="#10b981" href="#/jobs?status=completed" />
      <StatsCard title="Failed" value={stats.totalFailed} color="#ef4444" href="#/jobs?status=failed" />
    </div>

    <div class="chart-section">
      <div class="chart-header">
        <h3>Throughput {chartQueue ? `— ${chartQueue}` : '— All Queues'}</h3>
        <div class="chart-controls">
          <select class="queue-selector" value={chartQueue} onchange={(e) => switchQueue(e.currentTarget.value)}>
            <option value="">All Queues</option>
            {#each stats.queues as q}
              <option value={q.name}>{q.name}</option>
            {/each}
          </select>
          <div class="period-selector">
            <button class:active={chartPeriod === '1h'} onclick={() => switchPeriod('1h')}>1h</button>
            <button class:active={chartPeriod === '24h'} onclick={() => switchPeriod('24h')}>24h</button>
            <button class:active={chartPeriod === '7d'} onclick={() => switchPeriod('7d')}>7d</button>
          </div>
        </div>
      </div>
      <StatsChart completed={chartCompleted} failed={chartFailed} period={chartPeriod} />
    </div>

    <h3>Queues</h3>
    <table class="queues-table">
      <thead>
        <tr>
          <th class="sortable" onclick={() => toggleSort('name')}>
            Queue {sortKey === 'name' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('pending')}>
            Pending {sortKey === 'pending' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('running')}>
            Running {sortKey === 'running' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('completed')}>
            Completed {sortKey === 'completed' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('failed')}>
            Failed {sortKey === 'failed' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('total')}>
            Total {sortKey === 'total' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
        </tr>
      </thead>
      <tbody>
        {#each sortedQueues as queue}
          <tr class="clickable" onclick={() => window.location.hash = `#/jobs?queue=${encodeURIComponent(queue.name)}`}>
            <td>{queue.name}</td>
            <td class="num">{queue.pending}</td>
            <td class="num">{queue.running}</td>
            <td class="num">{queue.completed}</td>
            <td class="num">{queue.failed}</td>
            <td class="num total">{queue.total}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</div>

<style>
  .dashboard h2 {
    margin-bottom: 24px;
  }

  .dashboard h3 {
    margin: 32px 0 16px;
  }

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
  }

  .chart-section {
    margin-top: 32px;
  }

  .chart-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }

  .chart-header h3 {
    margin: 0;
  }

  .chart-controls {
    display: flex;
    gap: 12px;
    align-items: center;
  }

  .queue-selector {
    padding: 6px 12px;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    color: #334155;
    background: white;
    cursor: pointer;
  }

  .queue-selector:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
  }

  .period-selector {
    display: flex;
    gap: 4px;
    background: #f1f5f9;
    border-radius: 6px;
    padding: 2px;
  }

  .period-selector button {
    padding: 6px 14px;
    border: none;
    border-radius: 4px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    background: transparent;
    color: #64748b;
    transition: all 0.15s;
  }

  .period-selector button.active {
    background: white;
    color: #0f172a;
    box-shadow: 0 1px 2px rgba(0,0,0,0.1);
  }

  .period-selector button:hover:not(.active) {
    color: #334155;
  }

  .queues-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  .queues-table th,
  .queues-table td {
    padding: 12px 16px;
    text-align: left;
    border-bottom: 1px solid #eee;
  }

  .queues-table th {
    background: #f8f9fa;
    font-weight: 600;
  }

  .queues-table th.sortable {
    cursor: pointer;
    user-select: none;
  }

  .queues-table th.sortable:hover {
    background: #e9ecef;
  }

  .queues-table tr.clickable {
    cursor: pointer;
    transition: background 0.15s;
  }

  .queues-table tr.clickable:hover {
    background: #f5f7fa;
  }

  .queues-table td.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .queues-table td.total {
    font-weight: 600;
  }

  .loading {
    color: #666;
  }

  .error {
    color: #ef4444;
  }
</style>
