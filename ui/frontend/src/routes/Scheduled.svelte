<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'

  type ScheduledJob = {
    name: string
    schedule: string
    queue: string
    nextRun: Date | null
    lastRun: Date | null
  }

  let scheduledJobs = $state<ScheduledJob[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let sortKey = $state<keyof ScheduledJob>('name')
  let sortDir = $state<'asc' | 'desc'>('asc')
  let filterName = $state('')
  let filterQueue = $state('')

  let filteredJobs = $derived(
    scheduledJobs.filter(j => {
      if (filterName && !j.name.toLowerCase().includes(filterName.toLowerCase())) return false
      if (filterQueue && !j.queue.toLowerCase().includes(filterQueue.toLowerCase())) return false
      return true
    })
  )

  let sortedJobs = $derived(
    filteredJobs.slice().sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]
      if (aVal === null || aVal === undefined) return sortDir === 'asc' ? 1 : -1
      if (bVal === null || bVal === undefined) return sortDir === 'asc' ? -1 : 1
      if (aVal instanceof Date && bVal instanceof Date) {
        return sortDir === 'asc' ? aVal.getTime() - bVal.getTime() : bVal.getTime() - aVal.getTime()
      }
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return 0
    })
  )

  function toggleSort(key: keyof ScheduledJob) {
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = key
      sortDir = key === 'nextRun' || key === 'lastRun' ? 'asc' : 'asc'
    }
  }

  async function loadScheduledJobs() {
    try {
      const response = await jobsClient.listScheduledJobs({})
      scheduledJobs = response.jobs.map(j => ({
        name: j.name,
        schedule: j.schedule,
        queue: j.queue,
        nextRun: j.nextRun?.toDate() ?? null,
        lastRun: j.lastRun?.toDate() ?? null,
      }))
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load scheduled jobs'
    } finally {
      loading = false
    }
  }

  onMount(() => {
    loadScheduledJobs()
  })
</script>

<div class="scheduled-page">
  <h2>Scheduled Jobs</h2>

  <div class="filters">
    <input
      type="text"
      placeholder="Filter by name..."
      bind:value={filterName}
    />
    <input
      type="text"
      placeholder="Filter by queue..."
      bind:value={filterQueue}
    />
  </div>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if scheduledJobs.length === 0}
    <div class="empty-state">
      <p>No scheduled jobs registered.</p>
      <p class="hint">Use <code>queue.Schedule()</code> to register recurring jobs.</p>
    </div>
  {:else}
    <table class="scheduled-table">
      <thead>
        <tr>
          <th class="sortable" onclick={() => toggleSort('name')}>
            Name {sortKey === 'name' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('schedule')}>
            Schedule {sortKey === 'schedule' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('queue')}>
            Queue {sortKey === 'queue' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('nextRun')}>
            Next Run {sortKey === 'nextRun' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('lastRun')}>
            Last Run {sortKey === 'lastRun' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
        </tr>
      </thead>
      <tbody>
        {#each sortedJobs as job}
          <tr>
            <td class="job-name">{job.name}</td>
            <td class="schedule">{job.schedule}</td>
            <td>{job.queue}</td>
            <td>{job.nextRun?.toLocaleString() ?? '-'}</td>
            <td>{job.lastRun?.toLocaleString() ?? '-'}</td>
          </tr>
        {/each}
      </tbody>
    </table>

    <div class="summary">
      Showing {sortedJobs.length} of {scheduledJobs.length} scheduled jobs
    </div>
  {/if}
</div>

<style>
  .scheduled-page h2 {
    margin-bottom: 24px;
  }

  .filters {
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
  }

  .filters input {
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 6px;
    font-size: 14px;
    width: 200px;
  }

  .scheduled-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  .scheduled-table th,
  .scheduled-table td {
    padding: 14px 16px;
    text-align: left;
    border-bottom: 1px solid #eee;
  }

  .scheduled-table th {
    background: #f8f9fa;
    font-weight: 600;
  }

  .scheduled-table th.sortable {
    cursor: pointer;
    user-select: none;
  }

  .scheduled-table th.sortable:hover {
    background: #e9ecef;
  }

  .job-name {
    font-weight: 500;
  }

  .schedule {
    font-family: monospace;
    font-size: 13px;
    color: #666;
  }

  .empty-state {
    background: white;
    padding: 40px;
    border-radius: 8px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  .empty-state p {
    margin: 8px 0;
    color: #666;
  }

  .empty-state .hint {
    font-size: 14px;
  }

  .empty-state code {
    background: #f5f7fa;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 13px;
  }

  .summary {
    margin-top: 16px;
    color: #666;
    font-size: 14px;
  }

  .loading { color: #666; }
  .error { color: #ef4444; }
</style>
