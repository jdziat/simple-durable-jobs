<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'

  let { navigate, initialStatus = '', initialQueue = '' }: { navigate: (path: string) => void; initialStatus?: string; initialQueue?: string } = $props()

  type JobItem = {
    id: string
    type: string
    queue: string
    status: string
    attempt: number
    maxRetries: number
    createdAt: Date | null
    lastError: string
  }

  let jobs = $state<JobItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let total = $state(0)
  let page = $state(1)
  let statusFilter = $state(initialStatus)
  let queueFilter = $state(initialQueue)
  let typeFilter = $state('')
  let searchQuery = $state('')
  let sortKey = $state<keyof JobItem>('createdAt')
  let sortDir = $state<'asc' | 'desc'>('desc')

  const limit = 20

  let sortedJobs = $derived(
    jobs.slice().sort((a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]
      if (aVal === null || aVal === undefined) return sortDir === 'asc' ? -1 : 1
      if (bVal === null || bVal === undefined) return sortDir === 'asc' ? 1 : -1
      if (aVal instanceof Date && bVal instanceof Date) {
        return sortDir === 'asc' ? aVal.getTime() - bVal.getTime() : bVal.getTime() - aVal.getTime()
      }
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDir === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number)
    })
  )

  function toggleSort(key: keyof JobItem) {
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = key
      sortDir = key === 'createdAt' ? 'desc' : 'asc'
    }
  }

  async function loadJobs() {
    loading = true
    try {
      const response = await jobsClient.listJobs({
        status: statusFilter,
        queue: queueFilter,
        type: typeFilter,
        search: searchQuery,
        page,
        limit,
      })
      jobs = response.jobs.map(j => ({
        id: j.id,
        type: j.type,
        queue: j.queue,
        status: j.status,
        attempt: j.attempt,
        maxRetries: j.maxRetries,
        createdAt: j.createdAt?.toDate() ?? null,
        lastError: j.lastError,
      }))
      total = Number(response.total)
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load jobs'
    } finally {
      loading = false
    }
  }

  async function retryJob(id: string) {
    try {
      await jobsClient.retryJob({ id })
      loadJobs()
    } catch (e) {
      alert('Failed to retry job')
    }
  }

  async function deleteJob(id: string) {
    if (!confirm('Are you sure you want to delete this job?')) return
    try {
      await jobsClient.deleteJob({ id })
      loadJobs()
    } catch (e) {
      alert('Failed to delete job')
    }
  }

  function viewJob(id: string) {
    navigate(`#/jobs/${id}`)
  }

  function applyFilters() {
    page = 1
    loadJobs()
  }

  function clearFilters() {
    statusFilter = ''
    queueFilter = ''
    typeFilter = ''
    searchQuery = ''
    page = 1
    loadJobs()
  }

  onMount(() => {
    loadJobs()
  })
</script>

<div class="jobs-page">
  <h2>Jobs</h2>

  <div class="filters">
    <input
      type="text"
      placeholder="Search by ID..."
      bind:value={searchQuery}
      onkeydown={(e) => e.key === 'Enter' && applyFilters()}
    />
    <select bind:value={statusFilter} onchange={applyFilters}>
      <option value="">All Statuses</option>
      <option value="pending">Pending</option>
      <option value="running">Running</option>
      <option value="completed">Completed</option>
      <option value="failed">Failed</option>
    </select>
    <input
      type="text"
      placeholder="Queue..."
      bind:value={queueFilter}
      onkeydown={(e) => e.key === 'Enter' && applyFilters()}
    />
    <input
      type="text"
      placeholder="Type..."
      bind:value={typeFilter}
      onkeydown={(e) => e.key === 'Enter' && applyFilters()}
    />
    <button class="btn-search" onclick={applyFilters}>Search</button>
    <button class="btn-clear" onclick={clearFilters}>Clear</button>
  </div>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else}
    <table class="jobs-table">
      <thead>
        <tr>
          <th>ID</th>
          <th class="sortable" onclick={() => toggleSort('type')}>
            Type {sortKey === 'type' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('queue')}>
            Queue {sortKey === 'queue' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('status')}>
            Status {sortKey === 'status' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('attempt')}>
            Attempts {sortKey === 'attempt' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th class="sortable" onclick={() => toggleSort('createdAt')}>
            Created {sortKey === 'createdAt' ? (sortDir === 'asc' ? '▲' : '▼') : ''}
          </th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        {#each sortedJobs as job}
          <tr onclick={() => viewJob(job.id)} class="clickable">
            <td class="id">{job.id.slice(0, 8)}...</td>
            <td>{job.type}</td>
            <td>{job.queue}</td>
            <td>
              <span class="status status-{job.status}">{job.status}</span>
            </td>
            <td>{job.attempt}/{job.maxRetries}</td>
            <td>{job.createdAt?.toLocaleString() ?? '-'}</td>
            <td class="actions" onclick={(e) => e.stopPropagation()}>
              {#if job.status === 'failed'}
                <button class="btn-retry" onclick={() => retryJob(job.id)}>Retry</button>
              {/if}
              <button class="btn-delete" onclick={() => deleteJob(job.id)}>Delete</button>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>

    <div class="pagination">
      <span>Showing {Math.min((page - 1) * limit + 1, total)}-{Math.min(page * limit, total)} of {total}</span>
      <div class="pagination-buttons">
        <button disabled={page <= 1} onclick={() => { page--; loadJobs() }}>Previous</button>
        <span class="page-info">Page {page} of {Math.ceil(total / limit) || 1}</span>
        <button disabled={page * limit >= total} onclick={() => { page++; loadJobs() }}>Next</button>
      </div>
    </div>
  {/if}
</div>

<style>
  .jobs-page h2 {
    margin-bottom: 24px;
  }

  .filters {
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
    flex-wrap: wrap;
  }

  .filters input,
  .filters select {
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 6px;
    font-size: 14px;
  }

  .filters input[type="text"] {
    width: 150px;
  }

  .btn-search,
  .btn-clear {
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    font-size: 14px;
    cursor: pointer;
  }

  .btn-search {
    background: #3b82f6;
    color: white;
  }

  .btn-clear {
    background: #6b7280;
    color: white;
  }

  .jobs-table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }

  .jobs-table th,
  .jobs-table td {
    padding: 12px 16px;
    text-align: left;
    border-bottom: 1px solid #eee;
  }

  .jobs-table th {
    background: #f8f9fa;
    font-weight: 600;
  }

  .jobs-table th.sortable {
    cursor: pointer;
    user-select: none;
  }

  .jobs-table th.sortable:hover {
    background: #e9ecef;
  }

  .jobs-table tr.clickable {
    cursor: pointer;
  }

  .jobs-table tr.clickable:hover {
    background: #f5f7fa;
  }

  .id {
    font-family: monospace;
    font-size: 13px;
  }

  .status {
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 500;
  }

  .status-pending { background: #fef3c7; color: #92400e; }
  .status-running { background: #dbeafe; color: #1e40af; }
  .status-completed { background: #d1fae5; color: #065f46; }
  .status-failed { background: #fee2e2; color: #991b1b; }

  .actions {
    display: flex;
    gap: 8px;
  }

  .btn-retry,
  .btn-delete {
    padding: 4px 12px;
    border: none;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
  }

  .btn-retry {
    background: #3b82f6;
    color: white;
  }

  .btn-delete {
    background: #ef4444;
    color: white;
  }

  .pagination {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 16px;
    color: #666;
  }

  .pagination-buttons {
    display: flex;
    gap: 8px;
    align-items: center;
  }

  .page-info {
    padding: 0 12px;
    font-size: 14px;
  }

  .pagination-buttons button {
    padding: 8px 16px;
    border: 1px solid #ddd;
    background: white;
    border-radius: 6px;
    cursor: pointer;
  }

  .pagination-buttons button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .loading { color: #666; }
  .error { color: #ef4444; }
</style>
