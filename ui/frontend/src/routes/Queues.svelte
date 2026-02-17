<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'

  type QueueItem = {
    name: string
    pending: number
    running: number
    completed: number
    failed: number
    total: number
  }

  let queues = $state<QueueItem[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let sortKey = $state<keyof QueueItem>('name')
  let sortDir = $state<'asc' | 'desc'>('asc')
  let filterName = $state('')

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
      return sortDir === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number)
    })
  )

  function toggleSort(key: keyof QueueItem) {
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc'
    } else {
      sortKey = key
      sortDir = 'asc'
    }
  }

  async function loadQueues() {
    try {
      const response = await jobsClient.listQueues({})
      queues = response.queues.map(q => ({
        name: q.name,
        pending: Number(q.pending),
        running: Number(q.running),
        completed: Number(q.completed),
        failed: Number(q.failed),
        total: Number(q.pending) + Number(q.running) + Number(q.completed) + Number(q.failed),
      }))
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load queues'
    } finally {
      loading = false
    }
  }

  async function purgeQueue(name: string, status: string) {
    if (!confirm(`Purge all ${status} jobs from "${name}"?`)) return
    try {
      const response = await jobsClient.purgeQueue({ name, status })
      alert(`Deleted ${response.deleted} jobs`)
      loadQueues()
    } catch (e) {
      alert('Failed to purge queue')
    }
  }

  onMount(() => {
    loadQueues()
  })
</script>

<div class="queues-page">
  <h2>Queues</h2>

  <div class="filters">
    <input
      type="text"
      placeholder="Filter by name..."
      bind:value={filterName}
    />
  </div>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else}
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
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        {#each sortedQueues as queue}
          <tr>
            <td class="queue-name">{queue.name}</td>
            <td class="num">{queue.pending}</td>
            <td class="num">{queue.running}</td>
            <td class="num">{queue.completed}</td>
            <td class="num">{queue.failed}</td>
            <td class="num total">{queue.total}</td>
            <td class="actions">
              {#if queue.failed > 0}
                <button class="btn-purge" onclick={() => purgeQueue(queue.name, 'failed')}>
                  Purge Failed
                </button>
              {/if}
              {#if queue.completed > 0}
                <button class="btn-purge-secondary" onclick={() => purgeQueue(queue.name, 'completed')}>
                  Purge Completed
                </button>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>

    <div class="summary">
      Showing {sortedQueues.length} of {queues.length} queues
    </div>
  {/if}
</div>

<style>
  .queues-page h2 {
    margin-bottom: 24px;
  }

  .filters {
    margin-bottom: 20px;
  }

  .filters input {
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 6px;
    font-size: 14px;
    width: 250px;
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
    padding: 14px 16px;
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

  .queue-name {
    font-weight: 500;
  }

  .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .total {
    font-weight: 600;
  }

  .actions {
    display: flex;
    gap: 8px;
  }

  .btn-purge,
  .btn-purge-secondary {
    padding: 6px 12px;
    border: none;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
  }

  .btn-purge {
    background: #ef4444;
    color: white;
  }

  .btn-purge-secondary {
    background: #6b7280;
    color: white;
  }

  .summary {
    margin-top: 16px;
    color: #666;
    font-size: 14px;
  }

  .loading { color: #666; }
  .error { color: #ef4444; }
</style>
