<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import WaterfallChart from '../components/WaterfallChart.svelte'
  import type { Job, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

  let { id, navigate }: { id: string; navigate: (path: string) => void } = $props()

  let root = $state<Job | null>(null)
  let fanOuts = $state<FanOut[]>([])
  let children = $state<Job[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)

  async function loadWorkflow() {
    try {
      const response = await jobsClient.getWorkflow({ jobId: id })
      if (response.root) {
        root = response.root
        fanOuts = response.fanOuts
        children = response.children
      } else {
        error = 'Workflow not found'
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to load workflow'
    } finally {
      loading = false
    }
  }

  function handleJobClick(jobId: string) {
    navigate(`#/jobs/${jobId}`)
  }

  onMount(() => {
    loadWorkflow()
  })
</script>

<div class="workflow-detail">
  <a href="#/workflows" class="back-link">&larr; Back to Workflows</a>

  {#if loading}
    <p class="loading">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if root}
    <div class="header">
      <h2>{root.type}</h2>
      <span class="status status-{root.status}">{root.status}</span>
    </div>

    <WaterfallChart {root} {fanOuts} {children} onJobClick={handleJobClick} />
  {/if}
</div>

<style>
  .workflow-detail {
    max-width: 1200px;
  }

  .back-link {
    display: inline-block;
    margin-bottom: 16px;
    color: #3b82f6;
    text-decoration: none;
  }

  .header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 24px;
  }

  .header h2 {
    margin: 0;
  }

  .status {
    padding: 6px 12px;
    border-radius: 4px;
    font-size: 14px;
    font-weight: 500;
  }

  .status-pending  { background: #fef3c7; color: #92400e; }
  .status-running  { background: #dbeafe; color: #1e40af; }
  .status-completed { background: #d1fae5; color: #065f46; }
  .status-failed   { background: #fee2e2; color: #991b1b; }

  .loading { color: #666; }
  .error   { color: #ef4444; }
</style>
