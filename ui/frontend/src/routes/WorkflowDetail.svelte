<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
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
        error = null
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
    void loadWorkflow()
    const interval = setInterval(() => void loadWorkflow(), 5000)
    return () => clearInterval(interval)
  })
</script>

<div class="workflow-detail">
  <a href="#/workflows" class="back-link">Back to Workflows</a>

  {#if loading}
    <div class="loading">Loading...</div>
  {:else if error}
    <div class="error" role="alert">{error}</div>
  {:else if root}
    <div class="header">
      <h2>{root.type}</h2>
      <StatusBadge status={root.status} size="md" />
    </div>

    <WaterfallChart {root} {fanOuts} {children} onJobClick={handleJobClick} />
  {/if}
</div>

<style>
  .workflow-detail {
    display: grid;
    gap: var(--sp-4);
    max-width: none;
  }

  .back-link {
    width: fit-content;
    color: var(--accent);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    text-decoration: none;
  }

  .back-link::before {
    content: "< ";
  }

  .back-link:hover,
  .back-link:focus-visible {
    text-decoration: underline;
  }

  .header {
    display: flex;
    align-items: center;
    gap: var(--sp-3);
    flex-wrap: wrap;
  }

  .header h2 {
    margin: 0;
    color: var(--fg-primary);
    font-size: var(--fs-title);
    line-height: var(--lh-dense);
  }

  .loading {
    color: var(--fg-secondary);
  }

  .error {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  @media (max-width: 720px) {
    .workflow-detail {
      gap: var(--sp-3);
    }
  }
</style>
