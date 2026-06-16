<script lang="ts">
  import { onMount } from 'svelte'
  import { jobsClient } from '../lib/client'
  import { toast } from '../lib/stores/toast.svelte'
  import Button from '../lib/components/Button.svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import WaterfallChart from '../components/WaterfallChart.svelte'
  import type { Job, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

  let { id, navigate }: { id: string; navigate: (path: string) => void } = $props()

  let root = $state<Job | null>(null)
  let fanOuts = $state<FanOut[]>([])
  let children = $state<Job[]>([])
  let loading = $state(true)
  let error = $state<string | null>(null)
  let confirming = $state(false)
  let deleting = $state(false)

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

  async function deleteWorkflow() {
    // Delete the resolved true root (GetWorkflow walked up to it), not the route
    // id — deleting a mid-tree node would strand the real root + its fan-out
    // accounting. The button only renders inside {#if root}, so root is set.
    const rootId = root?.id ?? id
    deleting = true
    try {
      await jobsClient.deleteJob({ id: rootId, deleteSubtree: true })
      toast.push({ kind: 'ok', msg: 'workflow deleted' })
      navigate('#/workflows')
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'Failed to delete workflow' })
      deleting = false
      confirming = false
    }
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
      <div class="workflow-actions">
        {#if confirming}
          <span class="confirm-prompt">Delete this workflow and its {children.length} child job{children.length === 1 ? '' : 's'}?</span>
          <Button variant="destructive" size="sm" disabled={deleting} onclick={deleteWorkflow}>
            {deleting ? 'Deleting…' : 'Delete'}
          </Button>
          <Button variant="ghost" size="sm" disabled={deleting} onclick={() => (confirming = false)}>Cancel</Button>
        {:else}
          <Button variant="destructive" size="sm" onclick={() => (confirming = true)}>Delete workflow</Button>
        {/if}
      </div>
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

  .workflow-actions {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: var(--sp-2);
  }

  .confirm-prompt {
    color: var(--fg-secondary);
    font-size: var(--fs-small);
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
