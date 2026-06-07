<script lang="ts">
  import { onMount } from 'svelte'
  import Dashboard from './routes/Dashboard.svelte'
  import Jobs from './routes/Jobs.svelte'
  import JobDetail from './routes/JobDetail.svelte'
  import Queues from './routes/Queues.svelte'
  import Scheduled from './routes/Scheduled.svelte'
  import Workflows from './routes/Workflows.svelte'
  import WorkflowDetail from './routes/WorkflowDetail.svelte'
  import Ticker from './lib/components/Ticker.svelte'
  import NavRail from './lib/components/NavRail.svelte'
  import { start as startStats, stop as stopStats } from './lib/stores/stats.svelte'

  let currentPath = $state(window.location.hash.slice(1) || '/')
  let jobId = $state<string | null>(null)
  let workflowId = $state<string | null>(null)
  let queryParams = $state<Record<string, string>>({})

  function navigate(path: string) {
    window.location.hash = path
  }

  function parseHash(hash: string): { path: string; params: Record<string, string> } {
    const [pathPart, queryPart] = hash.split('?')
    const params: Record<string, string> = {}
    if (queryPart) {
      for (const pair of queryPart.split('&')) {
        const [key, value] = pair.split('=')
        if (key) params[decodeURIComponent(key)] = decodeURIComponent(value ?? '')
      }
    }
    return { path: pathPart || '/', params }
  }

  function handleHashChange() {
    const raw = window.location.hash.slice(1) || '/'
    const { path, params } = parseHash(raw)
    currentPath = path
    queryParams = params

    const jobMatch = path.match(/^\/jobs\/([^/]+)$/)
    jobId = jobMatch ? jobMatch[1] : null

    const workflowMatch = path.match(/^\/workflows\/([^/]+)$/)
    workflowId = workflowMatch ? workflowMatch[1] : null
  }

  onMount(() => {
    startStats()
    window.addEventListener('hashchange', handleHashChange)
    handleHashChange()
    return () => {
      window.removeEventListener('hashchange', handleHashChange)
      stopStats()
    }
  })

  function isActive(path: string): boolean {
    if (path === '/') return currentPath === '/'
    return currentPath.startsWith(path)
  }
</script>

<a class="skip-link" href="#content">Skip to content</a>
<div class="app">
  <Ticker />
  <NavRail {currentPath} {isActive} />
  <main id="content" class="content" tabindex="-1">
    {#if currentPath === '/'}
      <Dashboard />
    {:else if currentPath === '/jobs'}
      <Jobs {navigate} initialStatus={queryParams.status ?? ''} initialQueue={queryParams.queue ?? ''} />
    {:else if jobId}
      <JobDetail id={jobId} {navigate} />
    {:else if currentPath === '/queues'}
      <Queues />
    {:else if currentPath === '/scheduled'}
      <Scheduled />
    {:else if currentPath === '/workflows'}
      <Workflows />
    {:else if workflowId}
      <WorkflowDetail id={workflowId} {navigate} />
    {:else}
      <Dashboard />
    {/if}
  </main>
</div>

<style>
  .skip-link {
    position: fixed;
    top: var(--sp-3);
    left: var(--sp-3);
    z-index: 100;
    transform: translateY(calc(-100% - var(--sp-4)));
    padding: var(--sp-2) var(--sp-3);
    border: var(--border-strong);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-primary);
    font-weight: var(--fw-label);
    text-decoration: none;
    transition: transform var(--dur-instant) var(--ease);
  }

  .skip-link:focus {
    transform: translateY(0);
  }

  .app {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr);
    grid-template-rows: 56px minmax(0, 1fr);
    min-height: 100vh;
    background: var(--bg-base);
    color: var(--fg-primary);
  }

  .content {
    min-width: 0;
    min-height: calc(100vh - 56px);
    padding: var(--sp-6);
    background: var(--bg-base);
  }

  .content:focus {
    outline: none;
  }

  @media (max-width: 1439px) {
    .content {
      padding: var(--sp-4);
    }
  }

  @media (max-width: 767px) {
    .app {
      display: block;
      padding-bottom: 56px;
    }

    .content {
      min-height: calc(100vh - 112px);
      padding: var(--sp-3);
    }
  }
</style>
