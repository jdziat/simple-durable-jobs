<script lang="ts">
  import { onMount } from 'svelte'
  import Dashboard from './routes/Dashboard.svelte'
  import Jobs from './routes/Jobs.svelte'
  import JobDetail from './routes/JobDetail.svelte'
  import Queues from './routes/Queues.svelte'
  import Scheduled from './routes/Scheduled.svelte'

  let currentPath = $state(window.location.hash.slice(1) || '/')
  let jobId = $state<string | null>(null)
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
  }

  onMount(() => {
    window.addEventListener('hashchange', handleHashChange)
    handleHashChange()
    return () => window.removeEventListener('hashchange', handleHashChange)
  })

  function isActive(path: string): boolean {
    if (path === '/') return currentPath === '/'
    return currentPath.startsWith(path)
  }
</script>

<div class="app">
  <nav class="sidebar">
    <div class="logo">
      <h1>Jobs UI</h1>
    </div>
    <ul class="nav-links">
      <li>
        <a href="#/" class:active={isActive('/')}>Dashboard</a>
      </li>
      <li>
        <a href="#/jobs" class:active={isActive('/jobs')}>Jobs</a>
      </li>
      <li>
        <a href="#/queues" class:active={isActive('/queues')}>Queues</a>
      </li>
      <li>
        <a href="#/scheduled" class:active={isActive('/scheduled')}>Scheduled</a>
      </li>
    </ul>
  </nav>
  <main class="content">
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
    {:else}
      <Dashboard />
    {/if}
  </main>
</div>

<style>
  :global(*) {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }

  :global(body) {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    background: #f5f7fa;
    color: #333;
  }

  .app {
    display: flex;
    min-height: 100vh;
  }

  .sidebar {
    width: 220px;
    background: #1a1a2e;
    color: white;
    padding: 20px 0;
    position: fixed;
    height: 100vh;
  }

  .logo {
    padding: 0 20px 20px;
    border-bottom: 1px solid rgba(255,255,255,0.1);
  }

  .logo h1 {
    font-size: 20px;
    font-weight: 600;
  }

  .nav-links {
    list-style: none;
    padding: 20px 0;
  }

  .nav-links li {
    margin: 4px 0;
  }

  .nav-links a {
    display: block;
    padding: 12px 20px;
    color: rgba(255,255,255,0.7);
    text-decoration: none;
    transition: all 0.2s;
  }

  .nav-links a:hover,
  .nav-links a.active {
    background: rgba(255,255,255,0.1);
    color: white;
  }

  .content {
    flex: 1;
    margin-left: 220px;
    padding: 24px;
    min-height: 100vh;
  }
</style>
