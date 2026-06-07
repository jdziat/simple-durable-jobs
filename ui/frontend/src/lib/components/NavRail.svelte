<script lang="ts">
  import { icons, type IconName } from '../icons'
  import { stats } from '../stores/stats.svelte'

  interface Item {
    label: string
    href: string
    path: string
    icon: IconName
  }

  let { isActive }: { currentPath: string; isActive: (path: string) => boolean } = $props()

  const items: Item[] = [
    { label: 'Dashboard', href: '#/', path: '/', icon: 'dashboard' },
    { label: 'Jobs', href: '#/jobs', path: '/jobs', icon: 'jobs' },
    { label: 'Queues', href: '#/queues', path: '/queues', icon: 'queue' },
    { label: 'Scheduled', href: '#/scheduled', path: '/scheduled', icon: 'schedule' },
    { label: 'Workflows', href: '#/workflows', path: '/workflows', icon: 'workflow' },
  ]

  function initialExpanded() {
    if (typeof localStorage === 'undefined') return false
    try {
      return localStorage.getItem('blackbox-rail') === 'expanded'
    } catch {
      return false
    }
  }

  let expanded = $state(initialExpanded())
  let failed = $derived(stats.value?.totalFailed ?? 0)
  let anyPaused = $derived(stats.value?.queues.some(q => q.isPaused || q.paused > 0) ?? false)

  function toggleRail() {
    expanded = !expanded
    try {
      localStorage.setItem('blackbox-rail', expanded ? 'expanded' : 'collapsed')
    } catch {
      // Persistence is best-effort.
    }
  }
</script>

<nav class:expanded class="nav-rail" aria-label="Primary">
  <div class="nav-links">
    {#each items as item}
      <a href={item.href} class:active={isActive(item.path)} aria-label={item.label}>
        <svg viewBox="0 0 28 28" aria-hidden="true"><path d={icons[item.icon]} /></svg>
        <span class="nav-label">{item.label}</span>
        {#if item.label === 'Jobs' && failed > 0}
          <span class="nav-danger" aria-hidden="true"></span>
          <span class="nav-badge" aria-label={`${failed} failed jobs`}>{failed}</span>
        {/if}
        {#if item.label === 'Queues' && anyPaused}
          <span class="paused-glyph" aria-label="Paused queues">Ⅱ</span>
        {/if}
      </a>
    {/each}
  </div>
  <button class="rail-toggle" type="button" onclick={toggleRail} aria-label={expanded ? 'Collapse navigation' : 'Expand navigation'}>
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons.chevron} /></svg>
  </button>
</nav>

<style>
  .nav-rail {
    position: sticky;
    top: 56px;
    z-index: 10;
    display: flex;
    flex-direction: column;
    width: 56px;
    height: calc(100vh - 56px);
    border-right: var(--border-strong);
    background: var(--bg-base);
    box-shadow: inset 1px 0 0 var(--inset-sheen);
    transition: width var(--dur-settle) var(--ease);
  }

  .nav-rail.expanded {
    width: 200px;
  }

  .nav-links {
    display: flex;
    flex-direction: column;
    gap: var(--sp-1);
    padding: var(--sp-2) 0;
  }

  .nav-links a,
  .rail-toggle {
    position: relative;
    display: flex;
    align-items: center;
    min-height: 44px;
    border: 0;
    border-left: 3px solid transparent;
    background: transparent;
    color: var(--fg-secondary);
    text-decoration: none;
    cursor: pointer;
  }

  .nav-links a {
    padding: 0 var(--sp-2);
    gap: var(--sp-3);
  }

  .nav-links a:hover,
  .rail-toggle:hover {
    color: var(--fg-primary);
    background: var(--bg-raised);
  }

  .nav-links a.active {
    color: var(--fg-primary);
    border-left-color: var(--accent);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  svg {
    flex: 0 0 20px;
    width: 20px;
    height: 20px;
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .nav-label {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
    font-size: var(--fs-emph);
    font-weight: var(--fw-label);
  }

  .expanded .nav-label {
    position: static;
    width: auto;
    height: auto;
    margin: 0;
    overflow: visible;
    clip: auto;
  }

  .nav-danger {
    position: absolute;
    right: 9px;
    top: 9px;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--sig-danger);
  }

  .nav-badge,
  .paused-glyph {
    position: absolute;
    right: var(--sp-2);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-feature-settings: var(--num);
    color: var(--sig-danger);
  }

  .nav-badge {
    bottom: 7px;
  }

  .paused-glyph {
    color: var(--sig-paused);
  }

  .rail-toggle {
    justify-content: center;
    margin-top: auto;
    min-height: 40px;
    border-top: var(--border);
  }

  .expanded .rail-toggle svg {
    transform: rotate(180deg);
  }

  @media (max-width: 1023px) {
    .nav-rail,
    .nav-rail.expanded {
      width: 56px;
    }

    .nav-label {
      position: absolute;
      width: 1px;
      height: 1px;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
    }
  }

  @media (max-width: 767px) {
    .nav-rail,
    .nav-rail.expanded {
      position: fixed;
      top: auto;
      right: 0;
      bottom: 0;
      left: 0;
      width: 100%;
      height: 56px;
      border-top: var(--border-strong);
      border-right: 0;
    }

    .nav-links {
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      width: 100%;
      padding: 0;
    }

    .nav-links a {
      justify-content: center;
      min-height: 56px;
      border-left: 0;
      border-top: 3px solid transparent;
    }

    .nav-links a.active {
      border-top-color: var(--accent);
      border-left-color: transparent;
    }

    .rail-toggle {
      display: none;
    }
  }
</style>
