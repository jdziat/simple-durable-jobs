<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import EnvPill from './EnvPill.svelte'
  import { icons } from '../icons'
  import { absolute, relative } from '../time'
  import { density, toggleDensity } from '../stores/density.svelte'
  import { theme, toggleTheme } from '../stores/theme.svelte'
  import { lastUpdated, polling } from '../stores/stats.svelte'

  const appName = import.meta.env.VITE_APP_NAME ?? 'Durable Jobs'
  const env = import.meta.env.VITE_APP_ENV as 'prod' | 'staging' | 'dev' | undefined
  const version = import.meta.env.VITE_APP_VERSION as string | undefined

  let now = $state(Date.now())
  let clock: ReturnType<typeof setInterval> | null = null
  let ageMs = $derived(lastUpdated.value > 0 ? now - lastUpdated.value : Number.POSITIVE_INFINITY)
  let stale = $derived(ageMs > 15000)
  let heartbeatText = $derived(lastUpdated.value > 0 ? `${stale ? 'last update' : 'updated'} ${relative(lastUpdated.value).replace(' ago', '')} ago` : 'awaiting update')
  let clockText = $derived(absolute(now))

  onMount(() => {
    document.title = appName
    clock = setInterval(() => {
      now = Date.now()
    }, 1000)
  })

  onDestroy(() => {
    if (clock) clearInterval(clock)
  })
</script>

<header class="ticker">
  <div class="ticker-left">
    <span class="app-identity">{appName}</span>
    {#if env}
      <EnvPill {env} />
    {/if}
    {#if version}
      <span class="version">{version}</span>
    {/if}
    <span class="clock"><span class="ticker-label">Local</span> {clockText}</span>
    <span class:stale class:polling={polling.value} class="heartbeat" aria-live={stale ? 'polite' : 'off'}>
      <span class="heartbeat-line"></span>
      <span class="heartbeat-dot"></span>
      <span>{heartbeatText}</span>
    </span>
  </div>
  <div class="ticker-right">
    <button class="icon-button density-toggle" type="button" onclick={toggleDensity} title="Toggle density" aria-label="Toggle density">
      <span>{density.value === 'dense' ? '28' : '40'}</span>
    </button>
    <button class="icon-button theme-toggle" type="button" onclick={toggleTheme} title="Toggle theme" aria-label="Toggle theme">
      <svg viewBox="0 0 24 24" aria-hidden="true"><path d={theme.value === 'dark' ? icons.sun : icons.moon} /></svg>
    </button>
    <span class="cmd-chip"><svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons.command} /></svg> K</span>
  </div>
</header>

<style>
  .ticker {
    position: sticky;
    top: 0;
    z-index: 20;
    grid-column: 1 / -1;
    display: flex;
    align-items: center;
    justify-content: space-between;
    min-height: 56px;
    padding: 0 var(--sp-4);
    border-bottom: var(--border-strong);
    background: var(--bg-base);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .ticker-left,
  .ticker-right {
    display: flex;
    align-items: center;
    gap: var(--sp-3);
    min-width: 0;
  }

  .app-identity,
  .version,
  .clock,
  .heartbeat,
  .cmd-chip,
  .icon-button {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .app-identity {
    font-size: var(--fs-section);
    font-weight: var(--fw-head);
    white-space: nowrap;
  }

  .version,
  .clock,
  .heartbeat {
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
    white-space: nowrap;
  }

  .ticker-label {
    /* fg-secondary, not fg-muted: this 11px label is load-bearing (it marks
       the clock as CLIENT time) and muted is below AA at sub-16px (§6.4). */
    color: var(--fg-secondary);
    margin-right: var(--sp-1);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .heartbeat {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
  }

  .heartbeat-line {
    width: 24px;
    height: 2px;
    border-radius: 1px;
    background: var(--accent);
  }

  .heartbeat-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--accent);
  }

  .heartbeat.polling .heartbeat-line {
    animation: heartbeat var(--dur-quick) var(--ease) infinite alternate;
  }

  .heartbeat.stale {
    color: var(--sig-danger);
  }

  .heartbeat.stale .heartbeat-line {
    background: repeating-linear-gradient(90deg, var(--sig-warn) 0 4px, transparent 4px 7px);
  }

  .heartbeat.stale .heartbeat-dot {
    background: var(--sig-danger);
  }

  @keyframes heartbeat {
    from { opacity: 0.35; }
    to { opacity: 1; }
  }

  .icon-button,
  .cmd-chip {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 28px;
    min-width: 28px;
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-secondary);
  }

  .icon-button {
    cursor: pointer;
  }

  .icon-button:hover {
    color: var(--fg-primary);
    border-color: var(--hairline-strong);
  }

  .icon-button svg,
  .cmd-chip svg {
    width: 16px;
    height: 16px;
  }

  svg {
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .density-toggle span,
  .cmd-chip {
    padding: 0 var(--sp-2);
    font-size: var(--fs-micro);
  }

  .cmd-chip {
    gap: var(--sp-1);
    color: var(--fg-secondary);
  }

  @media (max-width: 767px) {
    .ticker {
      padding: 0 var(--sp-3);
    }

    .clock,
    .version,
    :global(.env-pill),
    .density-toggle,
    .cmd-chip {
      display: none;
    }

    .ticker-left {
      gap: var(--sp-2);
    }
  }
</style>
