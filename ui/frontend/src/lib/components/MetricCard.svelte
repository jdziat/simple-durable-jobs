<script lang="ts">
  import { deltaFlash } from '../actions/deltaFlash'

  let {
    label,
    value,
    status,
    href,
    sub,
    dominant = false,
    title,
  }: {
    label: string
    value: string | number
    status?: string
    href?: string
    sub?: string
    dominant?: boolean
    title?: string
  } = $props()

  let danger = $derived(dominant || status === 'failed' || status === 'dead-lettered')
  let cardTitle = $derived(title ?? (label === 'Active workers' ? 'workers currently holding running jobs; idle workers not counted' : undefined))
</script>

{#if href}
  <a class="card metric-card" class:danger href={href} title={cardTitle}>
    <span class="metric-label">{label}</span>
    <span class="metric-value" use:deltaFlash={value}>{value}</span>
    {#if sub}<span class="metric-sub">{sub}</span>{/if}
  </a>
{:else}
  <div class="card metric-card" class:danger title={cardTitle}>
    <span class="metric-label">{label}</span>
    <span class="metric-value" use:deltaFlash={value}>{value}</span>
    {#if sub}<span class="metric-sub">{sub}</span>{/if}
  </div>
{/if}

<style>
  .metric-card {
    display: grid;
    gap: var(--sp-2);
    min-height: 96px;
    padding: var(--sp-4);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: var(--fg-primary);
    text-decoration: none;
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .danger {
    border-color: var(--sig-danger);
    background: linear-gradient(0deg, var(--sig-danger-bg), var(--sig-danger-bg)), var(--bg-raised);
  }

  .metric-label,
  .metric-sub {
    color: var(--fg-secondary);
    font-size: var(--fs-label);
  }

  .metric-label {
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .metric-value {
    align-self: end;
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-title);
    font-feature-settings: var(--num);
    line-height: 1;
    transition: color var(--dur-quick) var(--ease), background var(--dur-quick) var(--ease), opacity var(--dur-quick) var(--ease);
  }

  .danger .metric-value {
    color: var(--sig-danger);
    font-size: var(--fs-hero);
    font-weight: var(--fw-hero);
  }

  .metric-value:global(.flash) {
    background: var(--sig-info-bg);
    color: var(--accent);
  }
</style>
