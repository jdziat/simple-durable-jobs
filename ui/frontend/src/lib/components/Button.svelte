<script lang="ts">
  import type { Snippet } from 'svelte'
  import { icons, type IconName } from '../icons'

  let {
    variant = 'secondary',
    size = 'md',
    icon,
    loading = false,
    disabled = false,
    class: className = '',
    onclick,
    children,
  }: {
    variant?: 'primary' | 'secondary' | 'ghost' | 'destructive'
    size?: 'sm' | 'md'
    icon?: IconName
    loading?: boolean
    disabled?: boolean
    class?: string
    onclick?: (event: MouseEvent) => void
    children?: Snippet
  } = $props()
</script>

<button
  class={`btn ${variant} ${size} ${className}`}
  type="button"
  disabled={disabled || loading}
  aria-busy={loading}
  {onclick}
>
  {#if loading}
    <span class="spinner" aria-hidden="true"></span>
  {:else if icon}
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons[icon]} /></svg>
  {/if}
  <span>{@render children?.()}</span>
</button>

<style>
  .btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: var(--sp-2);
    min-width: 0;
    border: var(--border);
    border-radius: var(--radius-input);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-weight: var(--fw-label);
    font-feature-settings: var(--num);
    line-height: 1;
    cursor: pointer;
    transition: background var(--dur-instant) var(--ease), color var(--dur-instant) var(--ease), opacity var(--dur-instant) var(--ease);
  }

  .md {
    min-height: 32px;
    padding: 0 var(--sp-3);
  }

  .sm {
    min-height: 26px;
    padding: 0 var(--sp-2);
  }

  .primary {
    border-color: var(--accent);
    background: var(--accent);
    color: var(--bg-base);
  }

  .secondary {
    background: var(--bg-raised);
    color: var(--fg-primary);
  }

  .ghost {
    border-color: transparent;
    background: transparent;
    color: var(--fg-secondary);
  }

  .destructive {
    border-color: var(--sig-danger);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  .btn:hover:not(:disabled) {
    border-color: var(--hairline-strong);
    color: var(--fg-primary);
  }

  .btn:disabled {
    cursor: not-allowed;
    opacity: 0.55;
  }

  svg {
    width: 15px;
    height: 15px;
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .spinner {
    width: 14px;
    height: 14px;
    border: 2px solid currentColor;
    border-right-color: transparent;
    border-radius: 50%;
    animation: spin var(--dur-settle) linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>
