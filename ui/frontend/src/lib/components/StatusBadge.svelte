<script lang="ts">
  import { statusToken } from '../status'

  let {
    status,
    size = 'sm',
    class: className = '',
  }: {
    status: string
    size?: 'sm' | 'md'
    class?: string
  } = $props()

  let token = $derived(statusToken(status))
  let slug = $derived((status || 'unknown').trim().toLowerCase().replace(/\s+/g, '-'))
  let style = $derived(`--status-color:${token.color};--status-bg:${token.bg};`)
</script>

<span
  class={`status status-${slug} ${className}`}
  class:size-md={size === 'md'}
  class:dashed={token.dashed}
  class:hatch={token.hatch}
  {style}
>{token.label}</span>

<style>
  .status {
    display: inline-flex;
    align-items: center;
    min-height: 20px;
    padding: 0 var(--sp-2);
    border: 1px solid color-mix(in srgb, var(--status-color) 55%, transparent);
    border-radius: var(--radius-chip);
    background: var(--status-bg);
    color: var(--status-color);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    line-height: 1;
    white-space: nowrap;
  }

  .size-md {
    min-height: 24px;
    font-size: var(--fs-body);
  }

  .dashed {
    border: 2px dashed var(--sig-danger);
  }

  .hatch {
    background:
      repeating-linear-gradient(135deg, transparent 0 5px, color-mix(in srgb, var(--status-color) 24%, transparent) 5px 7px),
      var(--sig-paused-bg);
  }
</style>
