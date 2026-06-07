<script lang="ts">
  import { icons, type IconName } from '../icons'

  let {
    title,
    hint,
    code,
    icon = 'search',
    class: className = '',
  }: {
    title: string
    hint: string
    code?: string
    icon?: IconName
    class?: string
  } = $props()
</script>

<div class={`empty ${className}`}>
  <svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons[icon]} /></svg>
  <div class="empty-title">{title}</div>
  <p>{hint}</p>
  {#if code}
    <code>{code}</code>
  {/if}
</div>

<style>
  .empty {
    display: grid;
    justify-items: center;
    gap: var(--sp-2);
    padding: var(--sp-8);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: var(--fg-secondary);
    text-align: center;
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  svg {
    width: 28px;
    height: 28px;
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .empty-title {
    color: var(--fg-primary);
    font-size: var(--fs-section);
    font-weight: var(--fw-head);
  }

  p {
    max-width: 52ch;
    font-size: var(--fs-body);
  }

  code {
    padding: var(--sp-2) var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
  }
</style>
