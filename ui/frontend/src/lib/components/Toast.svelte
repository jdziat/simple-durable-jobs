<script lang="ts">
  import { dismiss, toasts } from '../stores/toast.svelte'
</script>

<!-- No aria-live on the stack: each toast carries its own role/aria-live,
     and nested live regions double-announce on some screen readers. -->
<div class="toast-stack">
  {#each toasts.list as item (item.id)}
    <div class="toast toast-{item.kind}" role={item.kind === 'err' ? 'alert' : 'status'} aria-live={item.kind === 'err' ? 'assertive' : 'polite'}>
      <span>{item.msg}</span>
      {#if item.undo}
        <button type="button" onclick={() => { item.undo?.(); dismiss(item.id) }}>Undo</button>
      {/if}
      <button class="toast-close" type="button" aria-label="Dismiss" onclick={() => dismiss(item.id)}>×</button>
    </div>
  {/each}
</div>

<style>
  .toast-stack {
    position: fixed;
    top: calc(56px + var(--sp-4));
    right: var(--sp-4);
    z-index: 80;
    display: grid;
    gap: var(--sp-2);
    width: min(360px, calc(100vw - var(--sp-8)));
    pointer-events: none;
  }

  .toast {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto;
    align-items: center;
    gap: var(--sp-2);
    padding: var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: var(--fg-primary);
    box-shadow: var(--shadow-overlay);
    pointer-events: auto;
    animation: toastIn var(--dur-quick) var(--ease) both;
  }

  .toast-ok {
    border-color: var(--sig-ok);
  }

  .toast-err {
    border-color: var(--sig-danger);
  }

  .toast-info {
    border-color: var(--accent);
  }

  span {
    min-width: 0;
  }

  button {
    min-height: 26px;
    padding: 0 var(--sp-2);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    cursor: pointer;
  }

  .toast-close {
    width: 26px;
    padding: 0;
  }

  @keyframes toastIn {
    from { opacity: 0; transform: translateY(calc(-1 * var(--sp-2))); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
