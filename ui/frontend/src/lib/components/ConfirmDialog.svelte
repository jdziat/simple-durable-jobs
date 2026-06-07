<script module lang="ts">
  export { isArmed } from '../confirm'
</script>

<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { isArmed } from '../confirm'
  import Button from './Button.svelte'

  let {
    title,
    body,
    blastRadius,
    confirmWord,
    confirmLabel,
    onConfirm,
    onCancel,
  }: {
    title: string
    body: string
    blastRadius?: string
    confirmWord?: string
    confirmLabel: string
    onConfirm: () => void
    onCancel: () => void
  } = $props()

  let typed = $state('')
  let dialog = $state<HTMLDivElement>()
  let input = $state<HTMLInputElement>()
  let previousFocus: HTMLElement | null = null
  let armed = $derived(isArmed(typed, confirmWord))

  function focusables(): HTMLElement[] {
    return Array.from(dialog?.querySelectorAll<HTMLElement>('button, input, [href], [tabindex]:not([tabindex="-1"])') ?? []).filter(el => !el.hasAttribute('disabled'))
  }

  function cancel() {
    onCancel()
  }

  function confirm() {
    if (!armed) return
    onConfirm()
  }

  function keydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      event.preventDefault()
      cancel()
      return
    }
    if (event.key !== 'Tab') return
    const items = focusables()
    if (items.length === 0) return
    const first = items[0]
    const last = items[items.length - 1]
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault()
      last.focus()
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault()
      first.focus()
    }
  }

  onMount(() => {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
    setTimeout(() => (confirmWord ? input?.focus() : focusables()[0]?.focus()), 0)
  })

  onDestroy(() => {
    previousFocus?.focus()
  })
</script>

<div class="dialog-backdrop" role="presentation" onkeydown={keydown}>
  <div bind:this={dialog} class="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
    <h2 id="confirm-title">{title}</h2>
    <p>{body}</p>
    {#if blastRadius}
      <div class="blast-radius">{blastRadius}</div>
    {/if}
    {#if confirmWord}
      <label>
        <span>Type <strong>{confirmWord}</strong> to confirm</span>
        <input bind:this={input} bind:value={typed} autocomplete="off" />
      </label>
    {/if}
    <div class="dialog-actions">
      <Button variant="ghost" onclick={cancel}>Cancel</Button>
      <Button variant="destructive" disabled={!armed} onclick={confirm}>{confirmLabel}</Button>
    </div>
  </div>
</div>

<style>
  .dialog-backdrop {
    position: fixed;
    inset: 0;
    z-index: 70;
    display: grid;
    place-items: center;
    padding: var(--sp-4);
    background: color-mix(in srgb, var(--bg-base) 72%, transparent);
  }

  .confirm-dialog {
    display: grid;
    gap: var(--sp-4);
    width: min(520px, 100%);
    padding: var(--sp-6);
    border: var(--border-strong);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: var(--shadow-overlay);
    animation: dialogIn var(--dur-quick) var(--ease) both;
  }

  h2 {
    font-size: var(--fs-title);
    line-height: var(--lh-dense);
  }

  p,
  label span {
    color: var(--fg-secondary);
  }

  .blast-radius {
    padding: var(--sp-3);
    border: 1px solid var(--sig-danger);
    border-radius: var(--radius-input);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
  }

  label {
    display: grid;
    gap: var(--sp-2);
  }

  input {
    min-height: 34px;
    padding: 0 var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-family: var(--font-mono);
  }

  .dialog-actions {
    display: flex;
    justify-content: flex-end;
    gap: var(--sp-2);
  }

  @keyframes dialogIn {
    from { opacity: 0; transform: translateY(var(--sp-2)); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
