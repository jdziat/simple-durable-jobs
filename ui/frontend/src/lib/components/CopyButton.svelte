<script lang="ts">
  import { onDestroy } from 'svelte'
  import { icons } from '../icons'
  import { toast } from '../stores/toast.svelte'

  let { text }: { text: string } = $props()
  let copied = $state(false)
  let timer: ReturnType<typeof setTimeout> | null = null

  onDestroy(() => {
    if (timer) clearTimeout(timer)
  })

  async function copy() {
    try {
      await navigator.clipboard.writeText(text)
      copied = true
      toast.push({ kind: 'ok', msg: 'copied' })
      if (timer) clearTimeout(timer)
      timer = setTimeout(() => {
        copied = false
      }, 900)
    } catch (e) {
      toast.push({ kind: 'err', msg: e instanceof Error ? e.message : 'copy failed' })
    }
  }
</script>

<button class="copy-button" type="button" onclick={copy} aria-label="Copy">
  {#if copied}
    <span aria-hidden="true">✓</span>
  {:else}
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons.copy} /></svg>
  {/if}
</button>

<style>
  .copy-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-secondary);
    cursor: pointer;
  }

  .copy-button:hover {
    color: var(--fg-primary);
    border-color: var(--hairline-strong);
  }

  svg {
    width: 14px;
    height: 14px;
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  span {
    color: var(--sig-ok);
    font-family: var(--font-mono);
  }
</style>
