<script lang="ts">
  import { icons } from '../icons'
  import { toggleDensity } from '../stores/density.svelte'
  import { toggleTheme } from '../stores/theme.svelte'
  import { toast } from '../stores/toast.svelte'
  import { fuzzyFilter } from '../fuzzy'

  interface Action {
    id: string
    label: string
    hint: string
    run: () => void
  }

  let {
    open,
    onClose,
    navigate,
    queues = [],
    jobs = [],
    workflows = [],
  }: {
    open: boolean
    onClose: () => void
    navigate: (path: string) => void
    queues?: string[]
    jobs?: string[]
    workflows?: string[]
  } = $props()

  const statuses = ['pending', 'running', 'completed', 'failed', 'dead-lettered', 'cancelled', 'paused', 'waiting', 'retrying']
  const appName = import.meta.env.VITE_APP_NAME ?? 'Durable Jobs'
  const version = import.meta.env.VITE_APP_VERSION ?? 'unversioned'

  let query = $state('')
  let active = $state(0)
  let panel = $state<HTMLDivElement>()
  let input = $state<HTMLInputElement>()
  let previousFocus: HTMLElement | null = null
  let listId = 'command-palette-results'
  let actions = $derived.by<Action[]>(() => {
    const base: Action[] = [
      { id: 'theme', label: 'Toggle theme', hint: 'appearance', run: toggleTheme },
      { id: 'density', label: 'Toggle density', hint: 'layout', run: toggleDensity },
      { id: 'about', label: 'About / credits', hint: 'Durable Jobs · JetBrains Mono, OFL-1.1', run: () => toast.push({ kind: 'info', msg: `${appName} ${version} · JetBrains Mono, OFL-1.1` }) },
      ...statuses.map(status => ({ id: `status-${status}`, label: `Filter Jobs: ${status}`, hint: 'jobs', run: () => navigate(`/jobs?status=${encodeURIComponent(status)}`) })),
      ...queues.map(queue => ({ id: `queue-${queue}`, label: `Open queue: ${queue}`, hint: 'queues', run: () => navigate(`/jobs?queue=${encodeURIComponent(queue)}`) })),
      ...jobs.map(job => ({ id: `job-${job}`, label: `Open job: ${job}`, hint: 'jobs', run: () => navigate(`/jobs/${encodeURIComponent(job)}`) })),
      ...workflows.map(workflow => ({ id: `workflow-${workflow}`, label: `Open workflow: ${workflow}`, hint: 'workflows', run: () => navigate(`/workflows/${encodeURIComponent(workflow)}`) })),
    ]
    const filtered = fuzzyFilter(query, base, action => `${action.label} ${action.hint}`)
    if (query.trim()) {
      filtered.push({ id: `free-job-${query.trim()}`, label: `Open job id: ${query.trim()}`, hint: 'free typed', run: () => navigate(`/jobs/${encodeURIComponent(query.trim())}`) })
    }
    return filtered.slice(0, 10)
  })
  let activeId = $derived(actions[active]?.id ? `command-option-${actions[active].id}` : undefined)

  function close() {
    onClose()
  }

  function execute(action: Action | undefined) {
    if (!action) return
    action.run()
    close()
  }

  function focusables(): HTMLElement[] {
    return Array.from(panel?.querySelectorAll<HTMLElement>('input, button, [href], [tabindex]:not([tabindex="-1"])') ?? []).filter(el => !el.hasAttribute('disabled'))
  }

  function keydown(event: KeyboardEvent) {
    if (event.key === 'Escape') {
      event.preventDefault()
      close()
    } else if (event.key === 'ArrowDown') {
      event.preventDefault()
      active = Math.min(actions.length - 1, active + 1)
    } else if (event.key === 'ArrowUp') {
      event.preventDefault()
      active = Math.max(0, active - 1)
    } else if (event.key === 'Enter') {
      event.preventDefault()
      execute(actions[active])
    } else if (event.key === 'Tab') {
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
  }

  // Focus capture/restore must track the `open` PROP, not mount/unmount: the
  // palette is always mounted in App.svelte with only its body behind
  // `{#if open}`, so onMount fires once at app boot (input undefined) and
  // onDestroy only at teardown — which left the focus trap dead on open.
  $effect(() => {
    if (open) {
      previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
      queueMicrotask(() => input?.focus())
    } else {
      previousFocus?.focus()
      previousFocus = null
    }
  })
</script>

{#if open}
  <div class="palette-backdrop" role="presentation" onkeydown={keydown}>
    <div bind:this={panel} class="command-palette" role="dialog" aria-modal="true" aria-label="Command palette">
      <div class="search-row">
        <svg viewBox="0 0 24 24" aria-hidden="true"><path d={icons.search} /></svg>
        <input
          bind:this={input}
          bind:value={query}
          role="combobox"
          aria-expanded="true"
          aria-controls={listId}
          aria-activedescendant={activeId}
          placeholder="command, queue, job id"
          oninput={() => { active = 0 }}
        />
      </div>
      <div id={listId} class="results" role="listbox">
        {#each actions as action, index (action.id)}
          <button
            id={`command-option-${action.id}`}
            class:active={index === active}
            type="button"
            role="option"
            aria-selected={index === active}
            onclick={() => execute(action)}
          >
            <span>{action.label}</span>
            <small>{action.hint}</small>
          </button>
        {/each}
      </div>
    </div>
  </div>
{/if}

<style>
  .palette-backdrop {
    position: fixed;
    inset: 0;
    z-index: 75;
    display: grid;
    place-items: start center;
    padding-top: 12vh;
    background: color-mix(in srgb, var(--bg-base) 56%, transparent);
  }

  .command-palette {
    display: grid;
    width: min(680px, calc(100vw - var(--sp-8)));
    border: var(--border-strong);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: var(--shadow-overlay);
    overflow: hidden;
    animation: paletteIn var(--dur-quick) var(--ease) both;
  }

  .search-row {
    display: flex;
    align-items: center;
    gap: var(--sp-3);
    padding: var(--sp-3);
    border-bottom: var(--border);
    background: var(--bg-sunken);
  }

  svg {
    width: 18px;
    height: 18px;
    fill: none;
    stroke: currentColor;
    stroke-width: 2;
    stroke-linecap: round;
    stroke-linejoin: round;
    color: var(--fg-secondary);
  }

  input {
    width: 100%;
    border: 0;
    outline: 0;
    background: transparent;
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-section);
    font-feature-settings: var(--num);
  }

  .results {
    display: grid;
    padding: var(--sp-2);
  }

  .results button {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: var(--sp-3);
    align-items: center;
    min-height: 38px;
    padding: 0 var(--sp-3);
    border: 0;
    border-radius: var(--radius-input);
    background: transparent;
    color: var(--fg-primary);
    font-family: var(--font-mono);
    text-align: left;
    cursor: pointer;
  }

  .results button.active,
  .results button:hover {
    background: var(--bg-sunken);
  }

  small {
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
  }

  @keyframes paletteIn {
    from { opacity: 0; transform: translateY(calc(-1 * var(--sp-2))); }
    to { opacity: 1; transform: translateY(0); }
  }
</style>
