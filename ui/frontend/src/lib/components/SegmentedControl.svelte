<script lang="ts">
  type Option = string | { value: string; label: string }
  let {
    options,
    value,
    onChange,
  }: {
    options: Option[]
    value: string
    onChange: (value: string) => void
  } = $props()

  let buttons = $state<HTMLButtonElement[]>([])
  let normalized = $derived(options.map(option => typeof option === 'string' ? { value: option, label: option } : option))

  function select(next: string) {
    onChange(next)
  }

  function move(delta: number) {
    const index = normalized.findIndex(option => option.value === value)
    const next = (index + delta + normalized.length) % normalized.length
    select(normalized[next].value)
    buttons[next]?.focus()
  }
</script>

<div class="segmented-control period-selector" role="radiogroup">
  {#each normalized as option, index}
    <button
      bind:this={buttons[index]}
      type="button"
      role="radio"
      aria-checked={value === option.value}
      class:active={value === option.value}
      tabindex={value === option.value ? 0 : -1}
      onclick={() => select(option.value)}
      onkeydown={(event) => {
        if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
          event.preventDefault()
          move(1)
        } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
          event.preventDefault()
          move(-1)
        } else if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault()
          select(option.value)
        }
      }}
    >
      {option.label}
    </button>
  {/each}
</div>

<style>
  .segmented-control {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-1);
    padding: var(--sp-1);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
  }

  button {
    min-height: 28px;
    padding: 0 var(--sp-3);
    border: 0;
    border-radius: var(--radius-chip);
    background: transparent;
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    cursor: pointer;
  }

  button.active {
    color: var(--fg-primary);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }
</style>
