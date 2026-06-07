<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { absolute, relative } from '../time'

  type ProtoTimestamp = { toDate?: () => Date; seconds?: bigint | number | string; nanos?: number }
  type TimeInput = number | Date | ProtoTimestamp | null | undefined

  let {
    ts,
    mode = 'rel',
    live = false,
  }: {
    ts?: TimeInput
    mode?: 'rel' | 'abs' | 'both' | 'clock'
    live?: boolean
  } = $props()

  let now = $state(Date.now())
  let timer: ReturnType<typeof setInterval> | null = null
  let value = $derived.by(() => {
    now
    if (mode === 'clock') return absolute(Date.now())
    if (!ts) return '—'
    if (mode === 'abs') return absolute(ts)
    if (mode === 'both') return `${relative(ts)} · ${absolute(ts)}`
    return relative(ts)
  })

  onMount(() => {
    if (live || mode === 'clock') {
      timer = setInterval(() => {
        now = Date.now()
      }, 1000)
    }
  })

  onDestroy(() => {
    if (timer) clearInterval(timer)
  })
</script>

<time class="relative-time">{value}</time>

<style>
  .relative-time {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
    white-space: nowrap;
  }
</style>
