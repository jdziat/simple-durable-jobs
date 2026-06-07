<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { duration } from '../time'

  type ProtoTimestamp = { toDate?: () => Date; seconds?: bigint | number | string; nanos?: number }
  type TimeInput = number | Date | ProtoTimestamp | null | undefined

  let {
    from,
    to,
    live = true,
  }: {
    from: TimeInput
    to?: TimeInput
    live?: boolean
  } = $props()

  let now = $state(Date.now())
  let timer: ReturnType<typeof setInterval> | null = null
  let value = $derived(duration(from, to ?? now))

  onMount(() => {
    if (!to && live) {
      timer = setInterval(() => {
        now = Date.now()
      }, 1000)
    }
  })

  onDestroy(() => {
    if (timer) clearInterval(timer)
  })
</script>

<span class="duration">{value}</span>

<style>
  .duration {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
    white-space: nowrap;
  }
</style>
