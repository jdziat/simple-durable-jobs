<script module lang="ts">
  export { ageHeatLevel } from '../ageHeat'
</script>

<script lang="ts">
  import { relative } from '../time'
  import { ageHeatLevel } from '../ageHeat'

  type ProtoTimestamp = { toDate?: () => Date; seconds?: bigint | number | string; nanos?: number }
  type TimeInput = number | Date | ProtoTimestamp | null | undefined

  let {
    ts,
    warnAfterSec = 120,
    dangerAfterSec = 600,
  }: {
    ts: TimeInput
    warnAfterSec?: number
    dangerAfterSec?: number
  } = $props()

  function epoch(value: TimeInput): number {
    if (!value) return Date.now()
    if (value instanceof Date) return value.getTime()
    if (typeof value === 'number') return value
    if (typeof value.toDate === 'function') return value.toDate().getTime()
    return Number(value.seconds ?? 0) * 1000 + Math.floor(Number(value.nanos ?? 0) / 1_000_000)
  }

  let ageSec = $derived(Math.max(0, Math.floor((Date.now() - epoch(ts)) / 1000)))
  let level = $derived(ageHeatLevel(ageSec, warnAfterSec, dangerAfterSec))
</script>

<span class="age-heat level-{level}">{relative(ts)}</span>

<style>
  .age-heat {
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    white-space: nowrap;
  }

  .level-neutral {
    color: var(--fg-secondary);
  }

  .level-warn {
    color: var(--sig-warn);
  }

  .level-danger {
    color: var(--sig-danger);
  }
</style>
