<script lang="ts">
  let {
    completed,
    failed,
    period,
    labels,
  }: {
    completed: number[]
    failed: number[]
    period: '1h' | '24h' | '7d' | '30d'
    labels?: string[]
  } = $props()

  const width = 760
  const height = 260
  const left = 44
  const right = 16
  const top = 18
  const bottom = 46
  let count = $derived(Math.max(completed.length, failed.length))
  let values = $derived(Array.from({ length: count }, (_, index) => ({
    completed: completed[index] ?? 0,
    failed: failed[index] ?? 0,
    label: labels?.[index] ?? bucketLabel(index, count, period),
  })))
  let maxValue = $derived(Math.max(0, ...values.map(v => Math.max(v.completed, v.failed))))
  let yMax = $derived(niceMax(maxValue))
  let ticks = $derived(niceTicks(yMax))
  let empty = $derived(count === 0 || values.every(v => v.completed === 0 && v.failed === 0))
  // Explicit component options: combining dateStyle/timeStyle shortcuts with
  // timeZoneName is illegal per ECMA-402 and throws TypeError in V8/Firefox.
  let updated = $derived(new Intl.DateTimeFormat(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZoneName: 'short' }).format(new Date()))

  function niceMax(value: number): number {
    if (value <= 0) return 1
    const pow = Math.pow(10, Math.floor(Math.log10(value)))
    const norm = value / pow
    const nice = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10
    return nice * pow
  }

  function niceTicks(max: number): number[] {
    const step = niceMax(max / 4)
    const result: number[] = []
    for (let n = 0; n <= max; n += step) result.push(n)
    if (result[result.length - 1] !== max) result.push(max)
    return result
  }

  function bucketLabel(index: number, total: number, p: '1h' | '24h' | '7d' | '30d'): string {
    const now = Date.now()
    const span = p === '1h' ? 3600000 : p === '24h' ? 86400000 : p === '7d' ? 604800000 : 2592000000
    const date = new Date(now - span + (total <= 1 ? span : (index / (total - 1)) * span))
    return new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZoneName: 'short' }).format(date)
  }

  function x(index: number): number {
    if (count <= 1) return left + (width - left - right) / 2
    return left + (index / (count - 1)) * (width - left - right)
  }

  function y(value: number): number {
    return height - bottom - (value / yMax) * (height - top - bottom)
  }

  function linePath(key: 'completed' | 'failed'): string {
    if (count === 0) return ''
    if (count === 1) return `M ${left} ${y(values[0][key])} L ${width - right} ${y(values[0][key])}`
    return values.map((value, index) => `${index === 0 ? 'M' : 'L'} ${x(index)} ${y(value[key])}`).join(' ')
  }

  function areaPath(key: 'completed' | 'failed'): string {
    if (count === 0) return ''
    const base = y(0)
    if (count === 1) return `M ${left} ${base} L ${left} ${y(values[0][key])} L ${width - right} ${y(values[0][key])} L ${width - right} ${base} Z`
    return `${linePath(key)} L ${x(count - 1)} ${base} L ${x(0)} ${base} Z`
  }
</script>

<div class="area-chart">
  <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`Completed and failed jobs over ${period}`}>
    {#each ticks as tick}
      <line class="grid" x1={left} x2={width - right} y1={y(tick)} y2={y(tick)} />
      <text class="tick" x={left - 8} y={y(tick)}>{tick}</text>
    {/each}
    {#if empty}
      <line class="empty-rule" x1={left} x2={width - right} y1={height / 2} y2={height / 2} />
      <text class="no-data" x={width / 2} y={height / 2 - 8}>no data</text>
    {:else}
      <path class="completed-area" d={areaPath('completed')} />
      <path class="failed-area" d={areaPath('failed')} />
      <path class="completed-line" d={linePath('completed')} />
      <path class="failed-line" d={linePath('failed')} />
      {#if count === 1}
        <circle class="completed-dot" cx={x(0)} cy={y(values[0].completed)} r="3" />
        <circle class="failed-dot" cx={x(0)} cy={y(values[0].failed)} r="3" />
      {/if}
    {/if}
    {#if values.length > 0}
      <text class="axis-label" x={left} y={height - 14}>{values[0].label}</text>
      <text class="axis-label end" x={width - right} y={height - 14}>{values[values.length - 1].label}</text>
    {/if}
  </svg>
  <div class="legend">
    <span class="legend-item">
      <span class="swatch swatch-completed" aria-hidden="true"></span>
      Completed
    </span>
    <span class="legend-item">
      <span class="swatch swatch-failed" aria-hidden="true"></span>
      Failed
    </span>
  </div>
  <div class="caption">last updated {updated}</div>
  <table class="visually-hidden">
    <caption>Chart data</caption>
    <thead><tr><th>time</th><th>completed</th><th>failed</th></tr></thead>
    <tbody>
      {#each values as row}
        <tr><td>{row.label}</td><td>{row.completed}</td><td>{row.failed}</td></tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .area-chart {
    display: grid;
    gap: var(--sp-2);
    color: var(--fg-secondary);
  }

  svg {
    width: 100%;
    min-height: 220px;
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .grid,
  .empty-rule {
    stroke: var(--hairline);
    stroke-width: 1;
  }

  .tick,
  .axis-label,
  .caption,
  .no-data {
    fill: var(--fg-secondary);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-feature-settings: var(--num);
  }

  .end {
    text-anchor: end;
  }

  .no-data {
    text-anchor: middle;
  }

  .completed-area {
    fill: var(--sig-ok);
    opacity: 0.12;
  }

  .failed-area {
    fill: var(--sig-danger);
    opacity: 0.12;
  }

  .completed-line,
  .failed-line {
    fill: none;
    stroke-width: 2;
  }

  .completed-line,
  .completed-dot {
    stroke: var(--sig-ok);
    fill: var(--sig-ok);
  }

  .failed-line,
  .failed-dot {
    stroke: var(--sig-danger);
    fill: var(--sig-danger);
  }

  /* WCAG 1.4.1: the failed series is also dashed so completed vs failed is
     distinguishable without relying on the green/red color difference. */
  .failed-line {
    stroke-dasharray: 5 3;
  }

  .legend {
    display: flex;
    gap: var(--sp-4);
    justify-content: center;
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    color: var(--fg-secondary);
  }

  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-2);
  }

  /* CSS-drawn line swatches (a bordered span, NOT an <svg>) so the chart keeps a
     single <svg> for `.area-chart svg` selectors, and they don't reuse the
     .completed-line/.failed-line classes the chart paths own. Solid vs dashed
     mirrors the chart so the legend itself carries the non-color cue. */
  .swatch {
    display: inline-block;
    width: 24px;
    height: 0;
  }

  .swatch-completed {
    border-top: 2px solid var(--sig-ok);
  }

  .swatch-failed {
    border-top: 2px dashed var(--sig-danger);
  }

  .caption {
    justify-self: end;
  }
</style>
