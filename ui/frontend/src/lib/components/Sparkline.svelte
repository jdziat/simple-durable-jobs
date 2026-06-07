<script lang="ts">
  let {
    data,
    color = 'var(--accent)',
    label = 'sparkline',
  }: {
    data: number[]
    color?: string
    label?: string
  } = $props()

  const width = 120
  const height = 32
  const pad = 2
  let max = $derived(Math.max(1, ...data))
  let points = $derived(data.map((value, index) => {
    const x = data.length <= 1 ? width / 2 : pad + (index / (data.length - 1)) * (width - pad * 2)
    const y = height - pad - (value / max) * (height - pad * 2)
    return `${x},${y}`
  }).join(' '))
  let area = $derived(data.length ? `${pad},${height - pad} ${points} ${width - pad},${height - pad}` : '')
</script>

<svg class="sparkline" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={label} style:color={color}>
  {#if data.length === 0}
    <line x1={pad} x2={width - pad} y1={height / 2} y2={height / 2} />
  {:else if data.length === 1}
    <line x1={pad} x2={width - pad} y1={height - pad} y2={height - pad} />
    <circle cx={width / 2} cy={height - pad - (data[0] / max) * (height - pad * 2)} r="2" />
  {:else}
    <polygon points={area} class="spark-area" />
    <polyline points={points} />
  {/if}
</svg>

<style>
  .sparkline {
    display: block;
    width: 120px;
    height: 32px;
    color: var(--accent);
  }

  line,
  polyline {
    fill: none;
    stroke: currentColor;
    stroke-width: 1.5;
  }

  .spark-area {
    fill: currentColor;
    opacity: 0.14;
  }

  circle {
    fill: currentColor;
  }
</style>
