<script lang="ts">
  import { onMount, onDestroy } from 'svelte'
  import { Chart, registerables } from 'chart.js'

  Chart.register(...registerables)

  let { completed = [], failed = [], period = '1h' }: {
    completed: { timestamp: Date; value: number }[]
    failed: { timestamp: Date; value: number }[]
    period?: string
  } = $props()

  let canvas: HTMLCanvasElement
  let chart: Chart | null = null

  function buildChart() {
    if (chart) chart.destroy()
    if (!canvas) return

    // Merge all timestamps and sort
    const allTimestamps = new Set<number>()
    completed.forEach(d => allTimestamps.add(d.timestamp.getTime()))
    failed.forEach(d => allTimestamps.add(d.timestamp.getTime()))

    const sortedTimes = Array.from(allTimestamps).sort((a, b) => a - b)

    const completedMap = new Map(completed.map(d => [d.timestamp.getTime(), d.value]))
    const failedMap = new Map(failed.map(d => [d.timestamp.getTime(), d.value]))

    const labels = sortedTimes.map(t => {
      const d = new Date(t)
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    })

    chart = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Completed',
            data: sortedTimes.map(t => completedMap.get(t) ?? 0),
            borderColor: '#10b981',
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 3,
            pointHoverRadius: 5,
          },
          {
            label: 'Failed',
            data: sortedTimes.map(t => failedMap.get(t) ?? 0),
            borderColor: '#ef4444',
            backgroundColor: 'rgba(239, 68, 68, 0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 3,
            pointHoverRadius: 5,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          intersect: false,
          mode: 'index',
        },
        plugins: {
          legend: {
            position: 'top',
            labels: {
              usePointStyle: true,
              padding: 16,
            },
          },
          tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            padding: 12,
            cornerRadius: 8,
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            ticks: {
              precision: 0,
            },
            grid: {
              color: 'rgba(0, 0, 0, 0.05)',
            },
          },
          x: {
            grid: {
              display: false,
            },
          },
        },
      },
    })
  }

  $effect(() => {
    // Re-build when data or period changes
    completed; failed; period;
    if (canvas) buildChart()
  })

  onMount(() => {
    buildChart()
  })

  onDestroy(() => {
    if (chart) chart.destroy()
  })
</script>

<div class="chart-container">
  <canvas bind:this={canvas}></canvas>
  {#if completed.length === 0 && failed.length === 0}
    <div class="empty-state">
      <p>No stats data yet. Data appears as jobs complete or fail.</p>
    </div>
  {/if}
</div>

<style>
  .chart-container {
    position: relative;
    background: white;
    border-radius: 8px;
    padding: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    height: 300px;
  }

  .empty-state {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .empty-state p {
    color: #999;
    font-size: 14px;
  }
</style>
