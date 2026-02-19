<script lang="ts">
  import type { Job, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

  // ─── Props ────────────────────────────────────────────────────────────────

  let {
    root,
    fanOuts,
    children,
    onJobClick,
  }: {
    root: Job
    fanOuts: FanOut[]
    children: Job[]
    onJobClick?: (jobId: string) => void
  } = $props()

  // ─── Layout constants ─────────────────────────────────────────────────────

  const ROW_HEIGHT = 48
  const LABEL_WIDTH = 300
  const BAR_HEIGHT = 26
  const CHART_PADDING = 20
  const MIN_CHART_WIDTH = 800

  // ─── Status colours ───────────────────────────────────────────────────────

  const STATUS_COLORS: Record<string, string> = {
    pending: '#f59e0b',
    running: '#3b82f6',
    completed: '#22c55e',
    failed: '#ef4444',
    cancelled: '#9ca3af',
    waiting: '#a855f7',
  }

  const STATUS_BG: Record<string, string> = {
    pending: '#fef3c7',
    running: '#dbeafe',
    completed: '#d1fae5',
    failed: '#fee2e2',
    cancelled: '#f3f4f6',
    waiting: '#f3e8ff',
  }

  function statusColor(status: string): string {
    return STATUS_COLORS[status] ?? '#9ca3af'
  }

  function statusBg(status: string): string {
    return STATUS_BG[status] ?? '#f3f4f6'
  }

  // ─── Row data model ───────────────────────────────────────────────────────

  interface RootRow {
    type: 'root'
    job: Job
    indent: 0
  }

  interface FanOutHeaderRow {
    type: 'fanout-header'
    fanOut: FanOut
    indent: 1
  }

  interface SubjobRow {
    type: 'subjob'
    job: Job
    indent: 1 | 2
  }

  type Row = RootRow | FanOutHeaderRow | SubjobRow

  // ─── Selected job state (detail panel) ──────────────────────────────────

  let selectedJob = $state<Job | null>(null)
  let selectedFanOut = $state<FanOut | null>(null)

  function selectRow(row: Row) {
    if (row.type === 'fanout-header') {
      selectedJob = null
      selectedFanOut = selectedFanOut?.id === row.fanOut.id ? null : row.fanOut
    } else {
      selectedFanOut = null
      const job = row.job
      selectedJob = selectedJob?.id === job.id ? null : job
    }
  }

  // ─── Tooltip state ────────────────────────────────────────────────────────

  interface TooltipData {
    x: number
    y: number
    job: Job
  }

  let tooltip = $state<TooltipData | null>(null)

  // ─── Computed: time range ─────────────────────────────────────────────────

  const timeStart = $derived(root.createdAt?.toDate() ?? new Date())

  const timeEnd = $derived.by(() => {
    const now = new Date()
    let max = now.getTime()

    const rootEnd = root.completedAt?.toDate()?.getTime() ?? now.getTime()
    if (rootEnd > max) max = rootEnd

    for (const job of children) {
      const end = job.completedAt?.toDate()?.getTime() ?? now.getTime()
      if (end > max) max = end
    }

    return new Date(max)
  })

  const timeRange = $derived(Math.max(timeEnd.getTime() - timeStart.getTime(), 1))

  // ─── Computed: tick interval & format ─────────────────────────────────────

  const tickIntervalMs = $derived.by(() => {
    const rangeMin = timeRange / 60_000
    if (rangeMin < 5) return 30_000
    if (rangeMin < 30) return 5 * 60_000
    if (rangeMin < 120) return 15 * 60_000
    return 30 * 60_000
  })

  const tickFormat = $derived(timeRange < 5 * 60_000 ? 'hms' : 'hm')

  function formatTick(date: Date): string {
    const h = String(date.getHours()).padStart(2, '0')
    const m = String(date.getMinutes()).padStart(2, '0')
    if (tickFormat === 'hms') {
      const s = String(date.getSeconds()).padStart(2, '0')
      return `${h}:${m}:${s}`
    }
    return `${h}:${m}`
  }

  const ticks = $derived.by(() => {
    const result: { label: string; pct: number }[] = []
    const start = timeStart.getTime()
    const end = timeEnd.getTime()
    let t = Math.ceil(start / tickIntervalMs) * tickIntervalMs
    while (t <= end) {
      result.push({
        label: formatTick(new Date(t)),
        pct: (t - start) / timeRange,
      })
      t += tickIntervalMs
    }
    return result
  })

  // ─── Computed: sorted fanOuts ─────────────────────────────────────────────

  const sortedFanOuts = $derived(
    [...fanOuts].sort((a, b) => {
      const aT = a.createdAt?.toDate()?.getTime() ?? 0
      const bT = b.createdAt?.toDate()?.getTime() ?? 0
      return aT - bT
    })
  )

  // ─── Computed: children keyed by fanOutId ─────────────────────────────────

  const childrenByFanOut = $derived.by(() => {
    const map = new Map<string, Job[]>()
    for (const job of children) {
      if (job.fanOutId) {
        const bucket = map.get(job.fanOutId) ?? []
        bucket.push(job)
        map.set(job.fanOutId, bucket)
      }
    }
    for (const [key, jobs] of map) {
      map.set(key, [...jobs].sort((a, b) => a.fanOutIndex - b.fanOutIndex))
    }
    return map
  })

  // ─── Computed: rows ───────────────────────────────────────────────────────

  const rows = $derived.by<Row[]>(() => {
    const result: Row[] = [{ type: 'root', job: root, indent: 0 }]

    for (const fo of sortedFanOuts) {
      result.push({ type: 'fanout-header', fanOut: fo, indent: 1 })
      const jobs = childrenByFanOut.get(fo.id) ?? []
      for (const job of jobs) {
        result.push({ type: 'subjob', job, indent: 2 })
      }
    }

    const groupedIds = new Set(children.filter(j => j.fanOutId).map(j => j.id))
    for (const job of children) {
      if (!groupedIds.has(job.id) && !job.fanOutId) {
        result.push({ type: 'subjob', job, indent: 1 })
      }
    }

    return result
  })

  // ─── SVG geometry ─────────────────────────────────────────────────────────

  const TICK_AREA_HEIGHT = 36

  const svgHeight = $derived(TICK_AREA_HEIGHT + rows.length * ROW_HEIGHT + CHART_PADDING)

  // ─── Helpers: time-to-x position ──────────────────────────────────────────

  function timePct(date: Date | undefined | null): number {
    if (!date) return 0
    const t = date.getTime() - timeStart.getTime()
    return Math.max(0, Math.min(1, t / timeRange))
  }

  function barCoords(job: Job): { startPct: number; widthPct: number; isPending: boolean } {
    const now = new Date()
    const created = job.createdAt?.toDate() ?? now
    const started = job.startedAt?.toDate()
    const completed = job.completedAt?.toDate()

    if (!started && !completed) {
      return { startPct: timePct(created), widthPct: 0, isPending: true }
    }

    const barStart = started ?? created
    const barEnd = completed ?? now

    const startPct = timePct(barStart)
    const endPct = timePct(barEnd)
    return {
      startPct,
      widthPct: Math.max(endPct - startPct, 0),
      isPending: false,
    }
  }

  function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
    const m = Math.floor(ms / 60_000)
    const s = Math.floor((ms % 60_000) / 1000)
    return `${m}m ${s}s`
  }

  function jobDurationMs(job: Job): number | null {
    const started = job.startedAt?.toDate()
    if (!started) return null
    const ended = job.completedAt?.toDate() ?? new Date()
    return ended.getTime() - started.getTime()
  }

  function truncateId(id: string, len = 12): string {
    return id.length > len ? id.slice(0, len) + '...' : id
  }

  function formatJson(bytes: Uint8Array | undefined): string {
    if (!bytes || bytes.length === 0) return ''
    try {
      const text = new TextDecoder().decode(bytes)
      return JSON.stringify(JSON.parse(text), null, 2)
    } catch {
      return new TextDecoder().decode(bytes)
    }
  }

  // ─── Row y-coordinate ─────────────────────────────────────────────────────

  function rowY(rowIndex: number): number {
    return TICK_AREA_HEIGHT + rowIndex * ROW_HEIGHT
  }

  function barY(rowIndex: number): number {
    return rowY(rowIndex) + (ROW_HEIGHT - BAR_HEIGHT) / 2
  }

  // ─── Tooltip handlers ─────────────────────────────────────────────────────

  let containerEl = $state<HTMLDivElement | undefined>(undefined)

  function showTooltip(event: MouseEvent, job: Job) {
    const rect = containerEl?.getBoundingClientRect()
    if (!rect) return
    tooltip = {
      x: event.clientX - rect.left + 12,
      y: event.clientY - rect.top - 8,
      job,
    }
  }

  function hideTooltip() {
    tooltip = null
  }

  function handleRowClick(row: Row) {
    selectRow(row)
  }

  function handleJobNavigate(jobId: string) {
    if (onJobClick) onJobClick(jobId)
  }

  // ─── Fan-out connector line coordinates ───────────────────────────────────

  interface FanOutConnector {
    fanOutId: string
    rowStart: number
    rowEnd: number
    minBarStartPct: number
    maxBarEndPct: number
  }

  const fanOutConnectors = $derived.by<FanOutConnector[]>(() => {
    const connectors: FanOutConnector[] = []

    for (const fo of sortedFanOuts) {
      const jobs = childrenByFanOut.get(fo.id) ?? []
      if (jobs.length === 0) continue

      let firstIdx = -1
      let lastIdx = -1
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i]
        if (row.type === 'subjob' && row.job.fanOutId === fo.id) {
          if (firstIdx === -1) firstIdx = i
          lastIdx = i
        }
      }
      if (firstIdx === -1) continue

      let minStart = 1
      let maxEnd = 0
      for (const job of jobs) {
        const { startPct, widthPct } = barCoords(job)
        if (startPct < minStart) minStart = startPct
        if (startPct + widthPct > maxEnd) maxEnd = startPct + widthPct
      }

      connectors.push({
        fanOutId: fo.id,
        rowStart: firstIdx,
        rowEnd: lastIdx,
        minBarStartPct: minStart,
        maxBarEndPct: maxEnd,
      })
    }

    return connectors
  })

  const connectorByFanOut = $derived.by(() => {
    const map = new Map<string, FanOutConnector>()
    for (const c of fanOutConnectors) map.set(c.fanOutId, c)
    return map
  })

  // ─── Summary stats ────────────────────────────────────────────────────────

  const totalJobs = $derived(children.length + 1)
  const completedJobs = $derived(children.filter(j => j.status === 'completed').length + (root.status === 'completed' ? 1 : 0))
  const failedJobs = $derived(children.filter(j => j.status === 'failed').length + (root.status === 'failed' ? 1 : 0))
  const runningJobs = $derived(children.filter(j => j.status === 'running').length + (root.status === 'running' ? 1 : 0))

  const totalDurationMs = $derived.by(() => {
    const start = root.createdAt?.toDate()?.getTime()
    if (!start) return null
    const end = root.completedAt?.toDate()?.getTime() ?? Date.now()
    return end - start
  })
</script>

<div class="waterfall-chart" bind:this={containerEl}>
  <!-- Header with summary stats -->
  <div class="chart-header">
    <div class="header-left">
      <h4>Workflow Timeline</h4>
      <div class="summary-stats">
        <span class="stat">
          <span class="stat-count">{totalJobs}</span> jobs
        </span>
        <span class="stat-sep">|</span>
        <span class="stat stat-completed">
          <span class="stat-dot" style="background: {STATUS_COLORS.completed}"></span>
          {completedJobs}
        </span>
        <span class="stat stat-running">
          <span class="stat-dot" style="background: {STATUS_COLORS.running}"></span>
          {runningJobs}
        </span>
        {#if failedJobs > 0}
          <span class="stat stat-failed">
            <span class="stat-dot" style="background: {STATUS_COLORS.failed}"></span>
            {failedJobs}
          </span>
        {/if}
        {#if totalDurationMs !== null}
          <span class="stat-sep">|</span>
          <span class="stat">{formatDuration(totalDurationMs)}</span>
        {/if}
      </div>
    </div>
    <div class="legend">
      {#each Object.entries(STATUS_COLORS) as [status, color]}
        <div class="legend-item">
          <span class="legend-dot" style="background: {color}"></span>
          <span class="legend-label">{status}</span>
        </div>
      {/each}
    </div>
  </div>

  <!-- Chart area -->
  <div class="chart-container">
    <svg
      width="100%"
      height={svgHeight}
      style="min-width: {MIN_CHART_WIDTH}px; display: block;"
      role="img"
      aria-label="Workflow waterfall timeline"
    >
      <defs>
        <style>
          {`
            @keyframes wf-pulse {
              0%, 100% { opacity: 1; }
              50% { opacity: 0.55; }
            }
            .bar-running {
              animation: wf-pulse 1.4s ease-in-out infinite;
            }
          `}
        </style>
      </defs>

      <!-- ── Time axis ── -->
      <g class="time-axis">
        <line
          x1={LABEL_WIDTH}
          y1={TICK_AREA_HEIGHT - 4}
          x2="100%"
          y2={TICK_AREA_HEIGHT - 4}
          stroke="#e5e7eb"
          stroke-width="1"
        />
        {#each ticks as tick}
          <g>
            <line
              x1="{LABEL_WIDTH + tick.pct * (100 - (LABEL_WIDTH / MIN_CHART_WIDTH * 100))}%"
              y1={TICK_AREA_HEIGHT - 4}
              x2="{LABEL_WIDTH + tick.pct * (100 - (LABEL_WIDTH / MIN_CHART_WIDTH * 100))}%"
              y2={svgHeight}
              stroke="#f3f4f6"
              stroke-width="1"
            />
            <line
              x1="{LABEL_WIDTH + tick.pct * (100 - (LABEL_WIDTH / MIN_CHART_WIDTH * 100))}%"
              y1={TICK_AREA_HEIGHT - 8}
              x2="{LABEL_WIDTH + tick.pct * (100 - (LABEL_WIDTH / MIN_CHART_WIDTH * 100))}%"
              y2={TICK_AREA_HEIGHT - 4}
              stroke="#9ca3af"
              stroke-width="1"
            />
            <text
              x="{LABEL_WIDTH + tick.pct * (100 - (LABEL_WIDTH / MIN_CHART_WIDTH * 100))}%"
              y={TICK_AREA_HEIGHT - 12}
              text-anchor="middle"
              font-size="11"
              fill="#6b7280"
            >{tick.label}</text>
          </g>
        {/each}
      </g>

      <!-- ── Rows ── -->
      {#each rows as row, rowIndex}
        {@const ry = rowY(rowIndex)}
        {@const by = barY(rowIndex)}
        {@const isSelected = (row.type !== 'fanout-header' && selectedJob?.id === row.job.id) || (row.type === 'fanout-header' && selectedFanOut?.id === row.fanOut.id)}

        <!-- Row separator line -->
        <line
          x1="0"
          y1={ry}
          x2="100%"
          y2={ry}
          stroke="#f3f4f6"
          stroke-width="1"
        />

        {#if row.type === 'fanout-header'}
          <!-- Fan-out group header row -->
          <g
            class="row row-fanout-header"
            class:row-selected={isSelected}
            onclick={() => handleRowClick(row)}
            role="button"
            tabindex="0"
            onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') handleRowClick(row) }}
            aria-label="Fan-out: {row.fanOut.strategy} - {row.fanOut.completedCount}/{row.fanOut.totalCount}"
          >
            <rect
              x="0"
              y={ry}
              width="100%"
              height={ROW_HEIGHT}
              fill={isSelected ? '#eef2ff' : '#f9fafb'}
              class="row-bg-fo"
            />
            <!-- Indent line -->
            <line
              x1={row.indent * 16 + 4}
              y1={ry + 8}
              x2={row.indent * 16 + 4}
              y2={ry + ROW_HEIGHT - 8}
              stroke="#d1d5db"
              stroke-width="1.5"
            />
            <!-- Strategy badge -->
            <rect
              x={row.indent * 16 + 14}
              y={ry + (ROW_HEIGHT - 20) / 2}
              width={row.fanOut.strategy.length * 7 + 16}
              height="20"
              rx="4"
              fill="#1a1a2e"
            />
            <text
              x={row.indent * 16 + 14 + 8}
              y={ry + ROW_HEIGHT / 2}
              font-size="11"
              font-weight="600"
              fill="white"
              dominant-baseline="middle"
            >{row.fanOut.strategy || 'fanout'}</text>
            <!-- Progress -->
            <text
              x={row.indent * 16 + 14 + row.fanOut.strategy.length * 7 + 16 + 12}
              y={ry + ROW_HEIGHT / 2 - 1}
              font-size="12"
              fill="#374151"
              dominant-baseline="middle"
              font-weight="500"
            >{row.fanOut.completedCount}/{row.fanOut.totalCount} completed</text>
            {#if row.fanOut.failedCount > 0}
              <text
                x={row.indent * 16 + 14 + row.fanOut.strategy.length * 7 + 16 + 12 + 110}
                y={ry + ROW_HEIGHT / 2 - 1}
                font-size="12"
                fill="#ef4444"
                dominant-baseline="middle"
                font-weight="500"
              >{row.fanOut.failedCount} failed</text>
            {/if}
            <!-- Status badge on right of label area -->
            <rect
              x={LABEL_WIDTH - 60}
              y={ry + (ROW_HEIGHT - 20) / 2}
              width="52"
              height="20"
              rx="4"
              fill={statusBg(row.fanOut.status)}
            />
            <text
              x={LABEL_WIDTH - 34}
              y={ry + ROW_HEIGHT / 2}
              font-size="11"
              fill={statusColor(row.fanOut.status)}
              dominant-baseline="middle"
              text-anchor="middle"
              font-weight="600"
            >{row.fanOut.status}</text>

            <!-- Connector spine -->
            {#if connectorByFanOut.get(row.fanOut.id)}
              {@const conn = connectorByFanOut.get(row.fanOut.id)!}
              {@const connX = `${LABEL_WIDTH + conn.minBarStartPct * (MIN_CHART_WIDTH - LABEL_WIDTH) - 6}px`}
              <line
                x1={connX}
                y1={ry + ROW_HEIGHT}
                x2={connX}
                y2={rowY(conn.rowEnd) + ROW_HEIGHT / 2}
                stroke="#c7d2fe"
                stroke-width="2"
                stroke-dasharray="4 3"
              />
            {/if}
          </g>

        {:else}
          <!-- Job row (root or subjob) -->
          {@const job = row.job}
          {@const coords = barCoords(job)}
          {@const color = statusColor(job.status)}
          {@const isRunning = job.status === 'running'}
          {@const dur = jobDurationMs(job)}

          <g
            class="row row-job"
            class:row-clickable={true}
            class:row-selected={isSelected}
            onclick={() => handleRowClick(row)}
            onmousemove={(e) => showTooltip(e, job)}
            onmouseleave={hideTooltip}
            role="button"
            tabindex="0"
            onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') handleRowClick(row) }}
            aria-label={`${job.type} - ${job.status}`}
          >
            <!-- Row background -->
            <rect
              x="0"
              y={ry}
              width="100%"
              height={ROW_HEIGHT}
              fill={isSelected ? '#eef2ff' : (rowIndex % 2 === 0 ? 'white' : '#fafbfc')}
              class="row-bg"
            />

            <!-- Indent connector dots for sub-jobs -->
            {#if row.indent > 0}
              <circle
                cx={row.indent * 16 + 4}
                cy={ry + ROW_HEIGHT / 2}
                r="3"
                fill="#d1d5db"
              />
            {/if}

            <!-- Status dot -->
            <circle
              cx={row.indent * 16 + 18}
              cy={ry + ROW_HEIGHT / 2 - 6}
              r="4"
              fill={color}
            />

            <!-- Job type -->
            <text
              x={row.indent * 16 + 28}
              y={ry + ROW_HEIGHT / 2 - 6}
              font-size="13"
              fill="#111827"
              dominant-baseline="middle"
              font-weight={row.type === 'root' ? '700' : '500'}
            >{job.type}</text>

            <!-- Second line: ID + queue + duration -->
            <text
              x={row.indent * 16 + 28}
              y={ry + ROW_HEIGHT / 2 + 10}
              font-size="11"
              fill="#9ca3af"
              dominant-baseline="middle"
              font-family="ui-monospace, 'Cascadia Code', Menlo, Consolas, monospace"
            >{truncateId(job.id)}</text>

            <!-- Queue badge -->
            <rect
              x={row.indent * 16 + 28 + truncateId(job.id).length * 6.5 + 8}
              y={ry + ROW_HEIGHT / 2 + 3}
              width={job.queue.length * 6.5 + 10}
              height="15"
              rx="3"
              fill="#f3f4f6"
            />
            <text
              x={row.indent * 16 + 28 + truncateId(job.id).length * 6.5 + 8 + 5}
              y={ry + ROW_HEIGHT / 2 + 10.5}
              font-size="10"
              fill="#6b7280"
              dominant-baseline="middle"
            >{job.queue}</text>

            <!-- Duration on right of label area -->
            {#if dur !== null}
              <text
                x={LABEL_WIDTH - 8}
                y={ry + ROW_HEIGHT / 2 - 6}
                font-size="11"
                fill="#6b7280"
                dominant-baseline="middle"
                text-anchor="end"
                font-weight="500"
              >{formatDuration(dur)}</text>
            {/if}

            <!-- Attempt info on right of label area -->
            <text
              x={LABEL_WIDTH - 8}
              y={ry + ROW_HEIGHT / 2 + 10}
              font-size="10"
              fill="#9ca3af"
              dominant-baseline="middle"
              text-anchor="end"
            >{job.attempt}/{job.maxRetries}</text>

            <!-- Bar area -->
            {#if coords.isPending}
              <!-- Pending: diamond marker -->
              <g transform="translate({LABEL_WIDTH + coords.startPct * (MIN_CHART_WIDTH - LABEL_WIDTH)}, {ry + ROW_HEIGHT / 2})">
                <rect
                  x="-5"
                  y="-5"
                  width="10"
                  height="10"
                  rx="2"
                  fill={color}
                  transform="rotate(45)"
                />
              </g>
            {:else}
              <!-- Full bar with rounded ends -->
              <rect
                x="{LABEL_WIDTH + coords.startPct * (MIN_CHART_WIDTH - LABEL_WIDTH)}px"
                y={by}
                width="{Math.max(coords.widthPct * (MIN_CHART_WIDTH - LABEL_WIDTH), 6)}px"
                height={BAR_HEIGHT}
                rx="5"
                fill={color}
                fill-opacity={isRunning ? 1 : 0.85}
                class:bar-running={isRunning}
              />
              <!-- Duration label inside bar -->
              {#if coords.widthPct * (MIN_CHART_WIDTH - LABEL_WIDTH) > 55 && dur !== null}
                <text
                  x="{LABEL_WIDTH + coords.startPct * (MIN_CHART_WIDTH - LABEL_WIDTH) + 8}px"
                  y={by + BAR_HEIGHT / 2}
                  font-size="11"
                  fill="white"
                  dominant-baseline="middle"
                  pointer-events="none"
                  font-weight="500"
                >{formatDuration(dur)}</text>
              {/if}
            {/if}

            <!-- Hit target -->
            <rect
              x="0"
              y={ry}
              width="100%"
              height={ROW_HEIGHT}
              fill="transparent"
              class="hit-target"
            />
          </g>
        {/if}
      {/each}
    </svg>
  </div>

  <!-- Detail panel for selected job -->
  {#if selectedJob}
    {@const job = selectedJob}
    {@const dur = jobDurationMs(job)}
    {@const argsStr = formatJson(job.args)}
    {@const resultStr = formatJson(job.result)}
    <div class="detail-panel">
      <div class="detail-header">
        <div class="detail-title-row">
          <span class="detail-status-dot" style="background: {statusColor(job.status)}"></span>
          <h5>{job.type}</h5>
          <span class="detail-status-badge" style="background: {statusBg(job.status)}; color: {statusColor(job.status)}">{job.status}</span>
        </div>
        <div class="detail-actions">
          {#if onJobClick}
            <button class="btn-view" onclick={() => handleJobNavigate(job.id)}>View Full Detail</button>
          {/if}
          <button class="btn-close" onclick={() => selectedJob = null} aria-label="Close">&times;</button>
        </div>
      </div>

      <div class="detail-grid">
        <div class="detail-section">
          <h6>Identifiers</h6>
          <div class="detail-field">
            <span class="detail-label">Job ID</span>
            <span class="detail-value mono">{job.id}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Queue</span>
            <span class="detail-value">{job.queue}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Priority</span>
            <span class="detail-value">{job.priority}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Attempts</span>
            <span class="detail-value">{job.attempt}/{job.maxRetries}</span>
          </div>
          {#if job.fanOutId}
            <div class="detail-field">
              <span class="detail-label">Fan-Out ID</span>
              <span class="detail-value mono">{truncateId(job.fanOutId, 16)}</span>
            </div>
            <div class="detail-field">
              <span class="detail-label">Fan-Out Index</span>
              <span class="detail-value">{job.fanOutIndex}</span>
            </div>
          {/if}
          {#if job.parentJobId}
            <div class="detail-field">
              <span class="detail-label">Parent Job</span>
              <span class="detail-value mono">{truncateId(job.parentJobId, 16)}</span>
            </div>
          {/if}
          {#if job.rootJobId}
            <div class="detail-field">
              <span class="detail-label">Root Job</span>
              <span class="detail-value mono">{truncateId(job.rootJobId, 16)}</span>
            </div>
          {/if}
        </div>

        <div class="detail-section">
          <h6>Timing</h6>
          <div class="detail-field">
            <span class="detail-label">Created</span>
            <span class="detail-value">{job.createdAt?.toDate().toLocaleString() ?? '-'}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Started</span>
            <span class="detail-value">{job.startedAt?.toDate().toLocaleString() ?? '-'}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Completed</span>
            <span class="detail-value">{job.completedAt?.toDate().toLocaleString() ?? '-'}</span>
          </div>
          {#if dur !== null}
            <div class="detail-field">
              <span class="detail-label">Duration</span>
              <span class="detail-value highlight">{formatDuration(dur)}</span>
            </div>
          {/if}
        </div>

        {#if argsStr}
          <div class="detail-section detail-section-wide">
            <h6>Arguments</h6>
            <pre class="detail-pre">{argsStr}</pre>
          </div>
        {/if}

        {#if resultStr}
          <div class="detail-section detail-section-wide">
            <h6>Result</h6>
            <pre class="detail-pre">{resultStr}</pre>
          </div>
        {/if}

        {#if job.lastError}
          <div class="detail-section detail-section-wide">
            <h6>Error</h6>
            <pre class="detail-pre detail-error">{job.lastError}</pre>
          </div>
        {/if}
      </div>
    </div>
  {/if}

  <!-- Detail panel for selected fan-out -->
  {#if selectedFanOut}
    {@const fo = selectedFanOut}
    <div class="detail-panel">
      <div class="detail-header">
        <div class="detail-title-row">
          <span class="detail-status-dot" style="background: {statusColor(fo.status)}"></span>
          <h5>Fan-Out: {fo.strategy}</h5>
          <span class="detail-status-badge" style="background: {statusBg(fo.status)}; color: {statusColor(fo.status)}">{fo.status}</span>
        </div>
        <div class="detail-actions">
          <button class="btn-close" onclick={() => selectedFanOut = null} aria-label="Close">&times;</button>
        </div>
      </div>

      <div class="detail-grid">
        <div class="detail-section">
          <h6>Configuration</h6>
          <div class="detail-field">
            <span class="detail-label">Fan-Out ID</span>
            <span class="detail-value mono">{fo.id}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Parent Job</span>
            <span class="detail-value mono">{fo.parentJobId}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Strategy</span>
            <span class="detail-value">{fo.strategy}</span>
          </div>
          {#if fo.threshold > 0}
            <div class="detail-field">
              <span class="detail-label">Threshold</span>
              <span class="detail-value">{(fo.threshold * 100).toFixed(0)}%</span>
            </div>
          {/if}
          <div class="detail-field">
            <span class="detail-label">Cancel on Fail</span>
            <span class="detail-value">{fo.cancelOnFail ? 'Yes' : 'No'}</span>
          </div>
          {#if fo.timeoutAt?.toDate()}
            <div class="detail-field">
              <span class="detail-label">Timeout</span>
              <span class="detail-value">{fo.timeoutAt?.toDate().toLocaleString()}</span>
            </div>
          {/if}
        </div>

        <div class="detail-section">
          <h6>Progress</h6>
          <div class="detail-field">
            <span class="detail-label">Total</span>
            <span class="detail-value">{fo.totalCount}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Completed</span>
            <span class="detail-value" style="color: {STATUS_COLORS.completed}">{fo.completedCount}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Failed</span>
            <span class="detail-value" style="color: {STATUS_COLORS.failed}">{fo.failedCount}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Cancelled</span>
            <span class="detail-value">{fo.cancelledCount}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Created</span>
            <span class="detail-value">{fo.createdAt?.toDate().toLocaleString() ?? '-'}</span>
          </div>
          <div class="detail-field">
            <span class="detail-label">Updated</span>
            <span class="detail-value">{fo.updatedAt?.toDate().toLocaleString() ?? '-'}</span>
          </div>
        </div>
      </div>
    </div>
  {/if}

  <!-- Tooltip -->
  {#if tooltip && !selectedJob}
    {@const dur = jobDurationMs(tooltip.job)}
    <div
      class="tooltip"
      style="left: {tooltip.x}px; top: {tooltip.y}px"
      role="tooltip"
    >
      <div class="tt-type">{tooltip.job.type}</div>
      <div class="tt-row">
        <span class="tt-label">ID</span>
        <span class="tt-value tt-mono">{tooltip.job.id}</span>
      </div>
      <div class="tt-row">
        <span class="tt-label">Status</span>
        <span class="tt-value" style="color: {statusColor(tooltip.job.status)}">{tooltip.job.status}</span>
      </div>
      <div class="tt-row">
        <span class="tt-label">Queue</span>
        <span class="tt-value">{tooltip.job.queue}</span>
      </div>
      <div class="tt-row">
        <span class="tt-label">Attempt</span>
        <span class="tt-value">{tooltip.job.attempt}/{tooltip.job.maxRetries}</span>
      </div>
      {#if dur !== null}
        <div class="tt-row">
          <span class="tt-label">Duration</span>
          <span class="tt-value">{formatDuration(dur)}</span>
        </div>
      {/if}
      {#if tooltip.job.lastError}
        <div class="tt-error">{tooltip.job.lastError}</div>
      {/if}
      <div class="tt-hint">Click to view details</div>
    </div>
  {/if}
</div>

<style>
  .waterfall-chart {
    background: white;
    border-radius: 8px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    overflow: hidden;
    position: relative;
  }

  /* ── Header ── */

  .chart-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    padding: 16px 20px 14px;
    border-bottom: 1px solid #e5e7eb;
    gap: 16px;
    flex-wrap: wrap;
  }

  .header-left {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .chart-header h4 {
    margin: 0;
    font-size: 15px;
    font-weight: 600;
    color: #111827;
  }

  .summary-stats {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    color: #6b7280;
  }

  .stat {
    display: flex;
    align-items: center;
    gap: 4px;
  }

  .stat-count {
    font-weight: 600;
    color: #374151;
  }

  .stat-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .stat-sep {
    color: #d1d5db;
  }

  .stat-failed {
    color: #ef4444;
    font-weight: 500;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
  }

  .legend-item {
    display: flex;
    align-items: center;
    gap: 5px;
  }

  .legend-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .legend-label {
    font-size: 12px;
    color: #6b7280;
    text-transform: capitalize;
  }

  /* ── Chart container ── */

  .chart-container {
    overflow-x: auto;
    overflow-y: visible;
    -webkit-overflow-scrolling: touch;
  }

  /* ── Row interactions ── */

  .row-job .row-bg,
  .row-fanout-header .row-bg-fo {
    transition: fill 0.1s;
  }

  .row-clickable,
  .row-fanout-header {
    cursor: pointer;
  }

  .row-clickable:hover .row-bg {
    fill: #f0f9ff !important;
  }

  .row-fanout-header:hover .row-bg-fo {
    fill: #eef2ff !important;
  }

  .row-selected .row-bg {
    fill: #eef2ff !important;
  }

  .row-clickable:focus,
  .row-fanout-header:focus {
    outline: none;
  }

  .row-clickable:focus-visible .row-bg {
    fill: #e0f2fe !important;
  }

  /* ── Detail panel ── */

  .detail-panel {
    border-top: 2px solid #4f46e5;
    padding: 20px;
    background: #fafbff;
  }

  .detail-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
  }

  .detail-title-row {
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .detail-status-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .detail-title-row h5 {
    margin: 0;
    font-size: 16px;
    font-weight: 600;
    color: #111827;
  }

  .detail-status-badge {
    padding: 3px 10px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
  }

  .detail-actions {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .btn-view {
    padding: 6px 14px;
    border: none;
    border-radius: 5px;
    background: #4f46e5;
    color: white;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s;
  }

  .btn-view:hover {
    background: #4338ca;
  }

  .btn-close {
    width: 28px;
    height: 28px;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: #9ca3af;
    font-size: 20px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
  }

  .btn-close:hover {
    background: #f3f4f6;
    color: #374151;
  }

  .detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }

  .detail-section {
    background: white;
    border-radius: 6px;
    padding: 14px;
    border: 1px solid #e5e7eb;
  }

  .detail-section-wide {
    grid-column: 1 / -1;
  }

  .detail-section h6 {
    margin: 0 0 10px;
    font-size: 11px;
    font-weight: 600;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .detail-field {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 3px 0;
    gap: 12px;
  }

  .detail-label {
    font-size: 13px;
    color: #6b7280;
    flex-shrink: 0;
  }

  .detail-value {
    font-size: 13px;
    color: #111827;
    font-weight: 500;
    text-align: right;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .detail-value.mono {
    font-family: ui-monospace, 'Cascadia Code', Menlo, Consolas, monospace;
    font-size: 12px;
  }

  .detail-value.highlight {
    color: #4f46e5;
    font-weight: 600;
  }

  .detail-pre {
    background: #f8fafc;
    border: 1px solid #e5e7eb;
    border-radius: 4px;
    padding: 10px 12px;
    font-size: 12px;
    font-family: ui-monospace, 'Cascadia Code', Menlo, Consolas, monospace;
    overflow-x: auto;
    margin: 0;
    color: #374151;
    line-height: 1.6;
    max-height: 200px;
    overflow-y: auto;
  }

  .detail-error {
    background: #fef2f2;
    border-color: #fecaca;
    color: #991b1b;
  }

  /* ── Tooltip ── */

  .tooltip {
    position: absolute;
    z-index: 50;
    background: rgba(15, 23, 42, 0.96);
    color: #f8fafc;
    border-radius: 6px;
    padding: 10px 12px;
    font-size: 12px;
    line-height: 1.5;
    pointer-events: none;
    min-width: 180px;
    max-width: 280px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.25);
  }

  .tt-type {
    font-weight: 600;
    font-size: 13px;
    margin-bottom: 6px;
    color: #e2e8f0;
  }

  .tt-row {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    padding: 1px 0;
  }

  .tt-label {
    color: #94a3b8;
    flex-shrink: 0;
  }

  .tt-value {
    color: #f1f5f9;
    text-align: right;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .tt-mono {
    font-family: ui-monospace, 'Cascadia Code', Menlo, Consolas, monospace;
    font-size: 11px;
  }

  .tt-error {
    margin-top: 6px;
    padding-top: 6px;
    border-top: 1px solid rgba(255, 255, 255, 0.1);
    color: #fca5a5;
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .tt-hint {
    margin-top: 6px;
    padding-top: 4px;
    border-top: 1px solid rgba(255, 255, 255, 0.08);
    color: #64748b;
    font-size: 11px;
    font-style: italic;
  }
</style>
