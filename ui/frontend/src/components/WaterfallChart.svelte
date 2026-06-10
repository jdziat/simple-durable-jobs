<script lang="ts">
  import { onDestroy } from 'svelte'
  import StatusBadge from '../lib/components/StatusBadge.svelte'
  import { absolute } from '../lib/time'
  import { statusToken } from '../lib/status'
  import type { Job, FanOut } from '../lib/gen/jobs/v1/jobs_pb'

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

  const ROW_HEIGHT = 48
  const BAR_HEIGHT = 24
  const AXIS_HEIGHT = 36
  const MIN_TIMELINE_WIDTH = 640
  const LABEL_WIDTH_MIN = 420
  const LABEL_WIDTH_MAX = 520
  const LABEL_WIDTH_VW = 32
  const MOBILE_LABEL_WIDTH_MIN = 300
  const MOBILE_LABEL_WIDTH_MAX = 360
  const MOBILE_LABEL_WIDTH_VW = 42
  const MOBILE_BREAKPOINT = 720
  const TIMELINE_LEFT_INSET = 6
  const RIGHT_GUTTER = 8
  const MIN_BAR_WIDTH = 14
  const ONGOING_BAR_MIN_WIDTH = 72
  const ONGOING_BAR_MAX_WIDTH = 160
  const OUTLIER_RATIO = 8
  const OUTLIER_MIN_DELTA_MS = 60_000

  type RootRow = { type: 'root'; job: Job; indent: 0 }
  type FanOutHeaderRow = { type: 'fanout-header'; fanOut: FanOut; indent: 1 }
  type SubjobRow = { type: 'subjob'; job: Job; indent: 1 | 2 }
  type Row = RootRow | FanOutHeaderRow | SubjobRow
  type BarCoords = { x: number; width: number; pending: boolean; ongoing: boolean; naturalEndX: number }

  let selectedJob = $state<Job | null>(null)
  let selectedFanOut = $state<FanOut | null>(null)
  let focusedJob = $state<Job | null>(null)
  let resolvingIds = $state<Set<string>>(new Set())
  let scrollEl = $state<HTMLDivElement | null>(null)
  let labelWidth = $state(LABEL_WIDTH_MIN)
  let timelineWidth = $state(MIN_TIMELINE_WIDTH)
  let timelineInnerWidth = $derived(Math.max(1, timelineWidth - TIMELINE_LEFT_INSET - RIGHT_GUTTER))
  let timelinePlotEnd = $derived(TIMELINE_LEFT_INSET + timelineInnerWidth)

  let previousStatuses = new Map<string, string>()
  let resolveTimer: ReturnType<typeof setTimeout> | null = null
  let resizeObserver: ResizeObserver | null = null

  $effect(() => {
    const nextResolved = new Set<string>()
    for (const job of [root, ...children]) {
      const previous = previousStatuses.get(job.id)
      if (previous && previous !== 'completed' && job.status === 'completed') {
        nextResolved.add(job.id)
      }
      previousStatuses.set(job.id, job.status)
    }

    if (nextResolved.size > 0) {
      resolvingIds = nextResolved
      if (resolveTimer) clearTimeout(resolveTimer)
      resolveTimer = setTimeout(() => {
        resolvingIds = new Set()
        resolveTimer = null
      }, 220)
    }
  })

  $effect(() => {
    if (!scrollEl) return
    resizeObserver?.disconnect()
    resizeObserver = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width ?? 0
      const nextLabelWidth = responsiveLabelWidth(width)
      labelWidth = nextLabelWidth
      timelineWidth = Math.max(MIN_TIMELINE_WIDTH, Math.floor(width - nextLabelWidth))
    })
    resizeObserver.observe(scrollEl)

    return () => {
      resizeObserver?.disconnect()
      resizeObserver = null
    }
  })

  onDestroy(() => {
    if (resolveTimer) clearTimeout(resolveTimer)
    resizeObserver?.disconnect()
  })

  const sortedFanOuts = $derived(
    [...fanOuts].sort((a, b) => {
      const aT = a.createdAt?.toDate()?.getTime() ?? 0
      const bT = b.createdAt?.toDate()?.getTime() ?? 0
      return aT - bT
    })
  )

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

  const rows = $derived.by<Row[]>(() => {
    const result: Row[] = [{ type: 'root', job: root, indent: 0 }]

    for (const fanOut of sortedFanOuts) {
      result.push({ type: 'fanout-header', fanOut, indent: 1 })
      const jobs = childrenByFanOut.get(fanOut.id) ?? []
      for (const job of jobs) {
        result.push({ type: 'subjob', job, indent: 2 })
      }
    }

    for (const job of children) {
      if (!job.fanOutId) {
        result.push({ type: 'subjob', job, indent: 1 })
      }
    }

    return result
  })

  const timeDomain = $derived.by(() => {
    const now = Date.now()
    let fullMin = root.createdAt?.toDate()?.getTime() ?? now
    let fullMax = fullMin
    let finiteMin = Number.POSITIVE_INFINITY
    let finiteMax = Number.NEGATIVE_INFINITY
    let hasFiniteWork = false
    let longestOpenEnded = 0

    for (const job of [root, ...children]) {
      const created = job.createdAt?.toDate()?.getTime()
      const started = job.startedAt?.toDate()?.getTime()
      const completed = job.completedAt?.toDate()?.getTime()
      const start = started ?? created
      const end = completed ?? started ?? created

      if (start) fullMin = Math.min(fullMin, start)
      if (created) fullMin = Math.min(fullMin, created)
      fullMax = Math.max(fullMax, completed ?? (started ? now : (created ?? now)), end ?? now)

      if (start && completed && completed >= start) {
        hasFiniteWork = true
        finiteMin = Math.min(finiteMin, start)
        finiteMax = Math.max(finiteMax, completed)
      }

      if (started && !completed) {
        longestOpenEnded = Math.max(longestOpenEnded, now - started)
      }
    }

    const fullRange = Math.max(fullMax - fullMin, 1)
    const finiteRange = hasFiniteWork ? Math.max(finiteMax - finiteMin, 1) : fullRange
    const shouldFocusFiniteWork =
      hasFiniteWork &&
      longestOpenEnded > finiteRange * OUTLIER_RATIO &&
      longestOpenEnded - finiteRange > OUTLIER_MIN_DELTA_MS

    if (shouldFocusFiniteWork) {
      const padding = Math.max(1000, finiteRange * 0.12)
      return { start: finiteMin - padding, end: finiteMax + padding, focused: true }
    }

    return { start: fullMin, end: fullMax, focused: false }
  })
  const timeStart = $derived(new Date(timeDomain.start))
  const timeEnd = $derived(new Date(timeDomain.end))
  const timeRange = $derived(Math.max(timeEnd.getTime() - timeStart.getTime(), 1))
  const minimapHeight = $derived(Math.max(22, Math.min(48, rows.filter(row => row.type !== 'fanout-header').length * 4 + 8)))

  const ticks = $derived.by(() => {
    const rangeMin = timeRange / 60_000
    const interval = rangeMin < 5 ? 30_000 : rangeMin < 30 ? 5 * 60_000 : rangeMin < 120 ? 15 * 60_000 : 30 * 60_000
    const result: { label: string; x: number }[] = []
    const start = timeStart.getTime()
    const end = timeEnd.getTime()
    let t = Math.ceil(start / interval) * interval
    while (t <= end) {
      result.push({ label: formatTick(new Date(t)), x: xForTime(t) })
      t += interval
    }
    return result
  })

  const connectorByFanOut = $derived.by(() => {
    const map = new Map<string, number>()
    for (const fanOut of sortedFanOuts) {
      const jobs = childrenByFanOut.get(fanOut.id) ?? []
      const starts = jobs.map(job => barCoords(job).x).filter(x => Number.isFinite(x))
      if (starts.length > 0) map.set(fanOut.id, Math.max(TIMELINE_LEFT_INSET, Math.min(...starts) - 8))
    }
    return map
  })

  const totalJobs = $derived(children.length + 1)
  const completedJobs = $derived(children.filter(j => j.status === 'completed').length + (root.status === 'completed' ? 1 : 0))
  const failedJobs = $derived(children.filter(j => j.status === 'failed').length + (root.status === 'failed' ? 1 : 0))
  const runningJobs = $derived(children.filter(j => j.status === 'running').length + (root.status === 'running' ? 1 : 0))
  const totalDurationMs = $derived.by(() => {
    const start = root.createdAt?.toDate()?.getTime()
    if (!start) return null
    return (root.completedAt?.toDate()?.getTime() ?? Date.now()) - start
  })

  function formatTick(date: Date): string {
    const h = String(date.getHours()).padStart(2, '0')
    const m = String(date.getMinutes()).padStart(2, '0')
    const s = String(date.getSeconds()).padStart(2, '0')
    return timeRange < 5 * 60_000 ? `${h}:${m}:${s}` : `${h}:${m}`
  }

  function xForTime(time: number): number {
    const offset = ((time - timeStart.getTime()) / timeRange) * timelineInnerWidth
    return TIMELINE_LEFT_INSET + Math.max(0, Math.min(timelineInnerWidth, offset))
  }

  function effectiveEndForJob(job: Job): number {
    const now = Date.now()
    const completed = job.completedAt?.toDate()?.getTime()
    if (completed) return completed

    const started = job.startedAt?.toDate()?.getTime()
    if (started) return now

    return job.createdAt?.toDate()?.getTime() ?? now
  }

  function barCoords(job: Job): BarCoords {
    const now = Date.now()
    const created = job.createdAt?.toDate()?.getTime() ?? now
    const started = job.startedAt?.toDate()?.getTime()
    const completed = job.completedAt?.toDate()?.getTime()

    if (!started && !completed) {
      const x = xForTime(created)
      return { x, width: 0, pending: true, ongoing: false, naturalEndX: x }
    }

    const start = started ?? created
    const end = completed ?? effectiveEndForJob(job)
    const x = xForTime(start)
    const naturalEndX = xForTime(end)
    const naturalWidth = Math.max(MIN_BAR_WIDTH, naturalEndX - x)

    if (started && !completed) {
      const capWidth = clamp(timelineInnerWidth * 0.16, ONGOING_BAR_MIN_WIDTH, ONGOING_BAR_MAX_WIDTH)
      const width = Math.min(naturalWidth, capWidth, Math.max(MIN_BAR_WIDTH, timelinePlotEnd - x))
      return { x, width: Math.max(MIN_BAR_WIDTH, width), pending: false, ongoing: true, naturalEndX }
    }

    return {
      x,
      width: naturalWidth,
      pending: false,
      ongoing: false,
      naturalEndX,
    }
  }

  function roleClass(status: string): string {
    return `role-${statusToken(status).role}`
  }

  function statusSlug(status: string): string {
    return (status || 'unknown').trim().toLowerCase().replace(/\s+/g, '-')
  }

  function formatDuration(ms: number): string {
    if (!Number.isFinite(ms) || ms < 0) return '-'
    if (ms < 1000) return `${Math.round(ms)}ms`
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
    const m = Math.floor(ms / 60_000)
    const s = Math.floor((ms % 60_000) / 1000)
    return `${m}m ${s}s`
  }

  function jobDurationMs(job: Job): number | null {
    const started = job.startedAt?.toDate()
    if (!started || Number.isNaN(started.getTime())) return null
    const ended = job.completedAt?.toDate() ?? new Date()
    if (Number.isNaN(ended.getTime())) return null
    return Math.max(0, ended.getTime() - started.getTime())
  }

  function truncateId(id: string, len = 12): string {
    return id.length > len ? `${id.slice(0, len)}...` : id
  }

  function responsiveLabelWidth(containerWidth: number): number {
    if (containerWidth <= 0) return LABEL_WIDTH_MIN

    if (containerWidth <= MOBILE_BREAKPOINT) {
      return clamp(containerWidth * (MOBILE_LABEL_WIDTH_VW / 100), MOBILE_LABEL_WIDTH_MIN, MOBILE_LABEL_WIDTH_MAX)
    }

    return clamp(containerWidth * (LABEL_WIDTH_VW / 100), LABEL_WIDTH_MIN, LABEL_WIDTH_MAX)
  }

  function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value))
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

  function formatTime(value: { toDate?: () => Date } | undefined): string {
    if (!value?.toDate) return '-'
    const date = value.toDate()
    if (Number.isNaN(date.getTime())) return '-'
    return absolute(date)
  }

  function selectRow(row: Row) {
    if (row.type === 'fanout-header') {
      selectedJob = null
      selectedFanOut = selectedFanOut?.id === row.fanOut.id ? null : row.fanOut
    } else {
      selectedFanOut = null
      selectedJob = selectedJob?.id === row.job.id ? null : row.job
    }
  }

  function handleKey(event: KeyboardEvent, row: Row) {
    if (event.key !== 'Enter' && event.key !== ' ') return
    event.preventDefault()
    selectRow(row)
  }

  function navigateJob(event: MouseEvent, jobId: string) {
    if (!onJobClick) return
    event.preventDefault()
    event.stopPropagation()
    onJobClick(jobId)
  }

  function minimapY(rowIndex: number): number {
    const plottableRows = rows.filter(row => row.type !== 'fanout-header').length
    if (plottableRows <= 1) return Math.max(7, minimapHeight / 2 - 2)
    const step = (minimapHeight - 10) / Math.max(1, plottableRows - 1)
    let index = 0
    for (const row of rows.slice(0, rowIndex)) {
      if (row.type !== 'fanout-header') index += 1
    }
    return 5 + index * step
  }
</script>

<div class="waterfall-chart">
  <div class="chart-header">
    <div class="header-left">
      <h4>Workflow Timeline</h4>
      <div class="summary-stats">
        <span class="stat"><span class="stat-count">{totalJobs}</span> jobs</span>
        <span class="stat-sep">|</span>
        <span class="stat stat-completed"><span class="stat-dot role-ok"></span><span class="stat-count">{completedJobs}</span> completed</span>
        <span class="stat stat-running"><span class="stat-dot role-info"></span><span class="stat-count">{runningJobs}</span> running</span>
        {#if failedJobs > 0}
          <span class="stat stat-failed"><span class="stat-dot role-danger"></span><span class="stat-count">{failedJobs}</span> failed</span>
        {/if}
        {#if totalDurationMs !== null}
          <span class="stat-sep">|</span>
          <span class="stat">{formatDuration(totalDurationMs)}</span>
        {/if}
      </div>
    </div>
    <div class="legend">
      {#each ['pending', 'running', 'completed', 'failed', 'cancelled', 'waiting'] as status}
        <div class="legend-item">
          <span class={`legend-dot ${roleClass(status)}`}></span>
          <span class="legend-label">{status}</span>
        </div>
      {/each}
    </div>
  </div>

  <div class="minimap" aria-hidden="true">
    <svg width="100%" height={minimapHeight} viewBox={`0 0 ${timelineWidth} ${minimapHeight}`}>
      {#each rows as row, rowIndex}
        {#if row.type !== 'fanout-header'}
          {@const c = barCoords(row.job)}
          {@const y = minimapY(rowIndex)}
          {#if c.pending}
            <rect class={`bar bar-${statusSlug(row.job.status)} ${roleClass(row.job.status)}`} x={c.x} y={y} width="5" height="5" rx="1" transform={`rotate(45 ${c.x + 2.5} ${y + 2.5})`} />
          {:else}
            <rect class={`bar bar-${statusSlug(row.job.status)} ${roleClass(row.job.status)}`} x={c.x} y={y} width={c.width} height="3" rx="1.5" />
            {#if c.ongoing}
              <path class={`ongoing-marker ${roleClass(row.job.status)}`} d={`M ${c.x + c.width + 3} ${y - 1} L ${c.x + c.width + 8} ${y + 1.5} L ${c.x + c.width + 3} ${y + 4}`} />
            {/if}
          {/if}
        {/if}
      {/each}
    </svg>
  </div>

  <div
    class="gantt-scroll"
    bind:this={scrollEl}
    style={`--label-width:${labelWidth}px;--timeline-width:${timelineWidth}px;`}
  >
    <div class="axis-row">
      <div class="label-gutter axis-label">Job</div>
      <div class="timeline">
        <svg width={timelineWidth} height={AXIS_HEIGHT} role="img" aria-label="Workflow time axis">
          <line class="axis-line" x1={TIMELINE_LEFT_INSET} y1={AXIS_HEIGHT - 6} x2={timelinePlotEnd} y2={AXIS_HEIGHT - 6} />
          {#each ticks as tick}
            <g>
              <line class="tick-grid" x1={tick.x} y1={AXIS_HEIGHT - 6} x2={tick.x} y2={AXIS_HEIGHT} />
              <text class="tick-label" x={tick.x} y={AXIS_HEIGHT - 12} text-anchor="middle">{tick.label}</text>
            </g>
          {/each}
        </svg>
      </div>
    </div>

    {#each rows as row, rowIndex}
      {@const selected = row.type === 'fanout-header' ? selectedFanOut?.id === row.fanOut.id : selectedJob?.id === row.job.id}
      <div
        class="chart-row"
        class:row-selected={selected}
        class:row-alt={rowIndex % 2 === 1}
        role="button"
        tabindex="0"
        onclick={() => selectRow(row)}
        onkeydown={(event) => handleKey(event, row)}
        onfocus={() => { if (row.type !== 'fanout-header') focusedJob = row.job }}
        onblur={() => { if (row.type !== 'fanout-header' && focusedJob?.id === row.job.id) focusedJob = null }}
      >
        <div class="label-gutter">
          {#if row.type === 'fanout-header'}
            <div class="fanout-label" style={`padding-left:${row.indent * 16}px`}>
              <span class="strategy-badge">{row.fanOut.strategy || 'fanout'}</span>
              <span class="fanout-progress">{row.fanOut.completedCount}/{row.fanOut.totalCount} completed</span>
              {#if row.fanOut.failedCount > 0}
                <span class="fanout-failed">{row.fanOut.failedCount} failed</span>
              {/if}
              <StatusBadge status={row.fanOut.status} />
            </div>
          {:else}
            <div class="job-label" style={`padding-left:${row.indent * 16}px`}>
              <span class={`status-dot ${roleClass(row.job.status)}`}></span>
              <a href="#/jobs/{row.job.id}" class="job-link" onclick={(event) => navigateJob(event, row.job.id)}>
                <span class="job-type">{row.job.type}</span>
                <span class="job-id">{row.job.id}</span>
                <span class="queue-badge">{row.job.queue}</span>
              </a>
              <StatusBadge status={row.job.status} />
            </div>
          {/if}
        </div>

        <div class="timeline">
          <svg width={timelineWidth} height={ROW_HEIGHT} role="img" aria-label={row.type === 'fanout-header' ? `Fan-out ${row.fanOut.strategy}` : `${row.job.type} ${row.job.status}`}>
            <line class="row-rule" x1="0" y1="0" x2={timelinePlotEnd} y2="0" />
            {#if row.type === 'fanout-header'}
              {#if connectorByFanOut.get(row.fanOut.id) !== undefined}
                <line class="connector-spine" x1={connectorByFanOut.get(row.fanOut.id)} y1={ROW_HEIGHT / 2} x2={timelinePlotEnd} y2={ROW_HEIGHT / 2} />
              {/if}
            {:else}
              {@const c = barCoords(row.job)}
              {@const dur = jobDurationMs(row.job)}
              {@const fanOutX = row.job.fanOutId ? connectorByFanOut.get(row.job.fanOutId) : undefined}
              {#if fanOutX !== undefined}
                <line class="connector-spine" x1={fanOutX} y1="0" x2={fanOutX} y2={ROW_HEIGHT} />
              {/if}
              {#if c.pending}
                <rect
                  class={`bar bar-${statusSlug(row.job.status)} pending-diamond ${roleClass(row.job.status)}`}
                  class:resolve-fill={resolvingIds.has(row.job.id)}
                  x={c.x - 5}
                  y={ROW_HEIGHT / 2 - 5}
                  width="10"
                  height="10"
                  rx="2"
                  transform={`rotate(45 ${c.x} ${ROW_HEIGHT / 2})`}
                />
              {:else}
                <g>
                  {#if dur !== null}
                    <title>{row.job.type}: {formatDuration(dur)} ({row.job.status})</title>
                  {:else}
                    <title>{row.job.type}: {row.job.status}</title>
                  {/if}
                  <rect
                    class={`bar bar-${statusSlug(row.job.status)} ${roleClass(row.job.status)}`}
                    class:bar-running={row.job.status === 'running'}
                    class:resolve-fill={resolvingIds.has(row.job.id)}
                    x={c.x}
                    y={(ROW_HEIGHT - BAR_HEIGHT) / 2}
                    width={c.width}
                    height={BAR_HEIGHT}
                    rx="4"
                  />
                  {#if c.ongoing}
                    <line class={`ongoing-tail ${roleClass(row.job.status)}`} x1={c.x + c.width + 6} y1={ROW_HEIGHT / 2} x2={timelinePlotEnd} y2={ROW_HEIGHT / 2} />
                    <path class={`ongoing-marker ${roleClass(row.job.status)}`} d={`M ${c.x + c.width + 4} ${ROW_HEIGHT / 2 - 7} L ${c.x + c.width + 12} ${ROW_HEIGHT / 2} L ${c.x + c.width + 4} ${ROW_HEIGHT / 2 + 7}`} />
                  {/if}
                </g>
                {#if c.width > 56 && dur !== null}
                  <text class="bar-label" x={c.x + 8} y={ROW_HEIGHT / 2 + 4}>{formatDuration(dur)}</text>
                {/if}
              {/if}
            {/if}
          </svg>
        </div>
      </div>
    {/each}
  </div>

  {#if focusedJob && !selectedJob}
    <div class="focus-panel" role="tooltip">
      <span>{focusedJob.type}</span>
      <StatusBadge status={focusedJob.status} />
      {#if jobDurationMs(focusedJob) !== null}
        <span>{formatDuration(jobDurationMs(focusedJob)!)}</span>
      {/if}
    </div>
  {/if}

  {#if selectedJob}
    {@const job = selectedJob}
    {@const dur = jobDurationMs(job)}
    {@const argsStr = formatJson(job.args)}
    {@const resultStr = formatJson(job.result)}
    <div class="detail-panel">
      <div class="detail-header">
        <div class="detail-title-row">
          <span class={`detail-status-dot ${roleClass(job.status)}`}></span>
          <h5>{job.type}</h5>
          <StatusBadge status={job.status} />
        </div>
        <div class="detail-actions">
          {#if onJobClick}
            <button class="btn-view" type="button" onclick={() => onJobClick?.(job.id)}>View Full Detail</button>
          {/if}
          <button class="btn-close" type="button" onclick={() => selectedJob = null} aria-label="Close">&times;</button>
        </div>
      </div>

      <div class="detail-grid">
        <div class="detail-section">
          <h6>Identifiers</h6>
          <div class="detail-field"><span class="detail-label">Job ID</span><span class="detail-value mono">{job.id}</span></div>
          <div class="detail-field"><span class="detail-label">Queue</span><span class="detail-value">{job.queue}</span></div>
          <div class="detail-field"><span class="detail-label">Priority</span><span class="detail-value">{job.priority}</span></div>
          <div class="detail-field"><span class="detail-label">Attempts</span><span class="detail-value">{job.attempt}/{job.maxRetries}</span></div>
          {#if job.fanOutId}
            <div class="detail-field"><span class="detail-label">Fan-Out ID</span><span class="detail-value mono">{truncateId(job.fanOutId, 16)}</span></div>
            <div class="detail-field"><span class="detail-label">Fan-Out Index</span><span class="detail-value">{job.fanOutIndex}</span></div>
          {/if}
          {#if job.parentJobId}
            <div class="detail-field"><span class="detail-label">Parent Job</span><span class="detail-value mono">{truncateId(job.parentJobId, 16)}</span></div>
          {/if}
          {#if job.rootJobId}
            <div class="detail-field"><span class="detail-label">Root Job</span><span class="detail-value mono">{truncateId(job.rootJobId, 16)}</span></div>
          {/if}
        </div>

        <div class="detail-section">
          <h6>Timing</h6>
          <div class="detail-field"><span class="detail-label">Created</span><span class="detail-value">{formatTime(job.createdAt)}</span></div>
          <div class="detail-field"><span class="detail-label">Started</span><span class="detail-value">{formatTime(job.startedAt)}</span></div>
          <div class="detail-field"><span class="detail-label">Completed</span><span class="detail-value">{formatTime(job.completedAt)}</span></div>
          {#if dur !== null}
            <div class="detail-field"><span class="detail-label">Duration</span><span class="detail-value highlight">{formatDuration(dur)}</span></div>
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

  {#if selectedFanOut}
    {@const fo = selectedFanOut}
    <div class="detail-panel">
      <div class="detail-header">
        <div class="detail-title-row">
          <span class={`detail-status-dot ${roleClass(fo.status)}`}></span>
          <h5>Fan-Out: {fo.strategy}</h5>
          <StatusBadge status={fo.status} />
        </div>
        <button class="btn-close" type="button" onclick={() => selectedFanOut = null} aria-label="Close">&times;</button>
      </div>

      <div class="detail-grid">
        <div class="detail-section">
          <h6>Configuration</h6>
          <div class="detail-field"><span class="detail-label">Fan-Out ID</span><span class="detail-value mono">{fo.id}</span></div>
          <div class="detail-field"><span class="detail-label">Parent Job</span><span class="detail-value mono">{fo.parentJobId}</span></div>
          <div class="detail-field"><span class="detail-label">Strategy</span><span class="detail-value">{fo.strategy}</span></div>
          {#if fo.threshold > 0}
            <div class="detail-field"><span class="detail-label">Threshold</span><span class="detail-value">{(fo.threshold * 100).toFixed(0)}%</span></div>
          {/if}
          <div class="detail-field"><span class="detail-label">Cancel on Fail</span><span class="detail-value">{fo.cancelOnFail ? 'Yes' : 'No'}</span></div>
          <div class="detail-field"><span class="detail-label">Timeout</span><span class="detail-value">{formatTime(fo.timeoutAt)}</span></div>
        </div>

        <div class="detail-section">
          <h6>Progress</h6>
          <div class="detail-field"><span class="detail-label">Total</span><span class="detail-value">{fo.totalCount}</span></div>
          <div class="detail-field"><span class="detail-label">Completed</span><span class="detail-value ok">{fo.completedCount}</span></div>
          <div class="detail-field"><span class="detail-label">Failed</span><span class="detail-value danger">{fo.failedCount}</span></div>
          <div class="detail-field"><span class="detail-label">Cancelled</span><span class="detail-value">{fo.cancelledCount}</span></div>
          <div class="detail-field"><span class="detail-label">Created</span><span class="detail-value">{formatTime(fo.createdAt)}</span></div>
          <div class="detail-field"><span class="detail-label">Updated</span><span class="detail-value">{formatTime(fo.updatedAt)}</span></div>
        </div>
      </div>
    </div>
  {/if}
</div>

<style>
  .waterfall-chart {
    position: relative;
    overflow: hidden;
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
    color: var(--fg-primary);
    box-shadow: inset 0 1px 0 var(--inset-sheen);
  }

  .chart-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--sp-4);
    padding: var(--sp-4);
    border-bottom: var(--border);
    flex-wrap: wrap;
  }

  .header-left {
    display: grid;
    gap: var(--sp-2);
  }

  h4,
  h5,
  h6 {
    margin: 0;
  }

  h4 {
    font-size: var(--fs-section);
    font-weight: var(--fw-head);
  }

  .summary-stats,
  .legend {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: var(--sp-2);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
  }

  .stat,
  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: var(--sp-1);
  }

  .stat-count {
    color: var(--fg-primary);
    font-weight: var(--fw-head);
  }

  .stat-sep {
    color: var(--hairline-strong);
  }

  .stat-failed {
    color: var(--sig-danger);
    font-weight: var(--fw-head);
  }

  .stat-dot,
  .legend-dot,
  .status-dot,
  .detail-status-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: currentColor;
    color: var(--sig-neutral);
    flex: 0 0 auto;
  }

  .legend-label {
    text-transform: capitalize;
  }

  .minimap {
    padding: var(--sp-2) var(--sp-4);
    border-bottom: var(--border);
    background: var(--bg-sunken);
  }

  .gantt-scroll {
    --label-width: clamp(420px, 32vw, 520px);
    --label-gutter-pad-right: var(--sp-3);

    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }

  .axis-row,
  .chart-row {
    display: grid;
    grid-template-columns: var(--label-width) var(--timeline-width);
    min-width: calc(var(--label-width) + var(--timeline-width));
  }

  .chart-row {
    min-height: 48px;
    cursor: pointer;
  }

  .chart-row:hover,
  .chart-row:focus-visible,
  .row-selected {
    background: var(--bg-sunken);
  }

  .row-alt:not(:hover):not(:focus-visible):not(.row-selected) {
    background: color-mix(in srgb, var(--bg-raised) 92%, var(--bg-sunken));
  }

  .label-gutter {
    position: sticky;
    left: 0;
    z-index: 2;
    display: flex;
    align-items: center;
    width: var(--label-width);
    overflow: hidden;
    box-sizing: border-box;
    min-width: 0;
    min-height: 48px;
    padding: 0 var(--label-gutter-pad-right) 0 var(--sp-3);
    border-right: var(--border);
    background: inherit;
  }

  .axis-label {
    min-height: 36px;
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
    font-weight: var(--fw-label);
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .timeline {
    min-width: 0;
  }

  svg {
    display: block;
    overflow: visible;
  }

  .axis-line,
  .row-rule {
    stroke: var(--hairline);
    stroke-width: 1;
  }

  .tick-grid {
    stroke: var(--hairline-strong);
    stroke-width: 1;
  }

  .tick-label {
    fill: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-feature-settings: var(--num);
  }

  .connector-spine {
    stroke: color-mix(in srgb, var(--accent) 52%, var(--hairline));
    stroke-width: 2;
    stroke-dasharray: 4 4;
  }

  .fanout-label,
  .job-label {
    display: flex;
    align-items: center;
    gap: var(--sp-2);
    overflow: hidden;
    min-width: 0;
    width: 100%;
  }

  .fanout-label {
    gap: 6px;
    white-space: nowrap;
  }

  .strategy-badge,
  .queue-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 20px;
    border: var(--border);
    border-radius: var(--radius-chip);
    background: var(--bg-sunken);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-feature-settings: var(--num);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .strategy-badge {
    flex: 0 1 auto;
    max-width: 12ch;
    padding: 0 8px;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .queue-badge {
    flex: 0 0 auto;
    width: max-content;
    max-width: none;
    padding: 0 6px;
    overflow: visible;
    text-overflow: clip;
  }

  .fanout-progress {
    flex: 1 1 auto;
    min-width: 0;
    overflow: hidden;
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .fanout-failed {
    flex: 0 0 auto;
    color: var(--sig-danger);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-weight: var(--fw-head);
    white-space: nowrap;
  }

  .fanout-label :global(.status) {
    flex: 0 0 auto;
    max-width: none;
    overflow: visible;
    padding-inline: 6px;
    font-size: var(--fs-micro);
    text-overflow: clip;
  }

  .job-link {
    display: grid;
    grid-template-columns: minmax(0, 13ch) minmax(0, 1fr) max-content;
    align-items: center;
    flex: 1 1 auto;
    gap: var(--sp-2);
    min-width: 0;
    overflow: hidden;
    color: inherit;
    text-decoration: none;
  }

  .job-link:hover .job-type,
  .job-link:focus-visible .job-type {
    color: var(--accent);
  }

  .job-type {
    min-width: 0;
    overflow: hidden;
    color: var(--fg-primary);
    font-size: var(--fs-body);
    font-weight: var(--fw-head);
    line-height: var(--lh-dense);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .job-id {
    min-width: 0;
    overflow: hidden;
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-feature-settings: var(--num);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .job-label :global(.status) {
    flex: 0 0 auto;
    max-width: none;
    overflow: visible;
    text-overflow: clip;
  }

  .bar {
    fill: currentColor;
    color: var(--sig-neutral);
    opacity: 0.9;
  }

  .pending-diamond {
    fill: transparent;
    stroke: currentColor;
    stroke-width: 2;
  }

  .bar-running {
    animation: wf-scan var(--dur-sweep) var(--ease) infinite alternate;
  }

  .ongoing-tail {
    stroke: currentColor;
    stroke-width: 2;
    stroke-dasharray: 5 6;
    opacity: 0.42;
  }

  .ongoing-marker {
    fill: none;
    stroke: currentColor;
    stroke-linecap: round;
    stroke-linejoin: round;
    stroke-width: 2;
    opacity: 0.95;
  }

  .resolve-fill {
    animation: nodeResolve var(--dur-quick) var(--ease) 1;
  }

  .bar-label {
    fill: var(--bg-base);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-weight: var(--fw-label);
    pointer-events: none;
  }

  .role-ok { color: var(--sig-ok); }
  .role-info { color: var(--sig-info); }
  .role-danger { color: var(--sig-danger); }
  .role-warn { color: var(--sig-warn); }
  .role-neutral { color: var(--sig-neutral); }
  .role-purple { color: var(--sig-purple); }
  .role-paused { color: var(--sig-paused); }

  .focus-panel {
    display: flex;
    align-items: center;
    gap: var(--sp-2);
    padding: var(--sp-3) var(--sp-4);
    border-top: var(--border);
    background: var(--bg-sunken);
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
  }

  .detail-panel {
    padding: var(--sp-4);
    border-top: 2px solid var(--accent);
    background: var(--bg-sunken);
  }

  .detail-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--sp-3);
    margin-bottom: var(--sp-4);
  }

  .detail-title-row,
  .detail-actions {
    display: flex;
    align-items: center;
    gap: var(--sp-2);
    min-width: 0;
  }

  .detail-title-row h5 {
    color: var(--fg-primary);
    font-size: var(--fs-section);
    font-weight: var(--fw-head);
  }

  .btn-view,
  .btn-close {
    min-height: 28px;
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-raised);
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    cursor: pointer;
  }

  .btn-view {
    padding: 0 var(--sp-3);
    border-color: var(--accent);
    color: var(--accent);
  }

  .btn-close {
    width: 28px;
  }

  .btn-view:hover,
  .btn-close:hover {
    border-color: var(--hairline-strong);
    background: var(--bg-sunken);
  }

  .detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: var(--sp-4);
  }

  .detail-section {
    min-width: 0;
    padding: var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-panel);
    background: var(--bg-raised);
  }

  .detail-section-wide {
    grid-column: 1 / -1;
  }

  .detail-section h6 {
    margin-bottom: var(--sp-2);
    color: var(--fg-secondary);
    font-size: var(--fs-micro);
    font-weight: var(--fw-label);
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .detail-field {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--sp-3);
    padding: var(--sp-1) 0;
  }

  .detail-label {
    color: var(--fg-secondary);
    flex: 0 0 auto;
  }

  .detail-value {
    min-width: 0;
    overflow: hidden;
    color: var(--fg-primary);
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
    text-align: right;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .detail-value.highlight {
    color: var(--accent);
    font-weight: var(--fw-head);
  }

  .detail-value.ok {
    color: var(--sig-ok);
  }

  .detail-value.danger {
    color: var(--sig-danger);
  }

  .mono,
  .detail-pre {
    font-family: var(--font-mono);
    font-feature-settings: var(--num);
  }

  .detail-pre {
    max-height: 220px;
    overflow: auto;
    margin: 0;
    padding: var(--sp-3);
    border: var(--border);
    border-radius: var(--radius-input);
    background: var(--bg-sunken);
    color: var(--fg-primary);
    font-size: var(--fs-label);
    line-height: 1.6;
    white-space: pre-wrap;
  }

  .detail-error {
    border-color: var(--sig-danger);
    background: var(--sig-danger-bg);
    color: var(--sig-danger);
  }

  @keyframes wf-scan {
    from { opacity: 0.75; }
    to { opacity: 1; }
  }

  @keyframes nodeResolve {
    from { opacity: 0.15; }
    to { opacity: 1; }
  }

  @media (prefers-reduced-motion: reduce) {
    .bar-running {
      opacity: 1;
    }
  }

  @media (max-width: 720px) {
    .waterfall-chart {
      border-radius: var(--radius-panel);
    }

    .gantt-scroll {
      --label-width: clamp(300px, 42vw, 360px);
      --label-gutter-pad-right: var(--sp-3);
    }

    .label-gutter {
      padding: 0 var(--label-gutter-pad-right) 0 var(--sp-2);
    }

    .job-link {
      grid-template-columns: minmax(0, 12ch) minmax(0, 1fr);
    }

    .queue-badge {
      display: none;
    }

    .detail-grid {
      grid-template-columns: 1fr;
    }

    .detail-header {
      align-items: flex-start;
      flex-direction: column;
    }
  }
</style>
