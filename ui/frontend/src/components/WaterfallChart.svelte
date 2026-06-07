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
  const LABEL_WIDTH = 260
  const MONO_ADVANCE = 7

  type RootRow = { type: 'root'; job: Job; indent: 0 }
  type FanOutHeaderRow = { type: 'fanout-header'; fanOut: FanOut; indent: 1 }
  type SubjobRow = { type: 'subjob'; job: Job; indent: 1 | 2 }
  type Row = RootRow | FanOutHeaderRow | SubjobRow

  let selectedJob = $state<Job | null>(null)
  let selectedFanOut = $state<FanOut | null>(null)
  let focusedJob = $state<Job | null>(null)
  let resolvingIds = $state<Set<string>>(new Set())
  let scrollEl = $state<HTMLDivElement | null>(null)
  let timelineWidth = $state(MIN_TIMELINE_WIDTH)

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
      timelineWidth = Math.max(MIN_TIMELINE_WIDTH, Math.floor(width - LABEL_WIDTH))
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

  const timeStart = $derived(root.createdAt?.toDate() ?? new Date())
  const timeEnd = $derived.by(() => {
    const now = Date.now()
    let max = root.completedAt?.toDate()?.getTime() ?? now
    for (const job of children) {
      const completed = job.completedAt?.toDate()?.getTime()
      const started = job.startedAt?.toDate()?.getTime()
      const created = job.createdAt?.toDate()?.getTime()
      max = Math.max(max, completed ?? started ?? created ?? now)
    }
    return new Date(Math.max(max, now))
  })
  const timeRange = $derived(Math.max(timeEnd.getTime() - timeStart.getTime(), 1))

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
      if (starts.length > 0) map.set(fanOut.id, Math.max(0, Math.min(...starts) - 8))
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
    return Math.max(0, Math.min(timelineWidth, ((time - timeStart.getTime()) / timeRange) * timelineWidth))
  }

  function barCoords(job: Job): { x: number; width: number; pending: boolean } {
    const now = Date.now()
    const created = job.createdAt?.toDate()?.getTime() ?? now
    const started = job.startedAt?.toDate()?.getTime()
    const completed = job.completedAt?.toDate()?.getTime()

    if (!started && !completed) {
      return { x: xForTime(created), width: 0, pending: true }
    }

    const start = started ?? created
    const end = completed ?? now
    return {
      x: xForTime(start),
      width: Math.max(6, xForTime(end) - xForTime(start)),
      pending: false,
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

  function monoWidth(text: string, pad = 12): number {
    return Math.max(36, text.length * MONO_ADVANCE + pad)
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
</script>

<div class="waterfall-chart">
  <div class="chart-header">
    <div class="header-left">
      <h4>Workflow Timeline</h4>
      <div class="summary-stats">
        <span class="stat"><span class="stat-count">{totalJobs}</span> jobs</span>
        <span class="stat-sep">|</span>
        <span class="stat stat-completed"><span class="stat-dot role-ok"></span>{completedJobs}</span>
        <span class="stat stat-running"><span class="stat-dot role-info"></span>{runningJobs}</span>
        {#if failedJobs > 0}
          <span class="stat stat-failed"><span class="stat-dot role-danger"></span>{failedJobs}</span>
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
    <svg width="100%" height="22" viewBox={`0 0 ${timelineWidth} 22`}>
      {#each rows as row}
        {#if row.type !== 'fanout-header'}
          {@const c = barCoords(row.job)}
          {#if c.pending}
            <rect class={`bar bar-${statusSlug(row.job.status)} ${roleClass(row.job.status)}`} x={c.x} y="8" width="5" height="5" rx="1" transform={`rotate(45 ${c.x + 2.5} 10.5)`} />
          {:else}
            <rect class={`bar bar-${statusSlug(row.job.status)} ${roleClass(row.job.status)}`} x={c.x} y="7" width={c.width} height="8" rx="2" />
          {/if}
        {/if}
      {/each}
    </svg>
  </div>

  <div
    class="gantt-scroll"
    bind:this={scrollEl}
    style={`--label-width:${LABEL_WIDTH}px;--timeline-width:${timelineWidth}px;`}
  >
    <div class="axis-row">
      <div class="label-gutter axis-label">Job</div>
      <div class="timeline">
        <svg width={timelineWidth} height={AXIS_HEIGHT} role="img" aria-label="Workflow time axis">
          <line class="axis-line" x1="0" y1={AXIS_HEIGHT - 6} x2={timelineWidth} y2={AXIS_HEIGHT - 6} />
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
              <span class="strategy-badge" style={`width:${monoWidth(row.fanOut.strategy || 'fanout', 26)}px`}>{row.fanOut.strategy || 'fanout'}</span>
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
                <span class="job-meta">
                  <span>{truncateId(row.job.id)}</span>
                  <span class="queue-badge" style={`width:${monoWidth(row.job.queue, 10)}px`}>{row.job.queue}</span>
                </span>
                <StatusBadge status={row.job.status} />
              </a>
            </div>
          {/if}
        </div>

        <div class="timeline">
          <svg width={timelineWidth} height={ROW_HEIGHT} role="img" aria-label={row.type === 'fanout-header' ? `Fan-out ${row.fanOut.strategy}` : `${row.job.type} ${row.job.status}`}>
            <line class="row-rule" x1="0" y1="0" x2={timelineWidth} y2="0" />
            {#if row.type === 'fanout-header'}
              {#if connectorByFanOut.get(row.fanOut.id) !== undefined}
                <line class="connector-spine" x1={connectorByFanOut.get(row.fanOut.id)} y1={ROW_HEIGHT / 2} x2={timelineWidth} y2={ROW_HEIGHT / 2} />
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
          <button class="btn-close" type="button" onclick={() => selectedJob = null} aria-label="Close">x</button>
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
        <button class="btn-close" type="button" onclick={() => selectedFanOut = null} aria-label="Close">x</button>
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
    min-width: 0;
    min-height: 48px;
    padding: 0 var(--sp-3);
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
    min-width: 0;
    width: 100%;
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

  .fanout-progress {
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    white-space: nowrap;
  }

  .fanout-failed {
    color: var(--sig-danger);
    font-family: var(--font-mono);
    font-size: var(--fs-label);
    font-weight: var(--fw-head);
    white-space: nowrap;
  }

  .job-link {
    display: grid;
    gap: var(--sp-1);
    min-width: 0;
    color: inherit;
    text-decoration: none;
  }

  .job-link:hover .job-type,
  .job-link:focus-visible .job-type {
    color: var(--accent);
  }

  .job-type {
    overflow: hidden;
    color: var(--fg-primary);
    font-size: var(--fs-body);
    font-weight: var(--fw-head);
    line-height: var(--lh-dense);
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .job-meta {
    display: flex;
    align-items: center;
    gap: var(--sp-2);
    min-width: 0;
    color: var(--fg-secondary);
    font-family: var(--font-mono);
    font-size: var(--fs-micro);
    font-feature-settings: var(--num);
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

    .axis-row,
    .chart-row {
      grid-template-columns: 210px var(--timeline-width);
    }

    .label-gutter {
      padding: 0 var(--sp-2);
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
