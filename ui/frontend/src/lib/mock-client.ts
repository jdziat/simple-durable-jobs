/**
 * Mock client for the demo/GitHub Pages build.
 *
 * Provides the same method signatures as the Connect-RPC jobsClient so Svelte
 * components can work without a real backend.  Includes a simulation engine
 * that keeps the data moving.
 */

import { Timestamp, protoInt64 } from '@bufbuild/protobuf'
import {
  Job,
  Checkpoint,
  QueueStats,
  DataPoint,
  ScheduledJobInfo,
  GetStatsResponse,
  GetStatsHistoryResponse,
  ListJobsResponse,
  GetJobResponse,
  RetryJobResponse,
  DeleteJobResponse,
  ListQueuesResponse,
  PurgeQueueResponse,
  ListScheduledJobsResponse,
} from './gen/jobs/v1/jobs_pb.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let nextId = 1000

function uid(): string {
  return `job_${(nextId++).toString(36)}_${Math.random().toString(36).slice(2, 8)}`
}

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function minutesAgo(n: number): Date {
  return new Date(Date.now() - n * 60_000)
}

function ts(date: Date): Timestamp {
  return Timestamp.fromDate(date)
}

function i64(n: number): bigint {
  return protoInt64.parse(n)
}

const encoder = new TextEncoder()
function jsonBytes(obj: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(obj))
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const QUEUE_NAMES = ['default', 'emails', 'critical'] as const
type QueueName = (typeof QUEUE_NAMES)[number]

const JOB_TYPES = [
  'send-email',
  'process-order',
  'generate-report',
  'sync-inventory',
  'charge-payment',
  'resize-image',
  'send-webhook',
  'update-search-index',
] as const

const STATUSES = ['pending', 'running', 'completed', 'failed'] as const
type Status = (typeof STATUSES)[number]

const QUEUE_FOR_TYPE: Record<string, QueueName> = {
  'send-email': 'emails',
  'charge-payment': 'critical',
  'process-order': 'critical',
}

const ERROR_MESSAGES = [
  'connection timeout after 30s',
  'HTTP 503 Service Unavailable',
  'record not found: user_id=4821',
  'rate limit exceeded (429)',
  'invalid JSON in response body',
]

const ARGS_BY_TYPE: Record<string, () => unknown> = {
  'send-email': () => ({ to: `user${randInt(1, 500)}@example.com`, template: pick(['welcome', 'receipt', 'reset-password', 'weekly-digest']) }),
  'process-order': () => ({ orderId: `ORD-${randInt(10000, 99999)}`, items: randInt(1, 8) }),
  'generate-report': () => ({ reportType: pick(['daily-sales', 'inventory', 'user-activity']), format: 'pdf' }),
  'sync-inventory': () => ({ warehouseId: `WH-${randInt(1, 5)}`, skus: randInt(10, 200) }),
  'charge-payment': () => ({ amount: randInt(500, 25000), currency: 'USD', customerId: `cus_${randInt(1000, 9999)}` }),
  'resize-image': () => ({ imageId: `img_${randInt(100, 999)}`, width: 800, height: 600 }),
  'send-webhook': () => ({ url: 'https://hooks.example.com/events', event: pick(['order.created', 'user.signed_up', 'payment.received']) }),
  'update-search-index': () => ({ entity: pick(['product', 'user', 'order']), batchSize: randInt(50, 500) }),
}

// ---------------------------------------------------------------------------
// Mock data store
// ---------------------------------------------------------------------------

interface MockJob {
  id: string
  type: string
  queue: QueueName
  status: Status
  priority: number
  attempt: number
  maxRetries: number
  args: Uint8Array
  lastError: string
  createdAt: Date
  startedAt: Date | null
  completedAt: Date | null
}

interface HistoryPoint {
  timestamp: Date
  completed: number
  failed: number
}

const jobs: MockJob[] = []
const historyPoints: HistoryPoint[] = []

function makeJob(overrides: Partial<MockJob> = {}): MockJob {
  const type = overrides.type ?? pick(JOB_TYPES)
  const queue = overrides.queue ?? QUEUE_FOR_TYPE[type] ?? 'default'
  const status = overrides.status ?? 'pending'
  const createdAt = overrides.createdAt ?? minutesAgo(randInt(1, 120))
  const argsFn = ARGS_BY_TYPE[type] ?? (() => ({}))

  return {
    id: overrides.id ?? uid(),
    type,
    queue,
    status,
    priority: overrides.priority ?? randInt(0, 10),
    attempt: overrides.attempt ?? (status === 'failed' ? randInt(1, 3) : status === 'completed' ? 1 : 0),
    maxRetries: overrides.maxRetries ?? 3,
    args: overrides.args ?? jsonBytes(argsFn()),
    lastError: overrides.lastError ?? (status === 'failed' ? pick(ERROR_MESSAGES) : ''),
    createdAt,
    startedAt: overrides.startedAt ?? (['running', 'completed', 'failed'].includes(status) ? new Date(createdAt.getTime() + randInt(1, 10) * 1000) : null),
    completedAt: overrides.completedAt ?? (['completed', 'failed'].includes(status) ? new Date(createdAt.getTime() + randInt(10, 300) * 1000) : null),
  }
}

// Seed ~30 initial jobs spread across statuses
function seedJobs(): void {
  const distribution: { status: Status; count: number }[] = [
    { status: 'pending', count: 8 },
    { status: 'running', count: 5 },
    { status: 'completed', count: 12 },
    { status: 'failed', count: 5 },
  ]
  for (const { status, count } of distribution) {
    for (let i = 0; i < count; i++) {
      jobs.push(makeJob({ status }))
    }
  }
}

// Seed history points (last 60 minutes, one point per minute)
function seedHistory(): void {
  for (let i = 60; i >= 0; i--) {
    historyPoints.push({
      timestamp: minutesAgo(i),
      completed: randInt(2, 12),
      failed: randInt(0, 2),
    })
  }
}

// Checkpoints for process-order type jobs
function checkpointsForJob(job: MockJob): Checkpoint[] {
  if (job.type !== 'process-order') return []
  if (job.status === 'pending') return []

  const steps = [
    { callType: 'validate-order', result: '{"valid":true}' },
    { callType: 'reserve-inventory', result: '{"reserved":true}' },
    { callType: 'charge-payment', result: '{"chargeId":"ch_abc123"}' },
    { callType: 'send-confirmation', result: '{"emailSent":true}' },
  ]

  // Running jobs have partial checkpoints, completed have all, failed stop midway
  let count: number
  if (job.status === 'completed') {
    count = steps.length
  } else if (job.status === 'running') {
    count = randInt(1, 3)
  } else {
    // failed
    count = randInt(1, 3)
  }

  const checkpoints: Checkpoint[] = []
  for (let i = 0; i < count; i++) {
    const step = steps[i]
    const isFinalAndFailed = i === count - 1 && job.status === 'failed'
    checkpoints.push(
      new Checkpoint({
        id: `cp_${job.id}_${i}`,
        jobId: job.id,
        callIndex: i,
        callType: step.callType,
        result: isFinalAndFailed ? new Uint8Array(0) : encoder.encode(step.result),
        error: isFinalAndFailed ? job.lastError : '',
        createdAt: ts(new Date((job.startedAt?.getTime() ?? job.createdAt.getTime()) + i * 2000)),
      }),
    )
  }
  return checkpoints
}

// Scheduled jobs (static, never mutated)
const scheduledJobs: ScheduledJobInfo[] = [
  new ScheduledJobInfo({
    name: 'daily-report',
    schedule: '0 8 * * *',
    queue: 'default',
    nextRun: ts(new Date(Date.now() + 6 * 3600_000)),
    lastRun: ts(minutesAgo(1440)),
  }),
  new ScheduledJobInfo({
    name: 'sync-inventory',
    schedule: '*/15 * * * *',
    queue: 'default',
    nextRun: ts(new Date(Date.now() + 8 * 60_000)),
    lastRun: ts(minutesAgo(7)),
  }),
  new ScheduledJobInfo({
    name: 'weekly-digest-emails',
    schedule: '0 9 * * 1',
    queue: 'emails',
    nextRun: ts(new Date(Date.now() + 3 * 86400_000)),
    lastRun: ts(new Date(Date.now() - 4 * 86400_000)),
  }),
  new ScheduledJobInfo({
    name: 'stale-lock-reaper',
    schedule: '@every 5m',
    queue: 'critical',
    nextRun: ts(new Date(Date.now() + 2 * 60_000)),
    lastRun: ts(minutesAgo(3)),
  }),
]

// ---------------------------------------------------------------------------
// Simulation engine
// ---------------------------------------------------------------------------

let simulationTimer: ReturnType<typeof setInterval> | null = null

function ensureSimulation(): void {
  if (simulationTimer !== null) return
  if (jobs.length === 0) {
    seedJobs()
    seedHistory()
  }
  simulationTimer = setInterval(tick, 3000)
}

function tick(): void {
  // Transition 1-2 pending -> running
  const pending = jobs.filter((j) => j.status === 'pending')
  const toStart = pending.slice(0, randInt(1, 2))
  for (const j of toStart) {
    j.status = 'running'
    j.startedAt = new Date()
    j.attempt = j.attempt + 1
  }

  // Complete 1-2 running jobs
  const running = jobs.filter((j) => j.status === 'running')
  const toFinish = running.slice(0, randInt(1, 2))
  for (const j of toFinish) {
    if (Math.random() < 0.1) {
      // ~10% chance of failure
      j.status = 'failed'
      j.lastError = pick(ERROR_MESSAGES)
    } else {
      j.status = 'completed'
    }
    j.completedAt = new Date()
  }

  // Add 1-2 new pending jobs to keep pipeline flowing
  const newCount = randInt(1, 2)
  for (let i = 0; i < newCount; i++) {
    jobs.push(makeJob({ status: 'pending', createdAt: new Date() }))
  }

  // Cap at ~100 jobs: remove oldest completed/failed first
  if (jobs.length > 100) {
    // Sort terminal jobs by createdAt ascending, remove extras
    const terminal = jobs
      .map((j, idx) => ({ j, idx }))
      .filter(({ j }) => j.status === 'completed' || j.status === 'failed')
      .sort((a, b) => a.j.createdAt.getTime() - b.j.createdAt.getTime())

    let toRemove = jobs.length - 100
    const removeIndices = new Set<number>()
    for (const { idx } of terminal) {
      if (toRemove <= 0) break
      removeIndices.add(idx)
      toRemove--
    }
    // Remove from highest index to lowest so indices stay valid
    const sorted = [...removeIndices].sort((a, b) => b - a)
    for (const idx of sorted) {
      jobs.splice(idx, 1)
    }
  }

  // Record a history data point
  const completedInTick = toFinish.filter((j) => j.status === 'completed').length
  const failedInTick = toFinish.filter((j) => j.status === 'failed').length
  historyPoints.push({
    timestamp: new Date(),
    completed: completedInTick + randInt(1, 5),
    failed: failedInTick + randInt(0, 1),
  })

  // Keep history bounded to ~120 points
  if (historyPoints.length > 120) {
    historyPoints.splice(0, historyPoints.length - 120)
  }
}

// ---------------------------------------------------------------------------
// Conversion helpers: MockJob -> protobuf Job
// ---------------------------------------------------------------------------

function toProtoJob(j: MockJob): Job {
  return new Job({
    id: j.id,
    type: j.type,
    queue: j.queue,
    status: j.status,
    priority: j.priority,
    attempt: j.attempt,
    maxRetries: j.maxRetries,
    args: j.args as Uint8Array<ArrayBuffer>,
    lastError: j.lastError,
    createdAt: ts(j.createdAt),
    startedAt: j.startedAt ? ts(j.startedAt) : undefined,
    completedAt: j.completedAt ? ts(j.completedAt) : undefined,
  })
}

// ---------------------------------------------------------------------------
// Mock client (exported)
// ---------------------------------------------------------------------------

function countByStatus(source: MockJob[], status: Status): number {
  return source.filter((j) => j.status === status).length
}

function buildQueueStats(queueName: string): QueueStats {
  const inQueue = jobs.filter((j) => j.queue === queueName)
  return new QueueStats({
    name: queueName,
    pending: i64(countByStatus(inQueue, 'pending')),
    running: i64(countByStatus(inQueue, 'running')),
    completed: i64(countByStatus(inQueue, 'completed')),
    failed: i64(countByStatus(inQueue, 'failed')),
  })
}

export const mockJobsClient = {
  async getStats(
    _req: Record<string, unknown>,
  ): Promise<GetStatsResponse> {
    ensureSimulation()
    const queues = QUEUE_NAMES.map(buildQueueStats)
    return new GetStatsResponse({
      queues,
      totalPending: i64(countByStatus(jobs, 'pending')),
      totalRunning: i64(countByStatus(jobs, 'running')),
      totalCompleted: i64(countByStatus(jobs, 'completed')),
      totalFailed: i64(countByStatus(jobs, 'failed')),
      activeWorkers: jobs.filter((j) => j.status === 'running').length,
    })
  },

  async getStatsHistory(
    req: { period?: string; queue?: string },
  ): Promise<GetStatsHistoryResponse> {
    ensureSimulation()

    // Filter history by period length
    let points = historyPoints
    const period = req.period ?? '1h'
    let windowMs: number
    switch (period) {
      case '24h':
        windowMs = 24 * 3600_000
        break
      case '7d':
        windowMs = 7 * 86400_000
        break
      default: // '1h'
        windowMs = 3600_000
        break
    }
    const cutoff = Date.now() - windowMs
    points = points.filter((p) => p.timestamp.getTime() >= cutoff)

    // If a queue filter is given, scale values proportionally (mock approximation)
    let scale = 1
    if (req.queue) {
      const queueJobs = jobs.filter((j) => j.queue === req.queue)
      scale = jobs.length > 0 ? queueJobs.length / jobs.length : 0
    }

    return new GetStatsHistoryResponse({
      completed: points.map(
        (p) =>
          new DataPoint({
            timestamp: ts(p.timestamp),
            value: i64(Math.round(p.completed * scale)),
          }),
      ),
      failed: points.map(
        (p) =>
          new DataPoint({
            timestamp: ts(p.timestamp),
            value: i64(Math.round(p.failed * scale)),
          }),
      ),
    })
  },

  async listJobs(
    req: {
      status?: string
      queue?: string
      type?: string
      search?: string
      page?: number
      limit?: number
    },
  ): Promise<ListJobsResponse> {
    ensureSimulation()

    let filtered = [...jobs]
    if (req.status) filtered = filtered.filter((j) => j.status === req.status)
    if (req.queue) filtered = filtered.filter((j) => j.queue === req.queue)
    if (req.type) filtered = filtered.filter((j) => j.type === req.type)
    if (req.search) {
      const s = req.search.toLowerCase()
      filtered = filtered.filter(
        (j) =>
          j.id.toLowerCase().includes(s) ||
          j.type.toLowerCase().includes(s) ||
          j.queue.toLowerCase().includes(s),
      )
    }

    // Sort newest first
    filtered.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())

    const total = filtered.length
    const page = req.page ?? 1
    const limit = req.limit ?? 20
    const start = (page - 1) * limit
    const paged = filtered.slice(start, start + limit)

    return new ListJobsResponse({
      jobs: paged.map(toProtoJob),
      total: i64(total),
      page,
    })
  },

  async getJob(
    req: { id: string },
  ): Promise<GetJobResponse> {
    ensureSimulation()
    const j = jobs.find((job) => job.id === req.id)
    if (!j) {
      // Return empty response (component handles missing job)
      return new GetJobResponse({})
    }
    return new GetJobResponse({
      job: toProtoJob(j),
      checkpoints: checkpointsForJob(j),
    })
  },

  async retryJob(
    req: { id: string },
  ): Promise<RetryJobResponse> {
    ensureSimulation()
    const j = jobs.find((job) => job.id === req.id)
    if (j) {
      j.status = 'pending'
      j.lastError = ''
      j.startedAt = null
      j.completedAt = null
      j.attempt = 0
    }
    return new RetryJobResponse({
      job: j ? toProtoJob(j) : undefined,
    })
  },

  async deleteJob(
    req: { id: string },
  ): Promise<DeleteJobResponse> {
    ensureSimulation()
    const idx = jobs.findIndex((job) => job.id === req.id)
    if (idx !== -1) {
      jobs.splice(idx, 1)
    }
    return new DeleteJobResponse({})
  },

  async listQueues(
    _req: Record<string, unknown>,
  ): Promise<ListQueuesResponse> {
    ensureSimulation()
    return new ListQueuesResponse({
      queues: QUEUE_NAMES.map(buildQueueStats),
    })
  },

  async purgeQueue(
    req: { name: string; status?: string },
  ): Promise<PurgeQueueResponse> {
    ensureSimulation()
    let removed = 0
    for (let i = jobs.length - 1; i >= 0; i--) {
      const j = jobs[i]
      if (j.queue !== req.name) continue
      if (req.status && j.status !== req.status) continue
      jobs.splice(i, 1)
      removed++
    }
    return new PurgeQueueResponse({
      deleted: i64(removed),
    })
  },

  async listScheduledJobs(
    _req: Record<string, unknown>,
  ): Promise<ListScheduledJobsResponse> {
    ensureSimulation()
    return new ListScheduledJobsResponse({
      jobs: scheduledJobs,
    })
  },
}
