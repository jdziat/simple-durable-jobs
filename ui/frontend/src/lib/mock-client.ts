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
  FanOut as ProtoFanOut,
  GetWorkflowResponse,
  ListWorkflowsResponse,
  WorkflowSummary,
  PauseJobResponse,
  CancelJobResponse,
  ResumeJobResponse,
  PauseQueueResponse,
  ResumeQueueResponse,
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

const STATUSES = ['pending', 'running', 'completed', 'failed', 'paused', 'retrying', 'waiting', 'cancelled'] as const
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

// Track which queues are paused
const pausedQueues = new Set<string>()

interface MockJob {
  id: string
  type: string
  queue: QueueName
  tenant?: string
  metadata?: Record<string, string>
  status: Status | 'cancelled'
  priority: number
  attempt: number
  maxRetries: number
  args: Uint8Array
  lastError: string
  createdAt: Date
  startedAt: Date | null
  completedAt: Date | null
  deadLetteredAt?: Date
  deadLetterReason?: string
  worker?: string
  parentJobId?: string
  rootJobId?: string
  fanOutId?: string
  fanOutIndex?: number
  result?: Uint8Array
}

interface HistoryPoint {
  timestamp: Date
  completed: number
  failed: number
}

interface MockFanOut {
  id: string
  parentJobId: string
  totalCount: number
  completedCount: number
  failedCount: number
  cancelledCount: number
  strategy: string
  threshold: number
  status: string
  cancelOnFail: boolean
  createdAt: Date
  updatedAt: Date
}

const jobs: MockJob[] = []
const historyPoints: HistoryPoint[] = []
const workflowJobs: MockJob[] = []
const fanOuts: MockFanOut[] = []

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
    tenant: overrides.tenant,
    metadata: overrides.metadata,
    status,
    priority: overrides.priority ?? randInt(0, 10),
    attempt: overrides.attempt ?? (status === 'failed' ? randInt(1, 3) : status === 'completed' ? 1 : 0),
    maxRetries: overrides.maxRetries ?? 3,
    args: overrides.args ?? jsonBytes(argsFn()),
    lastError: overrides.lastError ?? (status === 'failed' ? pick(ERROR_MESSAGES) : ''),
    createdAt,
    startedAt: overrides.startedAt !== undefined
      ? overrides.startedAt
      : (['running', 'completed', 'failed'].includes(status as string) ? new Date(createdAt.getTime() + randInt(1, 10) * 1000) : null),
    completedAt: overrides.completedAt !== undefined
      ? overrides.completedAt
      : (['completed', 'failed'].includes(status as string) ? new Date(createdAt.getTime() + randInt(10, 300) * 1000) : null),
    deadLetteredAt: overrides.deadLetteredAt,
    deadLetterReason: overrides.deadLetterReason ?? '',
    worker: overrides.worker ?? (status === 'running' ? `worker-${randInt(1, 4)}` : ''),
    parentJobId: overrides.parentJobId,
    rootJobId: overrides.rootJobId,
    fanOutId: overrides.fanOutId,
    fanOutIndex: overrides.fanOutIndex,
    result: overrides.result,
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

  const deadLetteredAt = minutesAgo(18)
  jobs.push(makeJob({
    id: 'job_demo_dead_lettered',
    type: 'send-webhook',
    queue: 'critical',
    tenant: 'acme',
    metadata: { region: 'us-east', team: 'payments' },
    status: 'failed',
    attempt: 3,
    maxRetries: 3,
    createdAt: minutesAgo(28),
    completedAt: deadLetteredAt,
    lastError: 'HTTP 503 Service Unavailable',
    deadLetteredAt,
    deadLetterReason: 'max retries exhausted: HTTP 503 Service Unavailable',
  }))

  // A second dead-lettered job whose created_at/dead_lettered_at orderings
  // DIFFER from the one above (newer created, but older dead-lettered). This
  // makes the dead-letter view's "default = dead_lettered_at DESC" sort
  // observably distinct from a created_at sort — and lets the mock-client unit
  // test prove the per-view default rather than trivially pass on a single row.
  const olderDeadLetteredAt = minutesAgo(40)
  jobs.push(makeJob({
    id: 'job_demo_dead_lettered_2',
    type: 'reconcile-ledger',
    queue: 'critical',
    tenant: 'globex',
    metadata: { region: 'eu-west', team: 'billing' },
    status: 'failed',
    attempt: 5,
    maxRetries: 5,
    createdAt: minutesAgo(6),
    completedAt: olderDeadLetteredAt,
    lastError: 'deadlock detected; serialization failure',
    deadLetteredAt: olderDeadLetteredAt,
    deadLetterReason: 'max retries exhausted: deadlock detected',
  }))

  jobs.push(makeJob({
    id: 'job_demo_tenant_acme',
    type: 'charge-payment',
    queue: 'critical',
    tenant: 'acme',
    metadata: { region: 'us-east', team: 'payments', tier: 'gold' },
    status: 'completed',
    createdAt: minutesAgo(22),
    lastError: '',
  }))

  jobs.push(makeJob({
    id: 'job_demo_tenant_globex',
    type: 'generate-report',
    queue: 'default',
    tenant: 'globex',
    metadata: { region: 'eu-west', team: 'analytics' },
    status: 'pending',
    createdAt: minutesAgo(11),
    lastError: '',
  }))
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

// Seed workflow scenarios
function seedWorkflows(): void {
  // -------------------------------------------------------------------
  // Scenario 1: Batch Order Processing (FailFast)
  // Root job running, 5 sub-jobs (3 completed, 1 running, 1 pending)
  // -------------------------------------------------------------------
  const batchRootId = 'wf_batch_orders'
  const batchFanOutId = 'fo_batch_orders'
  const batchRootCreated = minutesAgo(30)
  const batchRootStarted = minutesAgo(29)

  workflowJobs.push(makeJob({
    id: batchRootId,
    type: 'process-batch-orders',
    status: 'running',
    queue: 'critical',
    tenant: 'acme',
    metadata: { region: 'us-east', workflow: 'batch-orders' },
    createdAt: batchRootCreated,
    startedAt: batchRootStarted,
    completedAt: null,
    priority: 8,
    attempt: 1,
    maxRetries: 3,
    args: jsonBytes({ batchId: 'BATCH-2024-001', orderCount: 5 }),
    lastError: '',
  }))

  fanOuts.push({
    id: batchFanOutId,
    parentJobId: batchRootId,
    totalCount: 5,
    completedCount: 3,
    failedCount: 0,
    cancelledCount: 0,
    strategy: 'fail_fast',
    threshold: 0,
    status: 'pending',
    cancelOnFail: false,
    createdAt: batchRootCreated,
    updatedAt: minutesAgo(5),
  })

  // 3 completed sub-jobs (staggered)
  for (let i = 0; i < 3; i++) {
    const subCreated = new Date(batchRootStarted.getTime() + i * 90_000)
    const subStarted = new Date(subCreated.getTime() + 2_000)
    const subCompleted = new Date(subStarted.getTime() + 45_000)
    workflowJobs.push(makeJob({
      id: `sub_batch_${i + 1}`,
      type: 'process-single-order',
      status: 'completed',
      queue: 'critical',
      tenant: 'acme',
      metadata: { region: 'us-east', workflow: 'batch-orders' },
      parentJobId: batchRootId,
      rootJobId: batchRootId,
      fanOutId: batchFanOutId,
      fanOutIndex: i,
      createdAt: subCreated,
      startedAt: subStarted,
      completedAt: subCompleted,
      priority: 8,
      attempt: 1,
      maxRetries: 3,
      args: jsonBytes({ orderId: `ORD-${10001 + i}`, items: randInt(1, 5) }),
      lastError: '',
    }))
  }

  // 1 running sub-job
  const runningSubCreated = new Date(batchRootStarted.getTime() + 3 * 90_000)
  const runningSubStarted = new Date(runningSubCreated.getTime() + 2_000)
  workflowJobs.push(makeJob({
    id: 'sub_batch_4',
    type: 'process-single-order',
    status: 'running',
    queue: 'critical',
    tenant: 'acme',
    metadata: { region: 'us-east', workflow: 'batch-orders' },
    parentJobId: batchRootId,
    rootJobId: batchRootId,
    fanOutId: batchFanOutId,
    fanOutIndex: 3,
    createdAt: runningSubCreated,
    startedAt: runningSubStarted,
    completedAt: null,
    priority: 8,
    attempt: 1,
    maxRetries: 3,
    args: jsonBytes({ orderId: 'ORD-10004', items: 3 }),
    lastError: '',
  }))

  // 1 pending sub-job
  const pendingSubCreated = new Date(batchRootStarted.getTime() + 4 * 90_000)
  workflowJobs.push(makeJob({
    id: 'sub_batch_5',
    type: 'process-single-order',
    status: 'pending',
    queue: 'critical',
    tenant: 'acme',
    metadata: { region: 'us-east', workflow: 'batch-orders' },
    parentJobId: batchRootId,
    rootJobId: batchRootId,
    fanOutId: batchFanOutId,
    fanOutIndex: 4,
    createdAt: pendingSubCreated,
    startedAt: null,
    completedAt: null,
    priority: 8,
    attempt: 0,
    maxRetries: 3,
    args: jsonBytes({ orderId: 'ORD-10005', items: 2 }),
    lastError: '',
  }))

  // -------------------------------------------------------------------
  // Scenario 2: ETL Pipeline (CollectAll) - all completed
  // -------------------------------------------------------------------
  const etlRootId = 'wf_etl_pipeline'
  const etlFanOutId = 'fo_etl_pipeline'
  const etlRootCreated = minutesAgo(45)
  const etlRootStarted = minutesAgo(44)
  const etlRootCompleted = minutesAgo(10)

  workflowJobs.push(makeJob({
    id: etlRootId,
    type: 'etl-pipeline',
    status: 'running',
    queue: 'default',
    tenant: 'globex',
    metadata: { region: 'eu-west', workflow: 'etl' },
    createdAt: etlRootCreated,
    startedAt: etlRootStarted,
    completedAt: null,
    priority: 5,
    attempt: 1,
    maxRetries: 3,
    args: jsonBytes({ pipelineId: 'ETL-2024-Q1', source: 's3://data-lake/raw', destination: 'postgres://warehouse' }),
    lastError: '',
  }))

  fanOuts.push({
    id: etlFanOutId,
    parentJobId: etlRootId,
    totalCount: 4,
    completedCount: 4,
    failedCount: 0,
    cancelledCount: 0,
    strategy: 'collect_all',
    threshold: 0,
    status: 'completed',
    cancelOnFail: false,
    createdAt: etlRootCreated,
    updatedAt: etlRootCompleted,
  })

  // 4 completed sub-jobs (staggered timing)
  for (let i = 0; i < 4; i++) {
    const subCreated = new Date(etlRootStarted.getTime() + i * 120_000)
    const subStarted = new Date(subCreated.getTime() + 3_000)
    const subCompleted = new Date(subStarted.getTime() + 8 * 60_000 + i * 30_000)
    workflowJobs.push(makeJob({
      id: `sub_etl_${i + 1}`,
      type: 'transform-record-batch',
      status: 'completed',
      queue: 'default',
      tenant: 'globex',
      metadata: { region: 'eu-west', workflow: 'etl' },
      parentJobId: etlRootId,
      rootJobId: etlRootId,
      fanOutId: etlFanOutId,
      fanOutIndex: i,
      createdAt: subCreated,
      startedAt: subStarted,
      completedAt: subCompleted,
      priority: 5,
      attempt: 1,
      maxRetries: 3,
      args: jsonBytes({ batchIndex: i, recordCount: randInt(500, 2000), schema: 'orders_v2' }),
      lastError: '',
    }))
  }

  // -------------------------------------------------------------------
  // Scenario 3: Failed Bulk Notifications (FailFast with cancellation)
  // -------------------------------------------------------------------
  const notifRootId = 'wf_failed_notifications'
  const notifFanOutId = 'fo_failed_notifications'
  const notifRootCreated = minutesAgo(20)
  const notifRootStarted = minutesAgo(19)
  const notifRootCompleted = minutesAgo(15)
  const cancellationTime = minutesAgo(16)

  workflowJobs.push(makeJob({
    id: notifRootId,
    type: 'send-bulk-notifications',
    status: 'failed',
    queue: 'emails',
    tenant: 'initech',
    metadata: { region: 'us-west', workflow: 'notifications' },
    createdAt: notifRootCreated,
    startedAt: notifRootStarted,
    completedAt: notifRootCompleted,
    priority: 3,
    attempt: 1,
    maxRetries: 3,
    args: jsonBytes({ campaignId: 'CAMP-2024-PROMO', recipientCount: 6 }),
    lastError: 'fan-out failed: SMTP connection refused',
  }))

  fanOuts.push({
    id: notifFanOutId,
    parentJobId: notifRootId,
    totalCount: 6,
    completedCount: 2,
    failedCount: 1,
    cancelledCount: 3,
    strategy: 'fail_fast',
    threshold: 0,
    status: 'failed',
    cancelOnFail: true,
    createdAt: notifRootCreated,
    updatedAt: cancellationTime,
  })

  // 2 completed sub-jobs
  for (let i = 0; i < 2; i++) {
    const subCreated = new Date(notifRootStarted.getTime() + i * 30_000)
    const subStarted = new Date(subCreated.getTime() + 1_000)
    const subCompleted = new Date(subStarted.getTime() + 20_000)
    workflowJobs.push(makeJob({
      id: `sub_notif_${i + 1}`,
      type: 'send-notification',
      status: 'completed',
      queue: 'emails',
      tenant: 'initech',
      metadata: { region: 'us-west', workflow: 'notifications' },
      parentJobId: notifRootId,
      rootJobId: notifRootId,
      fanOutId: notifFanOutId,
      fanOutIndex: i,
      createdAt: subCreated,
      startedAt: subStarted,
      completedAt: subCompleted,
      priority: 3,
      attempt: 1,
      maxRetries: 3,
      args: jsonBytes({ recipientId: `user_${1000 + i}`, channel: 'email', template: 'promo-2024' }),
      lastError: '',
    }))
  }

  // 1 failed sub-job
  const failedSubCreated = new Date(notifRootStarted.getTime() + 2 * 30_000)
  const failedSubStarted = new Date(failedSubCreated.getTime() + 1_000)
  const failedSubCompleted = new Date(failedSubStarted.getTime() + 12_000)
  workflowJobs.push(makeJob({
    id: 'sub_notif_3',
    type: 'send-notification',
    status: 'failed',
    queue: 'emails',
    tenant: 'initech',
    metadata: { region: 'us-west', workflow: 'notifications' },
    parentJobId: notifRootId,
    rootJobId: notifRootId,
    fanOutId: notifFanOutId,
    fanOutIndex: 2,
    createdAt: failedSubCreated,
    startedAt: failedSubStarted,
    completedAt: failedSubCompleted,
    priority: 3,
    attempt: 1,
    maxRetries: 3,
    args: jsonBytes({ recipientId: 'user_1002', channel: 'email', template: 'promo-2024' }),
    lastError: 'SMTP connection refused',
  }))

  // 3 cancelled sub-jobs (startedAt null, completedAt = cancellation time)
  for (let i = 0; i < 3; i++) {
    const subCreated = new Date(notifRootStarted.getTime() + (3 + i) * 30_000)
    workflowJobs.push({
      id: `sub_notif_${4 + i}`,
      type: 'send-notification',
      status: 'cancelled',
      queue: 'emails',
      tenant: 'initech',
      metadata: { region: 'us-west', workflow: 'notifications' },
      parentJobId: notifRootId,
      rootJobId: notifRootId,
      fanOutId: notifFanOutId,
      fanOutIndex: 3 + i,
      createdAt: subCreated,
      startedAt: null,
      completedAt: cancellationTime,
      priority: 3,
      attempt: 0,
      maxRetries: 3,
      args: jsonBytes({ recipientId: `user_${1003 + i}`, channel: 'email', template: 'promo-2024' }),
      lastError: '',
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
    overdue: false,
    missedFires: i64(0),
  }),
  new ScheduledJobInfo({
    name: 'sync-inventory',
    schedule: '*/15 * * * *',
    queue: 'default',
    nextRun: ts(new Date(Date.now() + 5 * 60_000)),
    lastRun: ts(minutesAgo(55)),
    overdue: true,
    missedFires: i64(3),
    expectedLastRun: ts(minutesAgo(10)),
  }),
  new ScheduledJobInfo({
    name: 'weekly-digest-emails',
    schedule: '0 9 * * 1',
    queue: 'emails',
    nextRun: ts(new Date(Date.now() + 3 * 86400_000)),
    lastRun: ts(new Date(Date.now() - 4 * 86400_000)),
    overdue: false,
    missedFires: i64(0),
  }),
  new ScheduledJobInfo({
    name: 'stale-lock-reaper',
    schedule: '@every 5m',
    queue: 'critical',
    nextRun: ts(new Date(Date.now() + 2 * 60_000)),
    lastRun: ts(minutesAgo(3)),
    overdue: false,
    missedFires: i64(0),
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
    seedWorkflows()
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
      j.deadLetteredAt = undefined
      j.deadLetterReason = ''
    } else {
      j.status = 'completed'
      j.deadLetteredAt = undefined
      j.deadLetterReason = ''
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
    tenant: j.tenant ?? '',
    metadata: j.metadata ?? {},
    status: j.status,
    priority: j.priority,
    attempt: j.attempt,
    maxRetries: j.maxRetries,
    args: j.args as Uint8Array<ArrayBuffer>,
    lastError: j.lastError,
    createdAt: ts(j.createdAt),
    startedAt: j.startedAt ? ts(j.startedAt) : undefined,
    completedAt: j.completedAt ? ts(j.completedAt) : undefined,
    deadLetteredAt: j.deadLetteredAt ? ts(j.deadLetteredAt) : undefined,
    deadLetterReason: j.deadLetterReason ?? '',
    worker: j.worker ?? '',
    parentJobId: j.parentJobId,
    rootJobId: j.rootJobId,
    fanOutId: j.fanOutId,
    fanOutIndex: j.fanOutIndex ?? 0,
    result: j.result ? (j.result as Uint8Array<ArrayBuffer>) : undefined,
  })
}

function fanOutToProto(fo: MockFanOut): ProtoFanOut {
  return new ProtoFanOut({
    id: fo.id,
    parentJobId: fo.parentJobId,
    totalCount: fo.totalCount,
    completedCount: fo.completedCount,
    failedCount: fo.failedCount,
    cancelledCount: fo.cancelledCount,
    strategy: fo.strategy,
    threshold: fo.threshold,
    status: fo.status,
    cancelOnFail: fo.cancelOnFail,
    createdAt: ts(fo.createdAt),
    updatedAt: ts(fo.updatedAt),
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
  const pending = inQueue.filter((j) => j.status === 'pending')
  const oldestPending = pending.reduce<Date | null>((oldest, job) => {
    if (oldest === null || job.createdAt.getTime() < oldest.getTime()) return job.createdAt
    return oldest
  }, null)
  return new QueueStats({
    name: queueName,
    pending: i64(countByStatus(inQueue, 'pending')),
    running: i64(countByStatus(inQueue, 'running')),
    completed: i64(countByStatus(inQueue, 'completed')),
    failed: i64(countByStatus(inQueue, 'failed')),
    paused: i64(countByStatus(inQueue, 'paused')),
    retrying: i64(countByStatus(inQueue, 'retrying')),
    waiting: i64(countByStatus(inQueue, 'waiting')),
    cancelled: i64(countByStatus(inQueue, 'cancelled')),
    isPaused: pausedQueues.has(queueName),
    oldestPendingAt: oldestPending ? ts(oldestPending) : undefined,
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
      totalPaused: i64(countByStatus(jobs, 'paused')),
      totalRetrying: i64(countByStatus(jobs, 'retrying')),
      totalWaiting: i64(countByStatus(jobs, 'waiting')),
      totalCancelled: i64(countByStatus(jobs, 'cancelled')),
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
      tenant?: string
      type?: string
      search?: string
      metaContains?: Record<string, string>
      page?: number
      limit?: number
      sortKey?: string
      sortDir?: string
    },
  ): Promise<ListJobsResponse> {
    ensureSimulation()

    let filtered = [...jobs, ...workflowJobs]
    if (req.status === 'dead-lettered') {
      filtered = filtered.filter((j) => j.deadLetteredAt)
    } else if (req.status) {
      filtered = filtered.filter((j) => j.status === req.status)
    }
    if (req.queue) filtered = filtered.filter((j) => j.queue === req.queue)
    if (req.tenant) filtered = filtered.filter((j) => (j.tenant ?? '') === req.tenant)
    if (req.type) filtered = filtered.filter((j) => j.type === req.type)
    if (req.metaContains) {
      filtered = filtered.filter((j) =>
        Object.entries(req.metaContains ?? {}).every(([key, value]) => (j.metadata ?? {})[key] === value),
      )
    }
    if (req.search) {
      const s = req.search.toLowerCase()
      filtered = filtered.filter(
        (j) =>
          j.id.toLowerCase().includes(s) ||
          j.type.toLowerCase().includes(s) ||
          j.queue.toLowerCase().includes(s) ||
          (j.tenant ?? '').toLowerCase().includes(s) ||
          Object.entries(j.metadata ?? {}).some(([key, value]) =>
            key.toLowerCase().includes(s) || value.toLowerCase().includes(s),
          ),
      )
    }

    // Server-side sort parity: honor the same whitelisted sort_key/sort_dir the
    // real backend applies so the published demo doesn't reintroduce the
    // misleading "page-local" sort affordance. An EMPTY sort_key means "use the
    // server's per-view default": dead_lettered_at desc for the dead-letter view
    // (most-recently-dead first), created_at desc everywhere else.
    if (!req.sortKey) {
      if (req.status === 'dead-lettered') {
        filtered.sort((a, b) => (b.deadLetteredAt?.getTime() ?? 0) - (a.deadLetteredAt?.getTime() ?? 0))
      } else {
        filtered.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())
      }
    } else {
      const dir = req.sortDir === 'asc' ? 1 : -1
      const sortVal = (j: (typeof filtered)[number]): string | number => {
        switch (req.sortKey) {
          case 'priority': return j.priority ?? 0
          case 'attempt': return j.attempt ?? 0
          case 'type': return j.type
          case 'queue': return j.queue
          case 'status': return j.status
          default: return j.createdAt.getTime() // unknown key fallback
        }
      }
      filtered.sort((a, b) => {
        const av = sortVal(a)
        const bv = sortVal(b)
        let cmp: number
        if (typeof av === 'string' && typeof bv === 'string') cmp = av.localeCompare(bv)
        else cmp = Number(av) - Number(bv)
        if (cmp !== 0) return cmp * dir
        return b.createdAt.getTime() - a.createdAt.getTime() // stable created_at tiebreak
      })
    }

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
    const allJobs = [...jobs, ...workflowJobs]
    const j = allJobs.find((job) => job.id === req.id)
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
      j.deadLetteredAt = undefined
      j.deadLetterReason = ''
      j.attempt = 0
    }
    return new RetryJobResponse({
      job: j ? toProtoJob(j) : undefined,
    })
  },

  async deleteJob(
    req: { id: string; deleteSubtree?: boolean },
  ): Promise<DeleteJobResponse> {
    ensureSimulation()
    if (req.deleteSubtree) {
      // Workflow-aware delete: remove the root and its whole subtree (anything
      // referencing it as parent or root), mirroring DeleteWorkflowSubtree.
      const isSubtree = (j: { id: string; parentJobId?: string; rootJobId?: string }) =>
        j.id === req.id || j.parentJobId === req.id || j.rootJobId === req.id
      for (const arr of [jobs, workflowJobs]) {
        for (let i = arr.length - 1; i >= 0; i--) {
          if (isSubtree(arr[i])) arr.splice(i, 1)
        }
      }
      return new DeleteJobResponse({})
    }
    const idx = jobs.findIndex((job) => job.id === req.id)
    if (idx !== -1) {
      jobs.splice(idx, 1)
    }
    return new DeleteJobResponse({})
  },

  async cancelJob(
    req: { id: string },
  ): Promise<CancelJobResponse> {
    ensureSimulation()
    const j = jobs.find((job) => job.id === req.id)
    if (j) {
      j.status = 'cancelled'
      j.lastError = 'cancelled by user'
      j.completedAt = new Date()
      j.deadLetteredAt = undefined
      j.deadLetterReason = ''
    }
    return new CancelJobResponse({
      job: j ? toProtoJob(j) : undefined,
    })
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

  async getWorkflow(req: { jobId: string }): Promise<GetWorkflowResponse> {
    ensureSimulation()
    // Find the job in both arrays
    const allJobs = [...jobs, ...workflowJobs]
    let job = allJobs.find(j => j.id === req.jobId)
    if (!job) return new GetWorkflowResponse({})

    // Walk to root
    let root = job
    let depth = 0
    while (root.parentJobId && depth < 100) {
      const parent = allJobs.find(j => j.id === root.parentJobId)
      if (!parent) break
      root = parent
      depth++
    }

    // Collect all fan-outs for root
    const rootFanOuts = fanOuts.filter(fo => fo.parentJobId === root.id)
    // Collect all children
    const children = workflowJobs.filter(j => j.rootJobId === root.id && j.id !== root.id)

    return new GetWorkflowResponse({
      root: toProtoJob(root),
      fanOuts: rootFanOuts.map(fanOutToProto),
      children: children.map(toProtoJob),
    })
  },

  async pauseJob(
    req: { id: string },
  ): Promise<PauseJobResponse> {
    ensureSimulation()
    const j = [...jobs, ...workflowJobs].find((job) => job.id === req.id)
    if (j && (j.status === 'pending' || j.status === 'running')) {
      j.status = 'paused'
    }
    return new PauseJobResponse({})
  },

  async resumeJob(
    req: { id: string },
  ): Promise<ResumeJobResponse> {
    ensureSimulation()
    const j = [...jobs, ...workflowJobs].find((job) => job.id === req.id)
    if (j && j.status === 'paused') {
      j.status = 'pending'
    }
    return new ResumeJobResponse({})
  },

  async pauseQueue(
    req: { name: string },
  ): Promise<PauseQueueResponse> {
    ensureSimulation()
    pausedQueues.add(req.name)
    return new PauseQueueResponse({})
  },

  async resumeQueue(
    req: { name: string },
  ): Promise<ResumeQueueResponse> {
    ensureSimulation()
    pausedQueues.delete(req.name)
    return new ResumeQueueResponse({})
  },

  async listWorkflows(req: { page?: number; limit?: number; status?: string }): Promise<ListWorkflowsResponse> {
    ensureSimulation()
    // Root jobs are workflow jobs with no parentJobId
    let roots = workflowJobs.filter(j => !j.parentJobId)
    if (req.status) roots = roots.filter(j => j.status === req.status)

    roots.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())

    const total = roots.length
    const page = req.page ?? 1
    const limit = req.limit ?? 20
    const start = (page - 1) * limit
    const paged = roots.slice(start, start + limit)

    const summaries = paged.map(root => {
      const rootFanOuts = fanOuts.filter(fo => fo.parentJobId === root.id)
      const children = workflowJobs.filter(j => j.rootJobId === root.id && j.id !== root.id)
      return new WorkflowSummary({
        rootJob: toProtoJob(root),
        totalJobs: children.length,
        completedJobs: children.filter(j => j.status === 'completed').length,
        failedJobs: children.filter(j => j.status === 'failed').length,
        runningJobs: children.filter(j => j.status === 'running').length,
        strategy: rootFanOuts[0]?.strategy ?? '',
      })
    })

    return new ListWorkflowsResponse({
      workflows: summaries,
      total: i64(total),
      page,
    })
  },
}
