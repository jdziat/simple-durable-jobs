import { v5 as uuidv5 } from 'uuid'

const e2eId = (name: string) => uuidv5(`sdj-e2e/${name}`, uuidv5.URL)

// Job IDs matching the Go test server seed data
export const JOBS = {
  PENDING_1: e2eId('e2e-pending-001'),
  PENDING_2: e2eId('e2e-pending-002'),
  PENDING_3: e2eId('e2e-pending-003'),
  PENDING_4: e2eId('e2e-pending-004'),
  PENDING_5: e2eId('e2e-pending-005'),
  RUNNING_1: e2eId('e2e-running-001'),
  RUNNING_2: e2eId('e2e-running-002'),
  RUNNING_3: e2eId('e2e-running-003'),
  COMPLETED_1: e2eId('e2e-completed-001'),
  COMPLETED_2: e2eId('e2e-completed-002'),
  FAILED_1: e2eId('e2e-failed-001'),
  FAILED_2: e2eId('e2e-failed-002'),
  // e2e-failed-003 is the dead-lettered fixture. It is deliberately a job no
  // other test navigates to or mutates, so FAILED_1/FAILED_2 stay plain failed
  // (their status/retry assertions don't collide with the Dead-lettered badge)
  // and the DLQ assertions stay stable regardless of test order.
  DEADLETTERED_1: e2eId('e2e-failed-003'),
  FAILED_4: e2eId('e2e-failed-004'),
  DEADLETTERED_REQUEUE: e2eId('e2e-failed-006'),
  DEADLETTERED_ACME_2: e2eId('e2e-failed-007'),
  PAUSED_1: e2eId('e2e-paused-001'),
  PAUSED_2: e2eId('e2e-paused-002'),
  CANCELLED_1: e2eId('e2e-cancelled-001'),
  WORKFLOW_ROOT: e2eId('e2e-workflow-root'),
  WORKFLOW_CHILD_1: e2eId('e2e-workflow-child-001'),
  WORKFLOW_CHILD_2: e2eId('e2e-workflow-child-002'),
  WORKFLOW_CHILD_3: e2eId('e2e-workflow-child-003'),
} as const

export const QUEUES = ['default', 'emails'] as const

export const TENANTS = {
  ACME: 'acme',
  BETA: 'beta',
  GLOBEX: 'globex',
} as const

export const METADATA = {
  ACME_PAYMENTS: {
    region: 'us-east',
    team: 'payments',
  },
  ACME_COMPLETED: {
    region: 'us-east',
    team: 'payments',
    tier: 'gold',
  },
  ACME_SUPPORT: {
    region: 'us-west',
    team: 'support',
  },
  BETA_FULFILLMENT: {
    region: 'eu-west',
    team: 'fulfillment',
  },
  GLOBEX_ANALYTICS: {
    region: 'eu-west',
    team: 'analytics',
    tier: 'silver',
  },
} as const

// Expected counts based on seed data
export const COUNTS = {
  PENDING: 5 + 1, // 5 standalone + 1 workflow pending child
  RUNNING: 3 + 2, // 3 standalone + workflow root + 1 workflow running child
  COMPLETED: 10 + 1, // 10 standalone + 1 workflow completed child
  FAILED: 7,
  PAUSED: 2,
  CANCELLED: 1,
  TOTAL: 32, // 5+3+10+7+2+1+4(workflow)
  DEADLETTERED_ACME: 2,
  GLOBEX: 2,
} as const

export const FAILED_ERRORS = [
  'connection timeout after 30s',
  'record not found: user_id=12345',
  'rate limit exceeded, retry after 60s',
  'invalid input: email format',
  'external API returned 503',
] as const
