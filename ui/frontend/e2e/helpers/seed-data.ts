// Job IDs matching the Go test server seed data
export const JOBS = {
  PENDING_1: 'e2e-pending-001',
  PENDING_2: 'e2e-pending-002',
  PENDING_3: 'e2e-pending-003',
  PENDING_4: 'e2e-pending-004',
  PENDING_5: 'e2e-pending-005',
  RUNNING_1: 'e2e-running-001',
  RUNNING_2: 'e2e-running-002',
  RUNNING_3: 'e2e-running-003',
  COMPLETED_1: 'e2e-completed-001',
  COMPLETED_2: 'e2e-completed-002',
  FAILED_1: 'e2e-failed-001',
  FAILED_2: 'e2e-failed-002',
  PAUSED_1: 'e2e-paused-001',
  PAUSED_2: 'e2e-paused-002',
  CANCELLED_1: 'e2e-cancelled-001',
  WORKFLOW_ROOT: 'e2e-workflow-root',
  WORKFLOW_CHILD_1: 'e2e-workflow-child-001',
  WORKFLOW_CHILD_2: 'e2e-workflow-child-002',
  WORKFLOW_CHILD_3: 'e2e-workflow-child-003',
} as const

export const QUEUES = ['default', 'emails'] as const

// Expected counts based on seed data
export const COUNTS = {
  PENDING: 5,
  RUNNING: 3 + 1, // 3 standalone + 1 workflow running child (+ workflow root running)
  COMPLETED: 10 + 1, // 10 standalone + 1 workflow completed child
  FAILED: 5,
  PAUSED: 2,
  CANCELLED: 1,
  TOTAL: 30, // 5+3+10+5+2+1+4(workflow)
} as const

export const FAILED_ERRORS = [
  'connection timeout after 30s',
  'record not found: user_id=12345',
  'rate limit exceeded, retry after 60s',
  'invalid input: email format',
  'external API returned 503',
] as const
