export type JobStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'dead-lettered'
  | 'cancelled'
  | 'paused'
  | 'waiting'
  | 'retrying'

export interface StatusToken {
  role: string
  color: string
  bg: string
  label: string
  dashed?: boolean
  hatch?: boolean
}

const tokenByRole = (role: string) => ({
  color: `var(--sig-${role})`,
  bg: `var(--sig-${role}-bg)`,
})

const STATUS_TOKENS = {
  pending: { role: 'warn', ...tokenByRole('warn'), label: 'pending' },
  running: { role: 'info', ...tokenByRole('info'), label: 'running' },
  completed: { role: 'ok', ...tokenByRole('ok'), label: 'completed' },
  failed: { role: 'danger', ...tokenByRole('danger'), label: 'failed' },
  'dead-lettered': { role: 'danger', ...tokenByRole('danger'), label: 'Dead-lettered', dashed: true },
  cancelled: { role: 'neutral', ...tokenByRole('neutral'), label: 'cancelled' },
  paused: { role: 'paused', ...tokenByRole('paused'), label: 'paused', hatch: true },
  waiting: { role: 'purple', ...tokenByRole('purple'), label: 'waiting' },
  retrying: { role: 'warn', ...tokenByRole('warn'), label: 'retrying' },
} satisfies Record<JobStatus, StatusToken>

export function statusToken(status: string): StatusToken {
  if (status in STATUS_TOKENS) return STATUS_TOKENS[status as JobStatus]
  return { role: 'neutral', ...tokenByRole('neutral'), label: status || 'unknown' }
}
