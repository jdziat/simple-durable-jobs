import { describe, expect, test } from 'vitest'
import { statusToken, type JobStatus } from './status'

const cases: Array<[JobStatus, string, string, Partial<ReturnType<typeof statusToken>>]> = [
  ['pending', 'warn', 'pending', {}],
  ['running', 'info', 'running', {}],
  ['completed', 'ok', 'completed', {}],
  ['failed', 'danger', 'failed', {}],
  ['dead-lettered', 'danger', 'Dead-lettered', { dashed: true }],
  ['cancelled', 'neutral', 'cancelled', {}],
  ['paused', 'paused', 'paused', { hatch: true }],
  ['waiting', 'purple', 'waiting', {}],
  ['retrying', 'warn', 'retrying', {}],
]

describe('statusToken', () => {
  test.each(cases)('%s maps to the canonical token', (status, role, label, flags) => {
    const token = statusToken(status)
    expect(token.role).toBe(role)
    expect(token.label).toBe(label)
    expect(token.color).toBe(`var(--sig-${role})`)
    expect(token.bg).toBe(`var(--sig-${role}-bg)`)
    expect(token.dashed).toBe(flags.dashed)
    expect(token.hatch).toBe(flags.hatch)
  })

  test('unknown statuses fall back to neutral without rewriting the label', () => {
    expect(statusToken('blocked')).toEqual({
      role: 'neutral',
      color: 'var(--sig-neutral)',
      bg: 'var(--sig-neutral-bg)',
      label: 'blocked',
    })
    expect(statusToken('').label).toBe('unknown')
  })
})
