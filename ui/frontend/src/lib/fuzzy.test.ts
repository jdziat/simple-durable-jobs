import { describe, expect, test } from 'vitest'
import { fuzzyFilter, fuzzyScore } from './fuzzy'

describe('fuzzy matcher', () => {
  test('scores exact matches highest', () => {
    expect(fuzzyScore('jobs', 'jobs')).toBeGreaterThan(fuzzyScore('jobs', 'open jobs'))
  })

  test('scores subsequence matches and rejects no-match', () => {
    expect(fuzzyScore('jbs', 'jobs')).toBeGreaterThan(0)
    expect(fuzzyScore('zzz', 'jobs')).toBe(0)
  })

  test('ranks stronger matches first', () => {
    const ranked = fuzzyFilter('job', ['open workflow', 'open job', 'jobs'], item => item)
    expect(ranked[0]).toBe('jobs')
    expect(ranked).toContain('open job')
    expect(ranked).not.toContain('open workflow')
  })
})
