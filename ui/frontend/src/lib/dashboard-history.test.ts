import { describe, expect, test } from 'vitest'
import { mergeHistory } from './dashboardHistory'

function point(ts: string, value: number) {
  const date = new Date(ts)
  return {
    timestamp: { toDate: () => date },
    value,
  }
}

describe('mergeHistory', () => {
  test('aligns sparse completion and failure buckets by timestamp union', () => {
    const completedOnly = '2026-06-07T12:17:00.000Z'
    const shared = '2026-06-07T12:20:00.000Z'
    const failureOnly = '2026-06-07T12:23:00.000Z'

    const merged = mergeHistory(
      [point(completedOnly, 12), point(shared, 5)],
      [point(shared, 1), point(failureOnly, 2)]
    )

    expect(merged.completed).toEqual([12, 5, 0])
    expect(merged.failed).toEqual([0, 1, 2])
    expect(merged.labels).toHaveLength(3)
  })
})
