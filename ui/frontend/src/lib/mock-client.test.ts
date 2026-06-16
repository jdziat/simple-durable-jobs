import { describe, expect, test } from 'vitest'
import { mockJobsClient } from './mock-client'

// Packet I: an empty sort_key means "use the server's per-view default" — the
// mock client mirrors the real backend (dead_lettered_at desc for the
// dead-letter view, created_at desc everywhere else) so the published demo and
// e2e exercise the same behavior the dashboard relies on.

function ms(t: { toDate(): Date } | undefined): number {
  return t ? t.toDate().getTime() : 0
}

describe('mockJobsClient.listJobs sort defaults', () => {
  test('empty sort_key on the default view orders by created_at desc', async () => {
    const resp = await mockJobsClient.listJobs({})
    expect(resp.jobs.length).toBeGreaterThan(1)
    for (let i = 1; i < resp.jobs.length; i++) {
      expect(ms(resp.jobs[i - 1].createdAt)).toBeGreaterThanOrEqual(ms(resp.jobs[i].createdAt))
    }
  })

  test('empty sort_key on the dead-letter view orders by dead_lettered_at desc', async () => {
    const resp = await mockJobsClient.listJobs({ status: 'dead-lettered' })
    expect(resp.jobs.length).toBeGreaterThan(0)
    for (const j of resp.jobs) {
      expect(j.deadLetteredAt).toBeDefined() // only dead-lettered jobs, never created_at-forced
    }
    for (let i = 1; i < resp.jobs.length; i++) {
      expect(ms(resp.jobs[i - 1].deadLetteredAt)).toBeGreaterThanOrEqual(ms(resp.jobs[i].deadLetteredAt))
    }
  })

  test('an explicit sort_key overrides the default', async () => {
    const resp = await mockJobsClient.listJobs({ sortKey: 'type', sortDir: 'asc' })
    expect(resp.jobs.length).toBeGreaterThan(1)
    for (let i = 1; i < resp.jobs.length; i++) {
      expect(resp.jobs[i - 1].type.localeCompare(resp.jobs[i].type)).toBeLessThanOrEqual(0)
    }
  })
})
