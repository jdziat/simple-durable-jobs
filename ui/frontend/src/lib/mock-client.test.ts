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
    // The seed has >=2 dead-lettered jobs whose dead_lettered_at order DIFFERS
    // from their created_at order, so this asserts the per-view default rather
    // than trivially passing on a single row.
    expect(resp.jobs.length).toBeGreaterThanOrEqual(2)
    for (const j of resp.jobs) {
      expect(j.deadLetteredAt).toBeDefined() // only dead-lettered jobs, never created_at-forced
    }
    for (let i = 1; i < resp.jobs.length; i++) {
      expect(ms(resp.jobs[i - 1].deadLetteredAt)).toBeGreaterThanOrEqual(ms(resp.jobs[i].deadLetteredAt))
    }
    // Guard against a regression to created_at sort: the dead_lettered_at order
    // must NOT coincide with the created_at order (the seed is built so they differ).
    const byDeadLettered = resp.jobs.map((j) => j.id)
    const byCreated = [...resp.jobs].sort((a, b) => ms(b.createdAt) - ms(a.createdAt)).map((j) => j.id)
    expect(byDeadLettered).not.toEqual(byCreated)
  })

  test('an explicit sort_key overrides the default', async () => {
    const resp = await mockJobsClient.listJobs({ sortKey: 'type', sortDir: 'asc' })
    expect(resp.jobs.length).toBeGreaterThan(1)
    for (let i = 1; i < resp.jobs.length; i++) {
      expect(resp.jobs[i - 1].type.localeCompare(resp.jobs[i].type)).toBeLessThanOrEqual(0)
    }
  })
})
