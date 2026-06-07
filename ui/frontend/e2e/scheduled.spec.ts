import { test, expect } from '@playwright/test'

test.describe('Scheduled Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/scheduled')
    await page.waitForSelector('.scheduled-page', { timeout: 10000 })
  })

  test('shows scheduled heading', async ({ page }) => {
    await expect(page.locator('h2')).toHaveText('Scheduled')
  })

  test('page loads without route error', async ({ page }) => {
    await expect(page.locator('.scheduled-page')).toBeVisible()
    await expect(page.locator('.filters input[name="name"]')).toBeVisible()
    await expect(page.locator('.filters input[name="queue"]')).toBeVisible()
  })

  test('shows registered schedule table', async ({ page }) => {
    const row = page.locator('.scheduled-table tbody tr', { hasText: 'cleanup' })
    await expect(row).toBeVisible()
    await expect(row.locator('.job-name')).toHaveText('cleanup')
    await expect(row.locator('.cron-cell code')).toHaveText('*/5 * * * *')
    await expect(row.locator('.cron-cell span')).toHaveText('every 5 minutes')
  })

  test('shows populated next run and never-fired last run', async ({ page }) => {
    const row = page.locator('.scheduled-table tbody tr', { hasText: 'cleanup' })
    const cells = row.locator('td')
    await expect(cells.nth(3)).not.toHaveText('—')
    await expect(cells.nth(3)).toContainText('in ')
    await expect(cells.nth(4)).toHaveText('—')
  })

  test('shows scheduled queue', async ({ page }) => {
    const row = page.locator('.scheduled-table tbody tr', { hasText: 'cleanup' })
    await expect(row.locator('.mono')).toHaveText('maintenance')
  })

  test('summary shows schedule count', async ({ page }) => {
    await expect(page.locator('.summary')).toContainText('1 of 1 scheduled jobs')
  })

  test('filter by name works', async ({ page }) => {
    await page.locator('.filters input[name="name"]').fill('cleanup')
    await expect(page.locator('.scheduled-table tbody tr')).toHaveCount(1)
    await page.locator('.filters input[name="name"]').fill('missing')
    await expect(page.locator('.empty')).toContainText('No scheduled jobs match')
  })

  test('shows empty state when API returns no schedules', async ({ page }) => {
    await page.route(url => new URL(url).pathname === '/jobs.v1.JobsService/ListScheduledJobs', async route => {
      const response = await route.fetch()
      const body = await response.json()
      const message = body.result ?? body
      message.jobs = []
      const headers = { ...response.headers(), 'content-type': 'application/json' }
      delete headers['content-length']
      await route.fulfill({ status: response.status(), headers, body: JSON.stringify(body) })
    })
    await page.reload()
    const empty = page.locator('.empty')
    await expect(empty).toBeVisible()
    await expect(empty).toContainText('No scheduled jobs')
    await expect(empty.getByText('queue.Schedule()')).toBeVisible()
  })
})
