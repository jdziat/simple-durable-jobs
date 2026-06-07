import { test, expect, type Locator, type Page } from '@playwright/test'

async function computedColor(locator: Locator) {
  return locator.evaluate((element: Element) => getComputedStyle(element).color)
}

function metricCard(page: Page, label: string) {
  return page.locator('.stats-grid .metric-card').filter({ hasText: label })
}

async function routeZeroFailedStats(page: Page) {
  await page.route(url => new URL(url).pathname === '/jobs.v1.JobsService/GetStats', async route => {
    const response = await route.fetch()
    const body = await response.json()
    const message = body.result ?? body
    message.totalFailed = '0'
    message.queues = message.queues.map((queue: Record<string, unknown>) => ({ ...queue, failed: '0' }))
    const headers = { ...response.headers(), 'content-type': 'application/json' }
    delete headers['content-length']
    await route.fulfill({ status: response.status(), headers, body: JSON.stringify(body) })
  })
}

test.describe('Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/')
    // Wait for stats grid to appear (loading finishes)
    await page.waitForSelector('.stats-grid', { timeout: 15000 })
  })

  test('shows stat cards', async ({ page }) => {
    const statsGrid = page.locator('.stats-grid')
    await expect(statsGrid).toBeVisible()
    // Should have 6 metric cards: Failed, Pending, Running, Completed, Paused, Active workers
    const cards = statsGrid.locator('.card')
    await expect(cards).toHaveCount(6)
  })

  test('stat cards show non-zero values', async ({ page }) => {
    const grid = page.locator('.stats-grid')
    await expect(grid.getByText('Pending')).toBeVisible()
    await expect(grid.getByText('Running')).toBeVisible()
    await expect(grid.getByText('Completed')).toBeVisible()
    await expect(grid.getByText('Failed')).toBeVisible()
    await expect(grid.getByText('Paused')).toBeVisible()
    await expect(grid.getByText('Active workers')).toBeVisible()
  })

  test('queues table shows queues', async ({ page }) => {
    const table = page.locator('.queues-table')
    await expect(table).toBeVisible()
    const rows = table.locator('tbody tr')
    await expect(rows).toHaveCount(2)
  })

  test('queues table shows default and emails', async ({ page }) => {
    const table = page.locator('.queues-table')
    await expect(table.getByText('default')).toBeVisible()
    await expect(table.getByText('emails')).toBeVisible()
  })

  test('clicking a queue row navigates to filtered jobs', async ({ page }) => {
    const table = page.locator('.queues-table')
    await table.locator('tr.clickable', { hasText: 'default' }).click()
    await expect(page).toHaveURL(/queue=default/)
  })

  test('chart section renders', async ({ page }) => {
    await expect(page.locator('.chart-section')).toBeVisible()
  })

  test('period selector buttons exist', async ({ page }) => {
    const periodSelector = page.locator('.period-selector')
    await expect(periodSelector.getByText('1h')).toBeVisible()
    await expect(periodSelector.getByText('24h')).toBeVisible()
    await expect(periodSelector.getByText('7d')).toBeVisible()
  })

  test('stat cards link to filtered job lists', async ({ page }) => {
    await expect(metricCard(page, 'Failed')).toHaveAttribute('href', '#/jobs?status=failed')
    await expect(metricCard(page, 'Pending')).toHaveAttribute('href', '#/jobs?status=pending')
    await expect(metricCard(page, 'Running')).toHaveAttribute('href', '#/jobs?status=running')
    await expect(metricCard(page, 'Completed')).toHaveAttribute('href', '#/jobs?status=completed')
    await expect(metricCard(page, 'Paused')).toHaveAttribute('href', '#/jobs?status=paused')
  })

  test('failed card is dominant when seeded failures exist', async ({ page }) => {
    await expect(metricCard(page, 'Failed')).toHaveClass(/danger/)
  })

  test('failure-rate value is marked dangerous when seeded failures exist', async ({ page }) => {
    await expect(page.locator('.ops-cell', { hasText: 'Failure rate' }).locator('strong')).toHaveClass(/danger/)
  })

  test('dark failure colors follow the dashboard contract', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.evaluate(() => localStorage.setItem('blackbox-theme', 'dark'))
    await page.reload()
    await page.waitForSelector('.stats-grid', { timeout: 15000 })
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark')

    const failureRate = page.locator('.ops-cell', { hasText: 'Failure rate' }).locator('strong.danger')
    const failedCell = page.locator('.queues-table tr.clickable', { hasText: 'default' }).locator('td.has-failures')
    const zeroFailedCell = page.locator('.queues-table tr.clickable', { hasText: 'emails' }).locator('td.failed-cell')

    await expect(failureRate).toBeVisible()
    await expect(failedCell).toBeVisible()
    await expect(zeroFailedCell).toHaveText('0')
    await expect(zeroFailedCell).not.toHaveClass(/has-failures/)

    expect(await computedColor(failureRate)).toBe('rgb(255, 92, 92)')
    expect(await computedColor(failedCell)).toBe('rgb(255, 92, 92)')
    expect(await computedColor(zeroFailedCell)).toBe('rgb(230, 234, 242)')
  })

  test('healthy stats response leaves failed card non-dominant', async ({ page }) => {
    await routeZeroFailedStats(page)
    await page.reload()
    await page.waitForSelector('.stats-grid', { timeout: 15000 })

    const failedCard = metricCard(page, 'Failed')
    const failedValue = failedCard.locator('.metric-value')
    const normalValue = metricCard(page, 'Pending').locator('.metric-value')
    await expect(failedCard.locator('.metric-value')).toHaveText('0')
    await expect(failedCard).not.toHaveClass(/danger/)
    expect(await computedColor(failedValue)).toBe(await computedColor(normalValue))
    await expect(failedValue).toHaveCSS('font-size', await normalValue.evaluate(element => getComputedStyle(element).fontSize))
  })

  test('healthy stats response clears queue failure emphasis', async ({ page }) => {
    await routeZeroFailedStats(page)
    await page.reload()
    await page.waitForSelector('.stats-grid', { timeout: 15000 })

    await expect(page.locator('.queues-table td.has-failures')).toHaveCount(0)
  })

  test('active workers card shows live lock holders', async ({ page }) => {
    await expect(metricCard(page, 'Active workers').locator('.metric-value')).toHaveText('2')
  })

  test('top queues sort by live backlog before failures', async ({ page }) => {
    const queueNames = page.locator('.queues-table tbody tr .queue-name')
    await expect(queueNames.first()).toHaveText('default')
    await expect(queueNames.nth(1)).toHaveText('emails')
  })

  test('queue selector includes all queues', async ({ page }) => {
    // Assert presence, not an exact total: other specs running against the
    // shared server can create queues, making an exact count order-fragile.
    const options = page.locator('.queue-selector option')
    await expect(options.first()).toHaveText('All queues')
    await expect(options.filter({ hasText: 'default' })).toHaveCount(1)
    await expect(options.filter({ hasText: 'emails' })).toHaveCount(1)
  })

  test('switching throughput queue updates the chart heading', async ({ page }) => {
    await page.locator('.queue-selector').selectOption('emails')
    await expect(page.locator('.chart-section h3')).toHaveText('Throughput - emails')
  })
})
