import { test, expect } from '@playwright/test'

test.describe('Dashboard', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/')
    // Wait for stats grid to appear (loading finishes)
    await page.waitForSelector('.stats-grid', { timeout: 15000 })
  })

  test('shows stat cards', async ({ page }) => {
    const statsGrid = page.locator('.stats-grid')
    await expect(statsGrid).toBeVisible()
    // Should have 5 stat cards: Pending, Running, Completed, Failed, Paused
    const cards = statsGrid.locator('.card')
    await expect(cards).toHaveCount(5)
  })

  test('stat cards show non-zero values', async ({ page }) => {
    const grid = page.locator('.stats-grid')
    await expect(grid.getByText('Pending')).toBeVisible()
    await expect(grid.getByText('Running')).toBeVisible()
    await expect(grid.getByText('Completed')).toBeVisible()
    await expect(grid.getByText('Failed')).toBeVisible()
    await expect(grid.getByText('Paused')).toBeVisible()
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
})
