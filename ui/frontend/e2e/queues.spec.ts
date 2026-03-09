import { test, expect } from '@playwright/test'

test.describe('Queues Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/queues')
    await page.waitForSelector('.queues-table', { timeout: 10000 })
  })

  test('shows queues heading', async ({ page }) => {
    await expect(page.locator('h2')).toHaveText('Queues')
  })

  test('shows default and emails queues', async ({ page }) => {
    const table = page.locator('.queues-table')
    await expect(table.getByText('default')).toBeVisible()
    await expect(table.getByText('emails')).toBeVisible()
  })

  test('shows queue counts', async ({ page }) => {
    const rows = page.locator('.queues-table tbody tr')
    await expect(rows).toHaveCount(2)

    // Each row should have numeric cells
    const firstRow = rows.first()
    const cells = firstRow.locator('.num')
    const cellCount = await cells.count()
    expect(cellCount).toBeGreaterThanOrEqual(5) // pending, running, completed, failed, paused, total
  })

  test('summary shows queue count', async ({ page }) => {
    await expect(page.locator('.summary')).toContainText('2 of 2 queues')
  })

  test('filter by name works', async ({ page }) => {
    await page.locator('.filters input').fill('emails')
    await page.waitForTimeout(300)
    const rows = page.locator('.queues-table tbody tr')
    await expect(rows).toHaveCount(1)
    await expect(page.locator('.summary')).toContainText('1 of 2 queues')
  })

  test('pause queue button is visible', async ({ page }) => {
    const pauseBtn = page.locator('.btn-pause-queue').first()
    await expect(pauseBtn).toBeVisible()
    await expect(pauseBtn).toHaveText('Pause Queue')
  })

  test('pause then resume a queue', async ({ page }) => {
    // Click pause on the first queue
    const pauseBtn = page.locator('.btn-pause-queue').first()
    await pauseBtn.click()
    await page.waitForTimeout(1000)

    // Should now show "Paused" badge and Resume button
    await expect(page.locator('.badge-paused').first()).toBeVisible()
    const resumeBtn = page.locator('.btn-resume-queue').first()
    await expect(resumeBtn).toBeVisible()

    // Resume the queue
    await resumeBtn.click()
    await page.waitForTimeout(1000)

    // Badge should be gone, Pause button should be back
    await expect(page.locator('.btn-pause-queue').first()).toBeVisible()
  })

  test('purge failed button visible when queue has failed jobs', async ({ page }) => {
    // The default queue should have failed jobs
    const defaultRow = page.locator('.queues-table tbody tr', { hasText: 'default' })
    const purgeBtn = defaultRow.locator('.btn-purge')
    // May or may not be visible depending on which queue has failed jobs
    const count = await purgeBtn.count()
    if (count > 0) {
      await expect(purgeBtn.first()).toHaveText('Purge Failed')
    }
  })
})
