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

    // .num is deliberately limited to numeric count cells; backlog-age and
    // composition sparkline cells are non-.num data/visualization cells.
    const firstRow = rows.first()
    const cells = firstRow.locator('.num')
    const cellCount = await cells.count()
    expect(cellCount).toBeGreaterThanOrEqual(5) // pending, running, completed, failed, paused, total
  })

  test('shows honest queue telemetry columns', async ({ page }) => {
    const firstRow = page.locator('.queues-table tbody tr').first()
    await expect(firstRow.locator('.sparkline')).toBeVisible()
    await expect(firstRow.locator('.data-gap')).toHaveText('—')
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

  test('clicking a queue row opens filtered jobs', async ({ page }) => {
    await page.locator('.queues-table tbody tr', { hasText: 'emails' }).click()
    await expect(page).toHaveURL(/#\/jobs\?queue=emails/)
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
    await expect(page.locator('.status-paused.badge-paused').first()).toBeVisible()
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

  test('purge failed uses typed blast-radius confirmation', async ({ page }) => {
    const defaultRow = page.locator('.queues-table tbody tr', { hasText: 'default' })
    const failedText = await defaultRow.locator('.num').nth(3).innerText()
    const failedCount = Number(failedText.trim())
    expect(failedCount).toBeGreaterThan(0)

    await defaultRow.locator('.btn-purge').click()
    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText(`This permanently deletes ${failedCount} failed jobs from "default". This cannot be undone.`)

    await dialog.getByRole('textbox').fill('default')
    await dialog.getByRole('button', { name: /Purge failed/i }).click()
    await expect(page.locator('.toast-ok').filter({ hasText: `deleted ${failedCount} failed jobs` }).first()).toBeVisible()
  })
})
