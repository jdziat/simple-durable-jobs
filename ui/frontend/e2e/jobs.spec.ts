import { test, expect } from '@playwright/test'

test.describe('Jobs Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/jobs')
    await page.waitForSelector('.jobs-table', { timeout: 10000 })
  })

  test('shows jobs heading', async ({ page }) => {
    await expect(page.locator('h2')).toHaveText('Jobs')
  })

  test('table loads with jobs', async ({ page }) => {
    const rows = page.locator('.jobs-table tbody tr')
    const count = await rows.count()
    expect(count).toBeGreaterThan(0)
  })

  test('filter by failed status', async ({ page }) => {
    await page.locator('.filters select').selectOption('failed')
    await page.waitForTimeout(500)
    const statuses = page.locator('.jobs-table tbody .status')
    const count = await statuses.count()
    expect(count).toBeGreaterThan(0)
    for (let i = 0; i < count; i++) {
      await expect(statuses.nth(i)).toHaveText('failed')
    }
  })

  test('filter by paused status', async ({ page }) => {
    await page.locator('.filters select').selectOption('paused')
    await page.waitForTimeout(500)
    const statuses = page.locator('.jobs-table tbody .status')
    const count = await statuses.count()
    expect(count).toBeGreaterThan(0)
    for (let i = 0; i < count; i++) {
      await expect(statuses.nth(i)).toHaveText('paused')
    }
  })

  test('clear filters resets view', async ({ page }) => {
    await page.locator('.filters select').selectOption('failed')
    await page.waitForTimeout(500)
    const failedCount = await page.locator('.jobs-table tbody tr').count()

    await page.locator('.btn-clear').click()
    await page.waitForTimeout(500)
    const allCount = await page.locator('.jobs-table tbody tr').count()
    expect(allCount).toBeGreaterThanOrEqual(failedCount)
  })

  test('clicking a job row navigates to detail', async ({ page }) => {
    const firstRow = page.locator('.jobs-table tbody tr.clickable').first()
    await firstRow.click()
    await expect(page).toHaveURL(/#\/jobs\/e2e-/)
  })

  test('pagination shows total count', async ({ page }) => {
    const pagination = page.locator('.pagination')
    await expect(pagination).toBeVisible()
    await expect(pagination).toContainText('of')
  })

  test('retry button visible on failed jobs', async ({ page }) => {
    await page.locator('.filters select').selectOption('failed')
    await page.waitForTimeout(500)
    const retryBtn = page.locator('.btn-retry').first()
    await expect(retryBtn).toBeVisible()
  })

  test('pause button visible on pending jobs', async ({ page }) => {
    await page.locator('.filters select').selectOption('pending')
    await page.waitForTimeout(500)
    const pauseBtn = page.locator('.btn-pause').first()
    await expect(pauseBtn).toBeVisible()
  })

  test('resume button visible on paused jobs', async ({ page }) => {
    await page.locator('.filters select').selectOption('paused')
    await page.waitForTimeout(500)
    const resumeBtn = page.locator('.btn-resume').first()
    await expect(resumeBtn).toBeVisible()
  })

  test('retry a failed job changes status', async ({ page }) => {
    await page.locator('.filters select').selectOption('failed')
    await page.waitForTimeout(500)
    const beforeCount = await page.locator('.jobs-table tbody tr').count()
    await page.locator('.btn-retry').first().click()
    await page.waitForTimeout(1000)
    // After retry, the job should no longer be in the failed list
    const afterCount = await page.locator('.jobs-table tbody tr').count()
    expect(afterCount).toBeLessThan(beforeCount)
  })

  test('pause a pending job changes status', async ({ page }) => {
    await page.locator('.filters select').selectOption('pending')
    await page.waitForTimeout(500)
    const beforeCount = await page.locator('.jobs-table tbody tr').count()
    await page.locator('.btn-pause').first().click()
    await page.waitForTimeout(1000)
    const afterCount = await page.locator('.jobs-table tbody tr').count()
    expect(afterCount).toBeLessThan(beforeCount)
  })

  test('delete a job removes it', async ({ page }) => {
    // Use a cancelled job that we don't need for other tests
    await page.locator('.filters select').selectOption('cancelled')
    await page.waitForTimeout(500)
    const beforeCount = await page.locator('.jobs-table tbody tr').count()
    if (beforeCount > 0) {
      page.on('dialog', dialog => dialog.accept())
      await page.locator('.btn-delete').first().click()
      await page.waitForTimeout(1000)
      const afterCount = await page.locator('.jobs-table tbody tr').count()
      expect(afterCount).toBeLessThan(beforeCount)
    }
  })
})
