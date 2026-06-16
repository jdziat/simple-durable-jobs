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
    await expect(page).toHaveURL(/status=failed/)
    const statuses = page.locator('.jobs-table tbody .status')
    const count = await statuses.count()
    expect(count).toBeGreaterThan(0)
    for (let i = 0; i < count; i++) {
      // A dead-lettered job has status 'failed' and shows under this filter with
      // a distinct "Dead-lettered" badge; both are valid failed-state rows.
      await expect(statuses.nth(i)).toHaveText(/^(failed|Dead-lettered)$/)
    }
  })

  test('filter by dead-lettered status', async ({ page }) => {
    await page.locator('.filters select').selectOption('dead-lettered')
    await page.waitForTimeout(500)
    const statuses = page.locator('.jobs-table tbody .status')
    const count = await statuses.count()
    expect(count).toBeGreaterThan(0)
    // The dead-lettered filter returns only dead-lettered jobs, each badged distinctly.
    for (let i = 0; i < count; i++) {
      await expect(statuses.nth(i)).toHaveText('Dead-lettered')
    }
    await expect(page.locator('.jobs-table tbody .status-dead-lettered').first()).toBeVisible()
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
    await expect(page).toHaveURL(/#\/jobs\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/)
  })

  test('pagination shows total count', async ({ page }) => {
    const pagination = page.locator('.pagination')
    await expect(pagination).toBeVisible()
    await expect(pagination).toContainText('of')
  })

  test('server-side sort caption is visible', async ({ page }) => {
    await expect(page.getByText(/sort the full result set/i)).toBeVisible()
  })

  test('failed jobs show error peek and copy action works', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write'])
    await page.locator('.filters select').selectOption('failed')
    await page.waitForTimeout(500)

    const peek = page.locator('.jobs-table tbody .error-peek').first()
    await expect(peek).toBeVisible()
    await peek.locator('.error-peek-toggle').focus()
    await expect(peek.locator('pre')).toBeVisible()

    const errorText = await peek.locator('pre').innerText()
    await peek.locator('.copy-button').click()
    await expect(page.locator('.toast-ok').filter({ hasText: 'copied' }).first()).toBeVisible()
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(errorText)
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
      const firstRow = page.locator('.jobs-table tbody tr').first()
      await firstRow.locator('.overflow-trigger').click()
      await firstRow.locator('.btn-delete').click()

      const dialog = page.getByRole('dialog')
      await expect(dialog).toBeVisible()
      await expect(dialog).toContainText('Permanently deletes job')
      await dialog.getByRole('textbox').fill('DELETE')
      await dialog.getByRole('button', { name: 'Delete job' }).click()

      await expect(page.locator('.toast-ok').filter({ hasText: 'job deleted' }).first()).toBeVisible()
      const afterCount = await page.locator('.jobs-table tbody tr').count()
      expect(afterCount).toBeLessThan(beforeCount)
    }
  })
})
