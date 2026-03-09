import { test, expect } from '@playwright/test'
import { JOBS } from './helpers/seed-data'

test.describe('Job Detail', () => {
  test('shows job metadata for a completed job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    // Header shows job type and status
    await expect(page.locator('.header h2')).toHaveText('GenerateReport')
    await expect(page.locator('.header .status')).toHaveText('completed')

    // Meta section shows ID, Queue, Priority, Attempts
    const meta = page.locator('.meta')
    await expect(meta).toContainText(JOBS.COMPLETED_1)
  })

  test('shows timestamps', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.timestamps', { timeout: 10000 })

    const timestamps = page.locator('.timestamps')
    await expect(timestamps).toContainText('Created')
    await expect(timestamps).toContainText('Started')
    await expect(timestamps).toContainText('Completed')
  })

  test('shows checkpoints for completed job with checkpoints', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    const checkpoints = page.locator('.checkpoints')
    await expect(checkpoints).toBeVisible()
    await expect(checkpoints.locator('h4')).toHaveText('Checkpoints')

    // Should have 2 checkpoint rows
    const rows = checkpoints.locator('tbody tr')
    await expect(rows).toHaveCount(2)
  })

  test('shows error box for failed job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.FAILED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.header .status')).toHaveText('failed')
    const errorBox = page.locator('.error-box')
    await expect(errorBox).toBeVisible()
    await expect(errorBox.locator('h4')).toHaveText('Last Error')
    await expect(errorBox.locator('pre')).toContainText('connection timeout')
  })

  test('shows arguments section', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.PENDING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    const argsBox = page.locator('.args-box')
    await expect(argsBox).toBeVisible()
    await expect(argsBox.locator('pre')).toContainText('example.com')
  })

  test('retry button on failed job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.FAILED_2}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    const retryBtn = page.locator('.actions .btn-retry')
    await expect(retryBtn).toBeVisible()
    await retryBtn.click()
    await page.waitForTimeout(1000)

    // After retry, status should change to pending
    await expect(page.locator('.header .status')).toHaveText('pending')
  })

  test('resume button on paused job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.PAUSED_2}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.header .status')).toHaveText('paused')
    const resumeBtn = page.locator('.actions .btn-resume')
    await expect(resumeBtn).toBeVisible()
  })

  test('back link navigates to jobs list', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.back-link', { timeout: 10000 })

    await page.locator('.back-link').click()
    await expect(page).toHaveURL(/#\/jobs$/)
  })

  test('delete button navigates back to jobs', async ({ page }) => {
    // Use failed-003 which we can afford to lose
    await page.goto(`/#/jobs/${JOBS.FAILED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    page.on('dialog', dialog => dialog.accept())
    await page.locator('.actions .btn-delete').click()
    await expect(page).toHaveURL(/#\/jobs$/)
  })
})
