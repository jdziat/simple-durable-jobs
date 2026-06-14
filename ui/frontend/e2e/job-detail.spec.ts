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
    await expect(timestamps).toContainText('Wait')
    await expect(timestamps).toContainText('Run')
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
    await expect(checkpoints.getByRole('button', { name: 'Copy' }).first()).toBeVisible()
  })

  test('shows error box for failed job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.FAILED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.header .status')).toHaveText('failed')
    const errorBox = page.locator('.error-box')
    await expect(errorBox).toBeVisible()
    await expect(errorBox.locator('h4')).toHaveText('Last Error')
    await expect(errorBox.locator('pre')).toContainText('connection timeout')
    // A plain failed job (no dead_lettered_at) shows no dead-letter panel.
    await expect(page.locator('.dead-letter-box')).toHaveCount(0)
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
    // Use a plain failed fixture that this file has already inspected.
    await page.goto(`/#/jobs/${JOBS.FAILED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await page.locator('.actions .btn-delete').click()
    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('Permanently deletes job')
    await dialog.getByRole('textbox').fill('DELETE')
    await dialog.getByRole('button', { name: 'Delete job' }).click()
    await expect(page).toHaveURL(/#\/jobs$/)
  })

  test('cancel dialog explains cooperative cancellation', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    const cancelBtn = page.locator('.actions .btn-cancel')
    await expect(cancelBtn).toBeVisible()
    await cancelBtn.click()
    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('cooperatively')
  })

  test('shows dead-letter status and panel for a dead-lettered job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.DEADLETTERED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    // The header status badge reads "Dead-lettered" (driven by dead_lettered_at,
    // not the underlying 'failed' status).
    await expect(page.locator('.header .status')).toHaveText('Dead-lettered')

    // A dedicated dead-letter panel shows the reason.
    const dlBox = page.locator('.dead-letter-box')
    await expect(dlBox).toBeVisible()
    await expect(dlBox.locator('h4')).toHaveText('Dead-letter')
    await expect(dlBox.locator('pre')).toContainText('max retries exhausted')

    // Cancel is for running jobs only; a terminal dead-lettered job must not show it.
    await expect(page.locator('.actions .btn-cancel')).toHaveCount(0)
  })

  test('shows worker for a running job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    const workerRow = page.locator('.meta-item', { hasText: 'Worker' })
    await expect(workerRow.locator('.value')).toHaveText('worker-1')
    await expect(workerRow.getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('shows dash worker for an unheld completed job', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.meta-item', { hasText: 'Worker' }).locator('.value')).toHaveText('—')
  })

  test('metadata shows queue', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.meta', { timeout: 10000 })
    await expect(page.locator('.meta-item', { hasText: 'Queue' }).locator('.value')).toHaveText('emails')
  })

  test('metadata shows priority', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.meta', { timeout: 10000 })
    await expect(page.locator('.meta-item', { hasText: 'Priority' }).locator('.value')).toHaveText('5')
  })

  test('metadata shows attempts and max retries', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.meta', { timeout: 10000 })
    await expect(page.locator('.meta-item', { hasText: 'Attempts/Max' }).locator('.value')).toContainText('/')
  })

  test('header has copy button for job id', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.header-id').getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('metadata has copy button for job id', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.meta', { timeout: 10000 })
    await expect(page.locator('.meta-item', { hasText: 'ID' }).getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('running job shows running completion indicator', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.timestamps', { timeout: 10000 })
    await expect(page.locator('.running-indicator')).toHaveText('running')
  })

  test('running job shows cancel action', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.actions .btn-cancel')).toBeVisible()
  })

  test('running job shows pause action', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.actions .btn-pause')).toBeVisible()
  })

  test('completed job does not show cancel action', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.actions .btn-cancel')).toHaveCount(0)
  })

  test('completed job has delete action', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.actions .btn-delete')).toBeVisible()
  })

  test('arguments copy button is visible', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.PENDING_1}`)
    await page.waitForSelector('.args-box', { timeout: 10000 })
    await expect(page.locator('.args-box').getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('last error copy button is visible', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.FAILED_4}`)
    await page.waitForSelector('.error-box', { timeout: 10000 })
    await expect(page.locator('.error-box').getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('dead-letter copy button is visible', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.DEADLETTERED_1}`)
    await page.waitForSelector('.dead-letter-box', { timeout: 10000 })
    await expect(page.locator('.dead-letter-box').getByRole('button', { name: 'Copy' })).toBeVisible()
  })

  test('checkpoint rows include created timestamps', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.checkpoints', { timeout: 10000 })
    await expect(page.locator('.checkpoints tbody tr').first().locator('td').nth(3)).not.toHaveText('')
  })

  test('checkpoint toggle is collapsed initially', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.checkpoints', { timeout: 10000 })
    await expect(page.locator('.checkpoint-toggle').first()).toHaveAttribute('aria-expanded', 'false')
  })

  test('checkpoint toggle expands payload', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.checkpoints', { timeout: 10000 })
    await page.locator('.checkpoint-toggle').first().click()
    await expect(page.locator('.checkpoint-result pre').first()).toBeVisible()
  })

  test('not found job shows empty state', async ({ page }) => {
    await page.goto('/#/jobs/not-found')
    await expect(page.locator('.empty')).toContainText('Job unavailable')
  })

  test('workflow completed child shows workflow section', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.WORKFLOW_CHILD_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.workflow-section')).toBeVisible()
  })

  test('workflow running child shows workflow section', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.WORKFLOW_CHILD_2}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.workflow-section')).toBeVisible()
  })

  test('pending job shows wait duration', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.PENDING_1}`)
    await page.waitForSelector('.timestamps', { timeout: 10000 })
    await expect(page.locator('.duration-row').first()).not.toHaveText('—')
  })

  test('completed job shows run duration', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.timestamps', { timeout: 10000 })
    await expect(page.locator('.duration-row').nth(1)).not.toHaveText('—')
  })

  test('header status for running job is lowercase running', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.RUNNING_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })
    await expect(page.locator('.header .status')).toHaveText('running')
  })
})
