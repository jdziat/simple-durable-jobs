import { test, expect, type Page } from '@playwright/test'
import { COUNTS, JOBS, METADATA, TENANTS } from './helpers/seed-data'

async function expectJobRow(page: Page, jobId: string, visible: boolean) {
  const row = page.locator('.jobs-table tbody tr').filter({ has: page.locator(`.id[title="${jobId}"]`) })
  if (visible) {
    await expect(row).toHaveCount(1)
  } else {
    await expect(row).toHaveCount(0)
  }
}

test.describe('Operator Surface', () => {
  test('tenant filter round-trips through the Jobs list URL and rows', async ({ page }) => {
    await page.goto('/#/jobs')
    await page.waitForSelector('.jobs-table', { timeout: 10000 })

    const rows = page.locator('.jobs-table tbody tr')
    await page.getByPlaceholder('Tenant...').fill(TENANTS.GLOBEX)

    await expect(page).toHaveURL(new RegExp(`tenant=${TENANTS.GLOBEX}`))
    await expect(rows).toHaveCount(COUNTS.GLOBEX)
    await expectJobRow(page, JOBS.PENDING_2, true)
    await expectJobRow(page, JOBS.COMPLETED_2, true)
    await expectJobRow(page, JOBS.DEADLETTERED_1, false)

    await page.locator('.btn-clear').click()
    await expect(page).toHaveURL(/#\/jobs$/)
    await expect(rows).toHaveCount(20)
  })

  test('dead-lettered detail requeue preserves checkpoint semantics', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.DEADLETTERED_REQUEUE}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.header .status')).toHaveText('Dead-lettered')
    await page.locator('.dead-letter-box .btn-requeue').click()

    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()
    await expect(dialog).toContainText('preserving existing checkpoints')
    await dialog.getByRole('textbox').fill('REQUEUE')
    await dialog.getByRole('button', { name: 'Requeue' }).click()

    await expect(page.locator('.toast-ok').filter({ hasText: 'job requeued' }).first()).toBeVisible()
    await expect(page.locator('.header .status')).toHaveText('pending')
    await expect(page.locator('.dead-letter-box')).toHaveCount(0)
  })

  test('dead-lettered tenant filter reports the post-filter total', async ({ page }) => {
    await page.goto('/#/jobs')
    await page.waitForSelector('.jobs-table', { timeout: 10000 })

    await page.locator('.filters select').selectOption('dead-lettered')
    await page.getByPlaceholder('Tenant...').fill(TENANTS.ACME)

    await expect(page).toHaveURL(new RegExp(`status=dead-lettered.*tenant=${TENANTS.ACME}`))
    const rows = page.locator('.jobs-table tbody tr')
    await expect(rows).toHaveCount(COUNTS.DEADLETTERED_ACME)
    await expectJobRow(page, JOBS.DEADLETTERED_1, true)
    await expectJobRow(page, JOBS.DEADLETTERED_ACME_2, true)
    await expectJobRow(page, JOBS.DEADLETTERED_REQUEUE, false)
    await expect(page.locator('.pagination')).toContainText(`of ${COUNTS.DEADLETTERED_ACME}`)
  })

  test('job detail renders tenant and metadata table rows', async ({ page }) => {
    await page.goto(`/#/jobs/${JOBS.COMPLETED_1}`)
    await page.waitForSelector('.job-detail .header', { timeout: 10000 })

    await expect(page.locator('.meta-item', { hasText: 'Tenant' }).locator('.value')).toHaveText(TENANTS.ACME)
    const metadata = page.locator('section.job-metadata')
    await expect(metadata).toBeVisible()

    for (const [key, value] of Object.entries(METADATA.ACME_COMPLETED)) {
      const row = metadata.getByRole('row', { name: `${key} ${value}` })
      await expect(row.getByRole('rowheader')).toHaveText(key)
      await expect(row.getByRole('cell')).toHaveText(value)
    }
  })
})
