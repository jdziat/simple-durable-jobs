import { test, expect } from '@playwright/test'

test.describe('Scheduled Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#/scheduled')
  })

  test('shows scheduled heading', async ({ page }) => {
    await expect(page.locator('h2')).toHaveText('Scheduled')
  })

  test('page loads without route error', async ({ page }) => {
    await expect(page.locator('.scheduled-page')).toBeVisible()
    await expect(page.locator('.filters input[name="name"]')).toBeVisible()
    await expect(page.locator('.filters input[name="queue"]')).toBeVisible()
  })

  test('shows empty state when no schedules are registered', async ({ page }) => {
    const empty = page.locator('.empty')
    await expect(empty).toBeVisible()
    await expect(empty).toContainText('No scheduled jobs')
    await expect(empty.getByText('queue.Schedule()')).toBeVisible()
  })
})
