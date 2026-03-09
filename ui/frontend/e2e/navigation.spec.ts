import { test, expect } from '@playwright/test'

test.describe('Navigation', () => {
  test('sidebar shows app title', async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('.logo h1')).toHaveText('Jobs UI')
  })

  test('sidebar has all navigation links', async ({ page }) => {
    await page.goto('/')
    const nav = page.locator('.nav-links')
    await expect(nav.getByRole('link', { name: 'Dashboard' })).toBeVisible()
    await expect(nav.getByRole('link', { name: 'Jobs' })).toBeVisible()
    await expect(nav.getByRole('link', { name: 'Queues' })).toBeVisible()
    await expect(nav.getByRole('link', { name: 'Scheduled' })).toBeVisible()
    await expect(nav.getByRole('link', { name: 'Workflows' })).toBeVisible()
  })

  test('Dashboard link is active by default', async ({ page }) => {
    await page.goto('/')
    const dashLink = page.locator('.nav-links a[href="#/"]')
    await expect(dashLink).toHaveClass(/active/)
  })

  test('clicking Jobs navigates to jobs page', async ({ page }) => {
    await page.goto('/')
    await page.locator('.nav-links').getByRole('link', { name: 'Jobs' }).click()
    await expect(page).toHaveURL(/#\/jobs/)
    await expect(page.locator('.nav-links a[href="#/jobs"]')).toHaveClass(/active/)
    await expect(page.locator('h2')).toHaveText('Jobs')
  })

  test('clicking Queues navigates to queues page', async ({ page }) => {
    await page.goto('/')
    await page.locator('.nav-links').getByRole('link', { name: 'Queues' }).click()
    await expect(page).toHaveURL(/#\/queues/)
    await expect(page.locator('h2')).toHaveText('Queues')
  })

  test('clicking Scheduled navigates to scheduled page', async ({ page }) => {
    await page.goto('/')
    await page.locator('.nav-links').getByRole('link', { name: 'Scheduled' }).click()
    await expect(page).toHaveURL(/#\/scheduled/)
  })

  test('clicking Workflows navigates to workflows page', async ({ page }) => {
    await page.goto('/')
    await page.locator('.nav-links').getByRole('link', { name: 'Workflows' }).click()
    await expect(page).toHaveURL(/#\/workflows/)
    await expect(page.locator('h2')).toHaveText('Workflows')
  })
})
