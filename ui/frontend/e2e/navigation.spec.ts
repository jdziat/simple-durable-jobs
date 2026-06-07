import { test, expect } from '@playwright/test'

test.describe('Navigation', () => {
  test('ticker shows app identity', async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('.app-identity')).toHaveText('Durable Jobs')
    await expect(page).toHaveTitle('Durable Jobs')
  })

  test('dark theme actually paints dark', async ({ page }) => {
    // Regression guard: a CSS cascade tie once made the light palette win
    // under data-theme="dark", rendering the hero theme as a white page.
    // Assert the COMPUTED background, not just the attribute. A dark system
    // preference must boot dark (first visit honors prefers-color-scheme).
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.goto('/')
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark')
    const bg = await page.evaluate(
      () => getComputedStyle(document.body).backgroundColor
    )
    expect(bg).toBe('rgb(11, 14, 20)') // --bg-base #0B0E14 (dark)
  })

  test('nav rail has all navigation links', async ({ page }) => {
    await page.goto('/')
    await expect(page.getByRole('navigation', { name: 'Primary' })).toBeVisible()
    await expect(page.locator('.nav-rail')).toBeVisible()
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
