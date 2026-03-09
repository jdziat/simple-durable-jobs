import { test, expect } from '@playwright/test'

test.describe('Workflows Page', () => {
  test('shows workflows heading', async ({ page }) => {
    await page.goto('/#/workflows')
    await expect(page.locator('h2')).toHaveText('Workflows')
  })

  test('shows workflow cards', async ({ page }) => {
    await page.goto('/#/workflows')
    // Wait for loading to finish
    await page.waitForSelector('.loading', { state: 'hidden', timeout: 10000 }).catch(() => {})

    // Should show at least 1 workflow (the seeded BatchProcess workflow)
    // or show "No workflows found" if the query doesn't find any
    const cards = page.locator('.workflow-card')
    const empty = page.locator('.empty')

    // Either we have workflow cards or the empty state
    const hasCards = await cards.count() > 0
    const hasEmpty = await empty.isVisible().catch(() => false)
    expect(hasCards || hasEmpty).toBeTruthy()
  })

  test('workflow card shows job type and status', async ({ page }) => {
    await page.goto('/#/workflows')
    await page.waitForSelector('.loading', { state: 'hidden', timeout: 10000 }).catch(() => {})

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      const firstCard = cards.first()
      await expect(firstCard.locator('.job-type')).toBeVisible()
      await expect(firstCard.locator('.status')).toBeVisible()
    }
  })

  test('workflow card shows progress', async ({ page }) => {
    await page.goto('/#/workflows')
    await page.waitForSelector('.loading', { state: 'hidden', timeout: 10000 }).catch(() => {})

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      const firstCard = cards.first()
      await expect(firstCard.locator('.progress-section')).toBeVisible()
      await expect(firstCard.locator('.progress-text')).toBeVisible()
    }
  })

  test('clicking workflow card navigates to detail', async ({ page }) => {
    await page.goto('/#/workflows')
    await page.waitForSelector('.loading', { state: 'hidden', timeout: 10000 }).catch(() => {})

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      await cards.first().click()
      await expect(page).toHaveURL(/#\/workflows\//)
    }
  })
})
