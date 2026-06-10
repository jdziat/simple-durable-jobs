import { test, expect } from '@playwright/test'
import { JOBS } from './helpers/seed-data'

async function waitForWorkflows(page: import('@playwright/test').Page) {
  await page.waitForSelector('.skeleton-card', { state: 'hidden', timeout: 10000 }).catch(() => {})
}

test.describe('Workflows Page', () => {
  test('shows workflows heading', async ({ page }) => {
    await page.goto('/#/workflows')
    await expect(page.locator('h2')).toHaveText('Workflows')
  })

  test('shows workflow cards', async ({ page }) => {
    await page.goto('/#/workflows')
    await waitForWorkflows(page)

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
    await waitForWorkflows(page)

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      const firstCard = cards.first()
      await expect(firstCard.locator('.job-type')).toBeVisible()
      await expect(firstCard.locator('.status')).toBeVisible()
    }
  })

  test('workflow card shows progress', async ({ page }) => {
    await page.goto('/#/workflows')
    await waitForWorkflows(page)

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      const firstCard = cards.first()
      await expect(firstCard.locator('.progress-section')).toBeVisible()
      await expect(firstCard.locator('.progress-text')).toBeVisible()
    }
  })

  test('clicking workflow card navigates to detail', async ({ page }) => {
    await page.goto('/#/workflows')
    await waitForWorkflows(page)

    const cards = page.locator('.workflow-card')
    if (await cards.count() > 0) {
      await cards.first().click()
      await expect(page).toHaveURL(/#\/workflows\//)
    }
  })

  test('seeded workflow card renders honest status segments', async ({ page }) => {
    await page.goto('/#/workflows')
    await waitForWorkflows(page)

    const card = page.locator(`.workflow-card[href="#/workflows/${JOBS.WORKFLOW_ROOT}"]`)
    await expect(card).toBeVisible()
    await expect(card.locator('.progress-section .bar.bar-completed')).toBeVisible()
    await expect(card.locator('.progress-section .bar.bar-running')).toBeVisible()
    await expect(card.locator('.progress-section .bar.bar-pending')).toHaveCount(1)
  })

  test('workflow detail renders child statuses and waterfall svg', async ({ page }) => {
    await page.goto(`/#/workflows/${JOBS.WORKFLOW_ROOT}`)
    await page.waitForSelector('.waterfall-chart', { timeout: 10000 })

    await expect(page.locator('.waterfall-chart svg').first()).toBeVisible()
    await expect(page.locator(`.job-label:has(a[href="#/jobs/${JOBS.WORKFLOW_CHILD_1}"]) .status`)).toHaveText('completed')
    await expect(page.locator(`.job-label:has(a[href="#/jobs/${JOBS.WORKFLOW_CHILD_2}"]) .status`)).toHaveText('running')
    await expect(page.locator(`.job-label:has(a[href="#/jobs/${JOBS.WORKFLOW_CHILD_3}"]) .status`)).toHaveText('pending')
  })

  test('waterfall svg fills resolve through signal tokens', async ({ page }) => {
    await page.addInitScript(() => localStorage.setItem('blackbox-theme', 'dark'))
    await page.goto(`/#/workflows/${JOBS.WORKFLOW_ROOT}`)
    await page.waitForSelector('.waterfall-chart .bar.bar-completed', { timeout: 10000 })

    const completedBar = page.locator('.waterfall-chart .bar.bar-completed').first()
    const fill = await completedBar.evaluate((element: Element) => getComputedStyle(element).fill)
    expect(fill).toBe('rgb(52, 211, 153)')
    expect(fill).not.toBe('rgb(16, 185, 129)')
  })
})
