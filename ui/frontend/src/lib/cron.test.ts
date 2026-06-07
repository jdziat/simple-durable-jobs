import { describe, expect, test } from 'vitest'
import { cronToProse } from './cron'

describe('cronToProse', () => {
  test('translates common interval patterns', () => {
    expect(cronToProse('*/5 * * * *')).toBe('every 5 minutes')
    expect(cronToProse('0 * * * *')).toBe('every hour')
    expect(cronToProse('15 */2 * * *')).toBe('every 2 hours at minute 15')
  })

  test('translates common daily, weekly, and monthly patterns', () => {
    expect(cronToProse('0 0 * * *')).toBe('every day at midnight')
    expect(cronToProse('30 14 * * 1')).toBe('every Monday at 2:30 PM')
    expect(cronToProse('0 9 15 * *')).toBe('every month on day 15 at 9 AM')
  })

  test('falls back to raw cron for exotic expressions', () => {
    expect(cronToProse('5 4 * */2 1-5')).toBe('5 4 * */2 1-5')
    expect(cronToProse('@daily')).toBe('@daily')
  })
})
