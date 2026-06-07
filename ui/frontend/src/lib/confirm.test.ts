import { describe, expect, test } from 'vitest'
import { isArmed } from './confirm'

describe('isArmed', () => {
  test('requires exact confirmation word', () => {
    expect(isArmed('DELETE', 'DELETE')).toBe(true)
    expect(isArmed('delete', 'DELETE')).toBe(false)
  })

  test('is armed when no confirmation word is required', () => {
    expect(isArmed('', undefined)).toBe(true)
  })
})
