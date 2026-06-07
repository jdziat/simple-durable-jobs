import { describe, expect, test } from 'vitest'
import { ageHeatLevel } from './ageHeat'

describe('ageHeatLevel', () => {
  test('maps below warn to neutral', () => {
    expect(ageHeatLevel(119, 120, 600)).toBe('neutral')
  })

  test('maps warn boundary and middle range to warn', () => {
    expect(ageHeatLevel(120, 120, 600)).toBe('warn')
    expect(ageHeatLevel(300, 120, 600)).toBe('warn')
  })

  test('maps danger boundary and above to danger', () => {
    expect(ageHeatLevel(600, 120, 600)).toBe('danger')
    expect(ageHeatLevel(601, 120, 600)).toBe('danger')
  })
})
