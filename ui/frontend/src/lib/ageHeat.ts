export function ageHeatLevel(ageSec: number, warnAfterSec = 120, dangerAfterSec = 600): 'neutral' | 'warn' | 'danger' {
  if (ageSec >= dangerAfterSec) return 'danger'
  if (ageSec >= warnAfterSec) return 'warn'
  return 'neutral'
}
