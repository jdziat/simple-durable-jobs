export type Density = 'dense' | 'comfortable'

function initialDensity(): Density {
  if (typeof document === 'undefined') return 'dense'
  try {
    const stored = localStorage.getItem('blackbox-density')
    if (stored === 'comfortable') return 'comfortable'
  } catch {
    // Density has no system signal; fall through to dense.
  }
  return 'dense'
}

export const density = $state<{ value: Density }>({ value: initialDensity() })

function applyDensity(next: Density) {
  density.value = next
  if (typeof document === 'undefined') return
  document.documentElement.setAttribute('data-density', next)
  try {
    localStorage.setItem('blackbox-density', next)
  } catch {
    // Persistence is best-effort.
  }
}

applyDensity(density.value)

export function toggleDensity() {
  applyDensity(density.value === 'dense' ? 'comfortable' : 'dense')
}
