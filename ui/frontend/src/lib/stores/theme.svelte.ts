export type Theme = 'dark' | 'light'

function initialTheme(): Theme {
  if (typeof document === 'undefined') return 'dark'
  const value = document.documentElement.getAttribute('data-theme')
  return value === 'light' ? 'light' : 'dark'
}

export const theme = $state<{ value: Theme }>({ value: initialTheme() })

function applyTheme(next: Theme) {
  theme.value = next
  if (typeof document === 'undefined') return
  document.documentElement.setAttribute('data-theme', next)
  try {
    localStorage.setItem('blackbox-theme', next)
  } catch {
    // Persistence is best-effort; the attribute is the source for this paint.
  }
}

export function toggleTheme() {
  applyTheme(theme.value === 'dark' ? 'light' : 'dark')
}
