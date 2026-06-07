export function deltaFlash(node: HTMLElement, value: unknown) {
  let previous = value
  let timeout: ReturnType<typeof setTimeout> | null = null
  const reduce = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches

  function clear() {
    if (timeout) clearTimeout(timeout)
    timeout = null
    node.classList.remove('flash')
  }

  return {
    // Svelte 5 actions only call update() on subsequent argument CHANGES —
    // never at mount — and `previous` is seeded in the action body, so the
    // very first update IS the first real delta and must flash (an extra
    // "initialized" guard here once swallowed the highest-signal change).
    update(next: unknown) {
      if (Object.is(previous, next)) return
      previous = next
      if (reduce) return
      clear()
      node.classList.add('flash')
      timeout = setTimeout(() => {
        node.classList.remove('flash')
        timeout = null
      }, 140)
    },
    destroy() {
      clear()
    },
  }
}
