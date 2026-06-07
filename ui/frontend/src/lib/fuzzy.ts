export function fuzzyScore(query: string, candidate: string): number {
  const q = query.trim().toLowerCase()
  const c = candidate.trim().toLowerCase()
  if (!q) return 1
  if (!c) return 0
  if (q === c) return 1000 + q.length
  const direct = c.indexOf(q)
  if (direct >= 0) return 700 + q.length * 10 - direct

  let score = 0
  let last = -1
  for (const ch of q) {
    const next = c.indexOf(ch, last + 1)
    if (next < 0) return 0
    score += next === last + 1 ? 12 : 4
    if (next === 0 || c[next - 1] === '-' || c[next - 1] === '_' || c[next - 1] === '/') score += 8
    score -= Math.max(0, next - last - 1)
    last = next
  }
  return Math.max(1, score - c.length * 0.1)
}

export function fuzzyFilter<T>(query: string, items: T[], key: (item: T) => string): T[] {
  return items
    .map(item => ({ item, score: fuzzyScore(query, key(item)) }))
    .filter(entry => entry.score > 0)
    .sort((a, b) => b.score - a.score || key(a.item).localeCompare(key(b.item)))
    .map(entry => entry.item)
}
