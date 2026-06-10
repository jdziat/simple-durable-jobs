export type Period = '1h' | '24h' | '7d' | '30d'
export type HistorySeries = { completed: number[]; failed: number[]; labels: string[] }
export type HistoryPoint = { timestamp?: { toDate(): Date }; value: bigint | number }

// The backend buckets completed and failed independently, so the series can
// differ in length and timestamp sets. Merge over the sorted union.
export function mergeHistory(completed: HistoryPoint[], failed: HistoryPoint[]): HistorySeries {
  const times = new Set<number>()
  for (const point of completed) if (point.timestamp) times.add(point.timestamp.toDate().getTime())
  for (const point of failed) if (point.timestamp) times.add(point.timestamp.toDate().getTime())
  const sorted = Array.from(times).sort((a, b) => a - b)
  const completedMap = new Map(completed.filter(p => p.timestamp).map(p => [p.timestamp!.toDate().getTime(), Number(p.value)]))
  const failedMap = new Map(failed.filter(p => p.timestamp).map(p => [p.timestamp!.toDate().getTime(), Number(p.value)]))
  const fmt = new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short',
  })
  return {
    completed: sorted.map(t => completedMap.get(t) ?? 0),
    failed: sorted.map(t => failedMap.get(t) ?? 0),
    labels: sorted.map(t => fmt.format(new Date(t))),
  }
}
