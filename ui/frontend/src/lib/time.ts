type ProtoTimestamp = { toDate?: () => Date; seconds?: bigint | number | string; nanos?: number }
type TimeInput = number | Date | ProtoTimestamp | null | undefined

function toDate(value: TimeInput): Date | null {
  if (value == null) return null
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value
  if (typeof value === 'number') return Number.isFinite(value) ? new Date(value) : null
  if (typeof value.toDate === 'function') return value.toDate()
  if ('seconds' in value) {
    const seconds = Number(value.seconds ?? 0)
    const nanos = Number(value.nanos ?? 0)
    return new Date(seconds * 1000 + Math.floor(nanos / 1_000_000))
  }
  return null
}

function parts(ms: number): string {
  const abs = Math.max(0, Math.floor(Math.abs(ms) / 1000))
  const days = Math.floor(abs / 86400)
  if (days > 0) return `${days}d`
  const hours = Math.floor(abs / 3600)
  const minutes = Math.floor((abs % 3600) / 60)
  const seconds = abs % 60
  if (hours > 0) return `${hours}h${minutes}m`
  if (minutes > 0) return `${minutes}m${seconds}s`
  return `${seconds}s`
}

export function relative(ts: TimeInput): string {
  const date = toDate(ts)
  if (!date) return '—'
  return `${parts(Date.now() - date.getTime())} ago`
}

export function absolute(ts: TimeInput): string {
  const date = toDate(ts)
  if (!date) return '—'
  const time = new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZoneName: 'short',
  }).formatToParts(date)
  const get = (type: string) => time.find(part => part.type === type)?.value ?? ''
  return `${get('hour')}:${get('minute')}:${get('second')} ${get('timeZoneName')}`.trim()
}

export function duration(a: TimeInput, b: TimeInput): string {
  const start = toDate(a)
  const end = toDate(b)
  if (!start || !end) return '—'
  return parts(end.getTime() - start.getTime())
}

export function countdown(ts: TimeInput): string {
  const date = toDate(ts)
  if (!date) return '—'
  return `in ${parts(date.getTime() - Date.now())}`
}
