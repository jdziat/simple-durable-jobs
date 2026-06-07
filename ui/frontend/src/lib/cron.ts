function plural(value: number, unit: string): string {
  return `${value} ${unit}${value === 1 ? '' : 's'}`
}

function hourLabel(hour: number, minute: number): string {
  if (hour === 0 && minute === 0) return 'midnight'
  if (hour === 12 && minute === 0) return 'noon'
  const suffix = hour >= 12 ? 'PM' : 'AM'
  const h = hour % 12 || 12
  return minute === 0 ? `${h} ${suffix}` : `${h}:${String(minute).padStart(2, '0')} ${suffix}`
}

export function cronToProse(expr: string): string {
  const raw = expr.trim()
  const parts = raw.split(/\s+/)
  if (parts.length !== 5) return raw

  const [minute, hour, dom, month, dow] = parts
  if (month !== '*') return raw

  const everyMinute = minute.match(/^\*\/(\d+)$/)
  if (everyMinute && hour === '*' && dom === '*' && dow === '*') {
    return `every ${plural(Number(everyMinute[1]), 'minute')}`
  }

  const everyHour = hour.match(/^\*\/(\d+)$/)
  if (/^\d+$/.test(minute) && everyHour && dom === '*' && dow === '*') {
    const n = Number(everyHour[1])
    return Number(minute) === 0 ? `every ${plural(n, 'hour')}` : `every ${plural(n, 'hour')} at minute ${Number(minute)}`
  }

  if (minute === '0' && hour === '*' && dom === '*' && dow === '*') return 'every hour'

  if (/^\d+$/.test(minute) && /^\d+$/.test(hour)) {
    const m = Number(minute)
    const h = Number(hour)
    if (m > 59 || h > 23) return raw
    if (dom === '*' && dow === '*') return `every day at ${hourLabel(h, m)}`
    if (dom === '*' && /^[0-7]$/.test(dow)) {
      const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
      return `every ${days[Number(dow)]} at ${hourLabel(h, m)}`
    }
    if (/^\d+$/.test(dom) && dow === '*') return `every month on day ${Number(dom)} at ${hourLabel(h, m)}`
  }

  return raw
}
