import { jobsClient } from '../client'

export interface QueueStat {
  name: string
  pending: number
  running: number
  completed: number
  failed: number
  paused: number
  isPaused: boolean
  total: number
}

export interface StatsSnapshot {
  totalPending: number
  totalRunning: number
  totalCompleted: number
  totalFailed: number
  totalPaused: number
  activeWorkers: number
  queues: QueueStat[]
}

export const stats = $state<{ value: StatsSnapshot | null }>({ value: null })
export const lastUpdated = $state<{ value: number }>({ value: 0 })
export const polling = $state<{ value: boolean }>({ value: false })
export const error = $state<{ value: string | null }>({ value: null })

let interval: ReturnType<typeof setInterval> | null = null
let consumers = 0

function mapStats(response: Awaited<ReturnType<typeof jobsClient.getStats>>): StatsSnapshot {
  return {
    totalPending: Number(response.totalPending),
    totalRunning: Number(response.totalRunning),
    totalCompleted: Number(response.totalCompleted),
    totalFailed: Number(response.totalFailed),
    totalPaused: Number(response.totalPaused),
    activeWorkers: Number(response.activeWorkers),
    queues: response.queues.map(q => {
      const pending = Number(q.pending)
      const running = Number(q.running)
      const completed = Number(q.completed)
      const failed = Number(q.failed)
      const paused = Number(q.paused)
      return {
        name: q.name,
        pending,
        running,
        completed,
        failed,
        paused,
        isPaused: q.isPaused,
        total: pending + running + completed + failed,
      }
    }),
  }
}

async function load() {
  if (polling.value) return
  polling.value = true
  try {
    stats.value = mapStats(await jobsClient.getStats({}))
    lastUpdated.value = Date.now()
    error.value = null
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Failed to load stats'
  } finally {
    polling.value = false
  }
}

export function start() {
  consumers += 1
  if (interval) return
  void load()
  interval = setInterval(() => void load(), 5000)
}

export function stop() {
  consumers = Math.max(0, consumers - 1)
  if (consumers > 0 || !interval) return
  clearInterval(interval)
  interval = null
}
