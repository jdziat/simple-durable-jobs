export type ToastKind = 'ok' | 'err' | 'info'

export interface ToastInput {
  kind: ToastKind
  msg: string
  undo?: () => void
}

export interface ToastItem extends ToastInput {
  id: number
}

export const toasts = $state<{ list: ToastItem[] }>({ list: [] })

let nextId = 1
const timers = new Map<number, ReturnType<typeof setTimeout>>()

export function dismiss(id: number) {
  const timer = timers.get(id)
  if (timer) clearTimeout(timer)
  timers.delete(id)
  toasts.list = toasts.list.filter(item => item.id !== id)
}

export function push(input: ToastInput): number {
  const id = nextId++
  const item: ToastItem = { ...input, id }
  toasts.list = [...toasts.list, item]
  const ttl = input.kind === 'err' ? 6000 : 4000
  timers.set(id, setTimeout(() => dismiss(id), ttl))
  return id
}

export const toast = { push, dismiss, toasts }
