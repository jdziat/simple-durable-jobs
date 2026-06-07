export function isArmed(typed: string, confirmWord?: string): boolean {
  return !confirmWord || typed === confirmWord
}
