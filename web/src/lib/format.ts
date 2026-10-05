import type { DimensionSnapshot, Merged } from './types'

export const LORE: Record<string, string> = {
  'c-137': 'C-137',
  'j19-zeta-7': 'J19-Zeta-7',
  'd-99': 'D-99',
  cronenberg: 'Cronenberg',
}

export function dimensionName(id: string): string {
  return LORE[id] ?? id
}

export type Health = 'healthy' | 'ejected' | 'half-open' | 'down'

export function health(d: DimensionSnapshot): Health {
  if (!d.healthy) return 'down'
  if (d.outlier === 'ejected') return 'ejected'
  if (d.outlier === 'half-open') return 'half-open'
  return 'healthy'
}

export function share(m: Merged | null, id: string): number {
  if (!m) return 0
  const total = m.dimensions.reduce((n, d) => n + d.attempts, 0)
  const d = m.dimensions.find((x) => x.id === id)
  return total > 0 && d ? d.attempts / total : 0
}

export function pct(v: number, digits = 1): string {
  if (!Number.isFinite(v)) return '-'
  return `${(v * 100).toFixed(digits).replace(/\.0+$/, '')}%`
}

export function ms(v: number): string {
  if (!v) return '-'
  return v < 10 ? `${v.toFixed(1)} ms` : `${Math.round(v)} ms`
}

export function errorRate(d: DimensionSnapshot): number {
  const total = d.attempts
  return total > 0 ? d.errors / total : 0
}

export function seconds(v: number | undefined): string {
  if (v === undefined) return '-'
  return v < 10 ? `${v.toFixed(1)} s` : `${Math.round(v)} s`
}

export function remaining(until: string, now: number): number {
  return Math.max(0, Math.ceil((Date.parse(until) - now) / 1000))
}

export function totalRps(m: Merged | null): number {
  if (!m) return 0
  return Object.values(m.routes).reduce((n, r) => n + r.requests, 0)
}
