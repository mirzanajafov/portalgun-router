import 'server-only'
import { readdirSync, readFileSync } from 'node:fs'
import path from 'node:path'

export type PolicyResult = {
  policy: string
  trials: number
  rollbacks: number
  rollbackRate: number
  ciLow: number
  ciHigh: number
  medianRollbackSec: number
  medianPromoteSec: number
  meanCanaryErrors: number
}

export type Cell = {
  group: string
  baseError: number
  share: number
  rho: number
  totalFailure?: boolean
  canarySlow: number
  blip?: boolean
  rps: number
  trials: number
}

export type CellResult = { cell: Cell; policies: PolicyResult[] }

export type SimResults = { name: string; config: Record<string, unknown>; cells: CellResult[] }

export function resultsDir(): string {
  return process.env.RESULTS_DIR ?? path.join(process.cwd(), '..', 'bench', 'results')
}

export function latestSim(): SimResults | null {
  const dir = path.join(resultsDir(), 'e4')
  let runs: string[]
  try {
    runs = readdirSync(dir).filter((d) => d.startsWith('sim-')).sort()
  } catch {
    return null
  }
  const name = runs.at(-1)
  if (!name) return null
  const raw = JSON.parse(readFileSync(path.join(dir, name, 'results.json'), 'utf8'))
  return { name, config: raw.config, cells: raw.cells }
}

export function aaRange(cells: CellResult[], policy: string): [number, number] {
  const rates = cells.filter((c) => c.cell.group === 'a/a').map((c) => c.policies.find((p) => p.policy === policy)?.rollbackRate ?? 0)
  return [Math.min(...rates), Math.max(...rates)]
}
