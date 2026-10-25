import 'server-only'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import path from 'node:path'
import { z, type ZodType } from 'zod'

const PolicyResult = z.object({
  policy: z.string(),
  trials: z.number(),
  rollbacks: z.number(),
  rollbackRate: z.number(),
  ciLow: z.number(),
  ciHigh: z.number(),
  medianRollbackSec: z.number(),
  medianPromoteSec: z.number(),
  meanCanaryErrors: z.number(),
})

const CellResult = z.object({
  cell: z.object({
    group: z.string(),
    baseError: z.number(),
    share: z.number(),
    rho: z.number(),
    totalFailure: z.boolean().optional(),
    canarySlow: z.number(),
    blip: z.boolean().optional(),
    rps: z.number(),
    trials: z.number(),
  }),
  policies: z.array(PolicyResult),
})
export type CellResult = z.infer<typeof CellResult>

const SimFile = z.object({ cells: z.array(CellResult) })

export type SimResults = z.infer<typeof SimFile> & { name: string }

function resultsDir(): string {
  return process.env.RESULTS_DIR ?? path.join(process.cwd(), '..', 'bench', 'results')
}

function read<T>(file: string, schema: ZodType<T>): T {
  const parsed = schema.safeParse(JSON.parse(readFileSync(file, 'utf8')))
  if (!parsed.success) throw new Error(`${file} is not what the Results page reads:\n${z.prettifyError(parsed.error)}`)
  return parsed.data
}

export type Run<T> = { name: string; summary: T }

export function latestRun<T>(experiment: string, schema: ZodType<T>): Run<T> | null {
  const dir = path.join(resultsDir(), experiment)
  let names: string[]
  try {
    names = readdirSync(dir).sort().reverse()
  } catch {
    return null
  }
  const name = names.find((n) => existsSync(path.join(dir, n, 'summary.json')))
  if (!name) return null
  return { name, summary: read(path.join(dir, name, 'summary.json'), schema) }
}

export function latestSim(): SimResults | null {
  const dir = path.join(resultsDir(), 'e4')
  let runs: string[]
  try {
    runs = readdirSync(dir)
      .filter((d) => d.startsWith('sim-'))
      .sort()
  } catch {
    return null
  }
  const name = runs.at(-1)
  if (!name) return null
  return { name, ...read(path.join(dir, name, 'results.json'), SimFile) }
}

export function aaRange(cells: CellResult[], policy: string): [number, number] {
  const rates = cells.filter((c) => c.cell.group === 'a/a').map((c) => c.policies.find((p) => p.policy === policy)?.rollbackRate ?? 0)
  return [Math.min(...rates), Math.max(...rates)]
}
