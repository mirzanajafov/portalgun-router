import * as z from 'zod/mini'

const Stat = z.object({ median: z.number(), min: z.number(), max: z.number() })
export type Stat = z.infer<typeof Stat>

const ProxyCell = z.object({
  system: z.string(),
  rate: z.number(),
  runs: z.number(),
  saturated: z.number(),
  died: z.optional(z.number()),
  generatorIssues: z.number(),
  p50Us: Stat,
  p99Us: Stat,
  p999Us: Stat,
  throughputRps: Stat,
  cpuPerRequestUs: Stat,
  userPerRequestUs: Stat,
  memoryPeakMb: z.optional(Stat),
  swapPeakMb: z.optional(Stat),
  overheadP50Us: z.number(),
  overheadP99Us: z.number(),
  collapsed: z.optional(z.number()),
  goodputRps: z.optional(Stat),
  okP99Us: z.optional(Stat),
  shedShare: z.optional(Stat),
})
export type ProxyCell = z.infer<typeof ProxyCell>

export const ProxyReport = z.object({ workload: z.string(), cells: z.array(ProxyCell), maxRate: z.record(z.string(), z.number()) })
export type ProxyReport = z.infer<typeof ProxyReport>

const LimiterSummary = z.object({
  cell: z.string(),
  baseline: z.optional(z.string()),
  runs: z.number(),
  invalidRuns: z.number(),
  p50Us: Stat,
  p99Us: Stat,
  addedP50Us: z.optional(Stat),
  addedP99Us: z.optional(Stat),
  overAdmission: Stat,
  maxAhead: Stat,
  maxBehind: Stat,
  worstRatio: Stat,
  falseAdmits: Stat,
  falseRejects: Stat,
  redisRoundTripsPerRequest: Stat,
  redisCommandsPerRequest: Stat,
  redisCpuPerRequestUs: Stat,
  degraded: Stat,
  droppedDecisions: z.number(),
})
export type LimiterSummary = z.infer<typeof LimiterSummary>
export const LimiterSummaries = z.array(LimiterSummary)

const FailoverSummary = z.object({
  cell: z.string(),
  runs: z.number(),
  invalidRuns: z.number(),
  detected: z.number(),
  moved: z.number(),
  recovered: z.number(),
  detectMs: Stat,
  moveMs: Stat,
  lost: Stat,
  lostAfter: Stat,
  p99BeforeMs: Stat,
  p99FaultMs: Stat,
  attemptsFault: Stat,
  recoverMs: Stat,
  falseEjections: Stat,
})
export type FailoverSummary = z.infer<typeof FailoverSummary>
export const FailoverSummaries = z.array(FailoverSummary)

export const PROXIES: { key: string; label: string }[] = [
  { key: 'stdlib', label: 'Go httputil.ReverseProxy' },
  { key: 'portalgun', label: 'PortalGun' },
  { key: 'nginx', label: 'nginx' },
  { key: 'haproxy', label: 'HAProxy' },
  { key: 'caddy', label: 'Caddy' },
  { key: 'envoy', label: 'Envoy' },
]

export function overloadLabel(system: string): string {
  if (system === 'portalgun') return 'PortalGun, no cap'
  const cap = /^portalgun-cap(\d+)$/.exec(system)
  if (cap) return `PortalGun, cap ${cap[1]}`
  return PROXIES.find((p) => p.key === system)?.label ?? system
}

export function overloadSystems(r: ProxyReport): string[] {
  const rank = (s: string) => {
    if (s === 'stdlib') return -2
    if (s === 'portalgun') return -1
    const cap = /^portalgun-cap(\d+)$/.exec(s)
    return cap ? 1e6 - Number(cap[1]) : 2e6
  }
  return [...new Set(r.cells.map((c) => c.system))].filter((s) => s !== 'direct').sort((a, b) => rank(a) - rank(b) || a.localeCompare(b))
}

export function sustained(r: ProxyReport, system: string, rate: number): boolean {
  const c = r.cells.find((x) => x.system === system && x.rate === rate)
  return c !== undefined && c.saturated * 2 < c.runs
}

export function commonRate(r: ProxyReport, systems: string[]): number | null {
  const rates = [...new Set(r.cells.map((c) => c.rate))].sort((a, b) => a - b)
  let best: number | null = null
  for (const rate of rates) {
    if (!systems.every((s) => sustained(r, s, rate))) break
    best = rate
  }
  return best
}

export type ProxyRow = { key: string; label: string; low?: ProxyCell; common?: ProxyCell; top?: ProxyCell; maxRate: number }

export function proxyRows(r: ProxyReport, low: number, common: number): ProxyRow[] {
  return PROXIES.filter((p) => r.cells.some((c) => c.system === p.key)).map((p) => {
    const at = (rate: number) => r.cells.find((c) => c.system === p.key && c.rate === rate)
    const maxRate = r.maxRate[p.key] ?? 0
    return { ...p, low: at(low), common: at(common), top: at(maxRate), maxRate }
  })
}

export const DESIGNS: { key: string; label: string }[] = [
  { key: 'static-split', label: 'Static split, no coordination' },
  { key: 'redis-exact', label: 'Redis on every request' },
  { key: 'local-sync-10ms', label: 'Local bucket, sync every 10 ms' },
  { key: 'local-sync-50ms', label: 'Local bucket, sync every 50 ms' },
  { key: 'local-sync-100ms', label: 'Local bucket, sync every 100 ms' },
  { key: 'local-sync-500ms', label: 'Local bucket, sync every 500 ms' },
  { key: 'lease-5ms', label: 'Token lease, 5 ms' },
  { key: 'lease-20ms', label: 'Token lease, 20 ms' },
  { key: 'lease-100ms', label: 'Token lease, 100 ms' },
]

export type LimiterRow = { key: string; label: string; cells: (LimiterSummary | undefined)[] }

export function limiterRows(sums: LimiterSummary[], loads: string[]): LimiterRow[] {
  const by = new Map(sums.map((s) => [s.cell, s]))
  return DESIGNS.filter((d) => loads.some((l) => by.has(`${d.key}/${l}`))).map((d) => ({
    ...d,
    cells: loads.map((l) => by.get(`${d.key}/${l}`)),
  }))
}

export type BeforeAfterRow = { key: string; label: string; before: (LimiterSummary | undefined)[]; after: (LimiterSummary | undefined)[] }

export function beforeAfter(before: LimiterSummary[], after: LimiterSummary[], loads: string[]): BeforeAfterRow[] {
  const old = new Map(limiterRows(before, loads).map((r) => [r.key, r.cells]))
  return limiterRows(after, loads).flatMap((r) => {
    const cells = old.get(r.key)
    return cells ? [{ key: r.key, label: r.label, before: cells, after: r.cells }] : []
  })
}

export function valid(s: { runs: number; invalidRuns: number } | undefined): boolean {
  return s !== undefined && s.runs > s.invalidRuns
}

export const FAULTS: { key: string; label: string }[] = [
  { key: 'crash', label: 'Crash' },
  { key: 'reset', label: 'Connection reset' },
  { key: 'blackhole', label: 'Blackhole' },
  { key: 'slow', label: '500 ms slower' },
  { key: 'partial', label: '30% errors' },
  { key: 'flapping', label: 'Flapping every 2 s' },
]

export const DETECTION: { key: string; label: string }[] = [
  { key: 'probes', label: 'Probes' },
  { key: 'passive', label: 'Passive' },
  { key: 'both', label: 'Both' },
  { key: 'both-retries', label: 'Both + retries' },
]

export type FailoverRow = { key: string; label: string; cells: (FailoverSummary | undefined)[] }

export function failoverMatrix(sums: FailoverSummary[]): FailoverRow[] {
  const by = new Map(sums.map((s) => [s.cell, s]))
  return FAULTS.filter((f) => DETECTION.some((d) => by.has(`${f.key}/${d.key}`))).map((f) => ({
    ...f,
    cells: DETECTION.map((d) => by.get(`${f.key}/${d.key}`)),
  }))
}

const RegistryTimes = z.object({
  registerMs: z.optional(Stat),
  joinMs: z.optional(Stat),
  gatewaysMs: z.optional(Stat),
  leaveGatewaysMs: z.optional(Stat),
  killToFirstMs: z.optional(Stat),
})
type RegistryTimes = z.infer<typeof RegistryTimes>

const RegistrySummary = z.extend(RegistryTimes, {
  cell: z.string(),
  runs: z.number(),
  failed: z.number(),
  invalid: z.number(),
  requests: z.number(),
  dropped: z.number(),
  lost: z.number(),
  registryVersions: z.number(),
  lostRegistrations: z.number(),
})
export type RegistrySummary = z.infer<typeof RegistrySummary>
export const RegistrySummaries = z.array(RegistrySummary)

const REGISTRY_CASES: { key: string; label: string }[] = [
  { key: 'join', label: 'A fifth dimension joins, then leaves' },
  { key: 'leader', label: "The registry's leader killed, back after 10 s" },
  { key: 'failover', label: 'A dimension joins while the leader is dead' },
  { key: 'down', label: 'Every registry node down for 30 s' },
]

const REGISTRY_TIMES: { cell: string; key: keyof RegistryTimes; label: string }[] = [
  { cell: 'join', key: 'registerMs', label: 'Registering' },
  { cell: 'join', key: 'joinMs', label: 'Registered to its first request' },
  { cell: 'join', key: 'gatewaysMs', label: 'Registered to both gateways on the new config' },
  { cell: 'join', key: 'leaveGatewaysMs', label: 'Deregistering to both gateways without it' },
  { cell: 'failover', key: 'registerMs', label: 'Registering while the leader is dead' },
  { cell: 'failover', key: 'killToFirstMs', label: 'Leader killed to the new dimension serving' },
]

export type RegistryCase = { key: string; label: string; sum: RegistrySummary }

export function registryCases(sums: RegistrySummary[]): RegistryCase[] {
  const by = new Map(sums.map((s) => [s.cell, s]))
  return REGISTRY_CASES.flatMap((c) => {
    const sum = by.get(c.key)
    return sum ? [{ ...c, sum }] : []
  })
}

export function registryTimes(sums: RegistrySummary[]): { label: string; time: Stat }[] {
  const by = new Map(sums.map((s) => [s.cell, s]))
  return REGISTRY_TIMES.flatMap(({ cell, key, label }) => {
    const time = by.get(cell)?.[key]
    return time ? [{ label, time }] : []
  })
}

export function registryTotals(cases: RegistryCase[]): { runs: number; failed: number; sent: number; lost: number } {
  return cases.reduce(
    (t, { sum: s }) => ({ runs: t.runs + s.runs, failed: t.failed + s.failed + s.invalid, sent: t.sent + s.requests - s.dropped, lost: t.lost + s.lost }),
    { runs: 0, failed: 0, sent: 0, lost: 0 },
  )
}

export function us(v: number): string {
  if (!Number.isFinite(v)) return '-'
  const a = Math.abs(v)
  if (a >= 1e6) return `${(v / 1e6).toFixed(1)} s`
  return a >= 1000 ? `${(v / 1000).toFixed(a >= 10000 ? 0 : 1)} ms` : `${Math.round(v)} µs`
}

export function signed(text: string, v: number): string {
  return v > 0 ? `+${text}` : text
}

export function rps(v: number): string {
  return v >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : `${Math.round(v)}`
}
