export type Stat = { median: number; min: number; max: number }

export type ProxyCell = {
  system: string
  rate: number
  runs: number
  saturated: number
  died?: number
  generatorIssues: number
  p50Us: Stat
  p99Us: Stat
  p999Us: Stat
  throughputRps: Stat
  cpuPerRequestUs: Stat
  userPerRequestUs: Stat
  memoryPeakMb?: Stat
  swapPeakMb?: Stat
  overheadP50Us: number
  overheadP99Us: number
  collapsed?: number
  goodputRps?: Stat
  okP99Us?: Stat
  shedShare?: Stat
}

export type ProxyReport = { workload: string; cells: ProxyCell[]; maxRate: Record<string, number> }

export type LimiterSummary = {
  cell: string
  baseline?: string
  runs: number
  invalidRuns: number
  p50Us: Stat
  p99Us: Stat
  addedP50Us?: Stat
  addedP99Us?: Stat
  overAdmission: Stat
  maxAhead: Stat
  maxBehind: Stat
  worstRatio: Stat
  falseAdmits: Stat
  falseRejects: Stat
  redisRoundTripsPerRequest: Stat
  redisCommandsPerRequest: Stat
  redisCpuPerRequestUs: Stat
  degraded: Stat
  droppedDecisions: number
}

export type FailoverSummary = {
  cell: string
  runs: number
  invalidRuns: number
  detected: number
  moved: number
  recovered: number
  detectMs: Stat
  moveMs: Stat
  lost: Stat
  lostAfter: Stat
  p99BeforeMs: Stat
  p99FaultMs: Stat
  attemptsFault: Stat
  recoverMs: Stat
  falseEjections: Stat
}

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

export type RegistrySummary = {
  cell: string
  runs: number
  failed: number
  invalid: number
  requests: number
  lost: number
  registryVersions: number
  lostRegistrations: number
  registerMs?: Stat
  joinMs?: Stat
  gatewaysMs?: Stat
  leaveGatewaysMs?: Stat
  killToFirstMs?: Stat
}

export const REGISTRY_CELLS: { cell: string; label: string }[] = [
  { cell: 'join', label: 'A fifth dimension joins, then leaves' },
  { cell: 'leader', label: "The registry's leader killed, back after 10 s" },
  { cell: 'failover', label: 'A dimension joins while the leader is dead' },
  { cell: 'down', label: 'Every registry node down for 30 s' },
]

const REGISTRY_TIMES: { cell: string; key: 'registerMs' | 'joinMs' | 'gatewaysMs' | 'leaveGatewaysMs' | 'killToFirstMs'; label: string }[] = [
  { cell: 'join', key: 'registerMs', label: 'Registering' },
  { cell: 'join', key: 'joinMs', label: 'Registered to its first request' },
  { cell: 'join', key: 'gatewaysMs', label: 'Registered to both gateways on the new config' },
  { cell: 'join', key: 'leaveGatewaysMs', label: 'Deregistering to both gateways without it' },
  { cell: 'failover', key: 'registerMs', label: 'Registering while the leader is dead' },
  { cell: 'failover', key: 'killToFirstMs', label: 'Leader killed to the new dimension serving' },
]

export type RegistryTimeRow = { label: string; runs: number; time: Stat }

export function registryTimes(sums: RegistrySummary[]): RegistryTimeRow[] {
  const by = new Map(sums.map((s) => [s.cell, s]))
  return REGISTRY_TIMES.flatMap(({ cell, key, label }) => {
    const s = by.get(cell)
    const time = s?.[key]
    return s && time ? [{ label, runs: s.runs - s.failed - s.invalid, time }] : []
  })
}

export function registryTotals(sums: RegistrySummary[]): { runs: number; requests: number; lost: number } {
  return sums.reduce((t, s) => ({ runs: t.runs + s.runs, requests: t.requests + s.requests, lost: t.lost + s.lost }), { runs: 0, requests: 0, lost: 0 })
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
