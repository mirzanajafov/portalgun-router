import { describe, expect, it } from 'vitest'
import {
  beforeAfter,
  commonRate,
  failoverMatrix,
  limiterRows,
  overloadLabel,
  overloadSystems,
  proxyRows,
  registryCases,
  registryTimes,
  registryTotals,
  rps,
  signed,
  us,
  valid,
  type FailoverSummary,
  type LimiterSummary,
  type ProxyCell,
  type ProxyReport,
  type RegistrySummary,
} from './measured'

const stat = (median: number) => ({ median, min: median, max: median })

function cell(system: string, rate: number, saturated = 0, runs = 3): ProxyCell {
  return {
    system,
    rate,
    runs,
    saturated,
    generatorIssues: 0,
    p50Us: stat(300),
    p99Us: stat(900),
    p999Us: stat(2000),
    throughputRps: stat(rate),
    cpuPerRequestUs: stat(120),
    userPerRequestUs: stat(60),
    memoryPeakMb: stat(rate / 100),
    overheadP50Us: 50,
    overheadP99Us: 200,
  }
}

describe('proxy results', () => {
  const report: ProxyReport = {
    workload: 'w1',
    cells: [
      cell('direct', 1000),
      cell('stdlib', 1000),
      cell('envoy', 1000),
      cell('direct', 4000),
      cell('stdlib', 4000),
      cell('envoy', 4000, 2),
      cell('stdlib', 6000),
    ],
    maxRate: { direct: 4000, stdlib: 6000, envoy: 1000 },
  }

  it('finds the highest rate that every proxy held', () => {
    expect(commonRate(report, ['stdlib', 'envoy'])).toBe(1000)
    expect(commonRate(report, ['stdlib'])).toBe(6000)
    expect(commonRate(report, ['nginx'])).toBeNull()
  })

  it('leaves out the direct baseline and keeps the order of the proxies', () => {
    const rows = proxyRows(report, 1000, 1000)
    expect(rows.map((r) => r.key)).toEqual(['stdlib', 'envoy'])
    expect(rows[0].top?.rate).toBe(6000)
    expect(rows[1].low?.overheadP50Us).toBe(50)
  })
})

describe('overload results', () => {
  it('orders the bare proxy, PortalGun without a cap, then the caps from the largest down', () => {
    const report: ProxyReport = {
      workload: 'w1',
      cells: ['portalgun-cap128', 'portalgun', 'portalgun-cap1024', 'stdlib', 'portalgun-cap256', 'direct'].map((s) => cell(s, 8000)),
      maxRate: {},
    }
    expect(overloadSystems(report)).toEqual(['stdlib', 'portalgun', 'portalgun-cap1024', 'portalgun-cap256', 'portalgun-cap128'])
  })

  it('names each system', () => {
    expect(overloadLabel('portalgun')).toBe('PortalGun, no cap')
    expect(overloadLabel('portalgun-cap256')).toBe('PortalGun, cap 256')
    expect(overloadLabel('stdlib')).toBe('Go httputil.ReverseProxy')
    expect(overloadLabel('mystery')).toBe('mystery')
  })
})

function limiter(cell: string, over: number, invalidRuns = 0): LimiterSummary {
  const s = stat(0)
  return {
    cell,
    runs: 3,
    invalidRuns,
    p50Us: stat(500),
    p99Us: stat(2000),
    overAdmission: stat(over),
    maxAhead: s,
    maxBehind: s,
    worstRatio: stat(1),
    falseAdmits: s,
    falseRejects: s,
    redisRoundTripsPerRequest: stat(1),
    redisCommandsPerRequest: stat(4),
    redisCpuPerRequestUs: stat(100),
    degraded: s,
    droppedDecisions: 0,
  }
}

describe('limiter results', () => {
  it('lines up designs with the loads asked for and skips designs that were not run', () => {
    const rows = limiterRows(
      [limiter('redis-exact/over-even-2gw', 0), limiter('lease-20ms/over-shift-2gw', -0.01), limiter('none/over-even-2gw', 0)],
      ['over-even-2gw', 'over-shift-2gw'],
    )
    expect(rows.map((r) => r.key)).toEqual(['redis-exact', 'lease-20ms'])
    expect(rows[1].cells[0]).toBeUndefined()
    expect(rows[1].cells[1]?.overAdmission.median).toBe(-0.01)
  })

  it('pairs each design measured on both sides of a change, load by load', () => {
    const before = [limiter('redis-exact/over-even-2gw', 0.116), limiter('lease-5ms/over-even-2gw', 0.012), limiter('local-sync-100ms/over-even-2gw', 0)]
    const after = [
      limiter('redis-exact/over-even-2gw', -0.002),
      limiter('redis-exact/zipf-10k-2gw', 0.004),
      limiter('lease-5ms/over-even-2gw', 0),
      limiter('lease-20ms/over-even-2gw', 0),
    ]
    const rows = beforeAfter(before, after, ['over-even-2gw', 'zipf-10k-2gw'])
    expect(rows.map((r) => r.key)).toEqual(['redis-exact', 'lease-5ms'])
    expect(rows[0].before.map((c) => c?.overAdmission.median)).toEqual([0.116, undefined])
    expect(rows[0].after.map((c) => c?.overAdmission.median)).toEqual([-0.002, 0.004])
  })

  it('treats a cell without a valid run as missing', () => {
    expect(valid(limiter('lease-5ms/over-even-2gw', 0, 3))).toBe(false)
    expect(valid(limiter('lease-5ms/over-even-2gw', 0, 2))).toBe(true)
    expect(valid(undefined)).toBe(false)
  })
})

describe('failover results', () => {
  it('builds the fault by detection matrix', () => {
    const f = (c: string) => ({ cell: c, runs: 3, invalidRuns: 0, lost: stat(10) }) as FailoverSummary
    const rows = failoverMatrix([f('crash/both'), f('crash/both-retries'), f('slow/probes'), f('crash/both-retries-refused')])
    expect(rows.map((r) => r.key)).toEqual(['crash', 'slow'])
    expect(rows[0].cells.map((c) => c?.cell)).toEqual([undefined, undefined, 'crash/both', 'crash/both-retries'])
  })
})

describe('registry results', () => {
  const sum = (cell: string, extra: Partial<RegistrySummary> = {}): RegistrySummary => ({
    cell,
    runs: 10,
    failed: 0,
    invalid: 0,
    requests: 1000,
    dropped: 0,
    lost: 0,
    registryVersions: 0,
    lostRegistrations: 0,
    ...extra,
  })

  it('lists the measured steps in their order', () => {
    const rows = registryTimes([sum('failover', { registerMs: stat(2100) }), sum('join', { joinMs: stat(15), gatewaysMs: stat(10) }), sum('down')])
    expect(rows.map((r) => [r.label, r.time.median])).toEqual([
      ['Registered to its first request', 15],
      ['Registered to both gateways on the new config', 10],
      ['Registering while the leader is dead', 2100],
    ])
  })

  it('shows the known cases in order and adds up only those', () => {
    const cases = registryCases([sum('down', { runs: 3, requests: 500 }), sum('partition', { lost: 9 }), sum('join', { lost: 2, failed: 1, dropped: 100 })])
    expect(cases.map((c) => c.key)).toEqual(['join', 'down'])
    expect(registryTotals(cases)).toEqual({ runs: 13, failed: 1, sent: 1400, lost: 2 })
  })
})

describe('units', () => {
  it('formats microseconds, signs and rates', () => {
    expect(us(53.4)).toBe('53 µs')
    expect(us(1530)).toBe('1.5 ms')
    expect(us(25400)).toBe('25 ms')
    expect(us(23_303_600)).toBe('23.3 s')
    expect(signed(us(53), 53)).toBe('+53 µs')
    expect(signed(us(-20), -20)).toBe('-20 µs')
    expect(rps(12500)).toBe('12.5k')
    expect(rps(4000)).toBe('4k')
    expect(rps(800)).toBe('800')
  })
})
