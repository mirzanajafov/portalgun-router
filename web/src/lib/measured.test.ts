import { describe, expect, it } from 'vitest'
import { commonRate, failoverMatrix, limiterRows, proxyRows, rps, signed, us, valid, type FailoverSummary, type LimiterSummary, type ProxyCell, type ProxyReport } from './measured'

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
    cells: [cell('direct', 1000), cell('stdlib', 1000), cell('envoy', 1000), cell('direct', 4000), cell('stdlib', 4000), cell('envoy', 4000, 2), cell('stdlib', 6000)],
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
    const rows = limiterRows([limiter('redis-exact/over-even-2gw', 0), limiter('lease-20ms/over-shift-2gw', -0.01), limiter('none/over-even-2gw', 0)], ['over-even-2gw', 'over-shift-2gw'])
    expect(rows.map((r) => r.key)).toEqual(['redis-exact', 'lease-20ms'])
    expect(rows[1].cells[0]).toBeUndefined()
    expect(rows[1].cells[1]?.overAdmission.median).toBe(-0.01)
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

describe('units', () => {
  it('formats microseconds, signs and rates', () => {
    expect(us(53.4)).toBe('53 µs')
    expect(us(1530)).toBe('1.5 ms')
    expect(us(25400)).toBe('25 ms')
    expect(signed(us(53), 53)).toBe('+53 µs')
    expect(signed(us(-20), -20)).toBe('-20 µs')
    expect(rps(12500)).toBe('12.5k')
    expect(rps(4000)).toBe('4k')
    expect(rps(800)).toBe('800')
  })
})
