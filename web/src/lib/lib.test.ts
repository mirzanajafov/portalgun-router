import { describe, expect, it } from 'vitest'
import { candidate } from '@/components/CanaryPanel'
import { positions } from '@/components/DimensionMap'
import { worthShowing } from '@/components/EventLog'
import { scale } from '@/components/LlrChart'
import { errorRate, health, pct, remaining, share } from './format'
import { appendHistory, appendLlr } from './live'
import type { DimensionSnapshot, Merged, RolloutStatus } from './types'

const dim = (over: Partial<DimensionSnapshot>): DimensionSnapshot => ({
  id: 'c-137',
  version: 'v1',
  attempts: 100,
  errors: 0,
  p50Ms: 10,
  p99Ms: 30,
  inflight: 0,
  healthy: true,
  outlier: 'closed',
  ...over,
})

const merged = (dims: DimensionSnapshot[], stable = 'v1', canary?: string): Merged => ({
  second: 100,
  instances: ['gw-1'],
  configVersions: { 'gw-1': 1 },
  routes: {
    portal: {
      requests: 0,
      limited: 0,
      failed: 0,
      stable,
      canary,
      weight: 0,
      versions: [
        { attempts: 0, errors: 0, slow: 0 },
        { attempts: 0, errors: 0, slow: 0 },
      ],
      slowThresholdMs: 0,
    },
  },
  dimensions: dims,
})

describe('format', () => {
  it('ranks health from the probes and the breaker', () => {
    expect(health(dim({}))).toBe('healthy')
    expect(health(dim({ outlier: 'ejected' }))).toBe('ejected')
    expect(health(dim({ outlier: 'half-open' }))).toBe('half-open')
    expect(health(dim({ healthy: false, outlier: 'ejected' }))).toBe('down')
  })

  it('computes shares and rates without dividing by zero', () => {
    const m = merged([dim({ id: 'a', attempts: 30 }), dim({ id: 'b', attempts: 10 })])
    expect(share(m, 'a')).toBe(0.75)
    expect(share(m, 'missing')).toBe(0)
    expect(share(merged([dim({ attempts: 0 })]), 'c-137')).toBe(0)
    expect(errorRate(dim({ attempts: 0, errors: 0 }))).toBe(0)
    expect(errorRate(dim({ attempts: 200, errors: 5 }))).toBe(0.025)
  })

  it('formats percentages and countdowns', () => {
    expect(pct(0.25)).toBe('25%')
    expect(pct(0.0125, 2)).toBe('1.25%')
    expect(pct(Number.NaN)).toBe('-')
    expect(remaining(new Date(10_500).toISOString(), 5_000)).toBe(6)
    expect(remaining(new Date(1_000).toISOString(), 5_000)).toBe(0)
  })
})

describe('live state', () => {
  it('keeps a minute of history per dimension', () => {
    let h: Record<string, number[]> = {}
    for (let i = 0; i < 70; i++) h = appendHistory(h, merged([dim({ attempts: i })]))
    expect(h['c-137']).toHaveLength(60)
    expect(h['c-137'].at(-1)).toBe(69)
  })

  it('restarts the evidence curve when a new rollout begins', () => {
    const status = (id: string, step: number, llr: number): RolloutStatus => ({
      active: true,
      rollout: { id, route: 'portal', stable: 'v1', canary: 'v2', startedAt: '', steps: [] },
      step: { step, weight: 0.01, startedAt: '', llrErrors: llr, llrSlow: 0, stableRequests: 0, canaryRequests: 0, stableErrors: 0, canaryErrors: 0 },
      steps: [0.01],
      dataSec: 1,
      llrErrors: llr,
      llrSlow: 0,
      lower: -3,
      upper: 6,
    })
    let pts = appendLlr([], status('ro-1', 0, 1), 1)
    pts = appendLlr(pts, status('ro-1', 0, 2), 2)
    expect(pts).toHaveLength(2)
    pts = appendLlr(pts, status('ro-2', 0, 0.5), 3)
    expect(pts).toHaveLength(1)
    expect(pts[0].errors).toBe(0.5)
    expect(appendLlr(pts, { ...status('ro-2', 0, 1), active: false }, 4)).toBe(pts)
  })
})

describe('panels', () => {
  it('offers the version that is not stable as the next canary', () => {
    const m = merged([dim({ id: 'a' }), dim({ id: 'b', version: 'v2' }), dim({ id: 'c', version: 'v2' })])
    expect(candidate(m)).toEqual({ stable: 'v1', canary: 'v2', target: 'b', hosts: ['b', 'c'] })
    expect(candidate(null).hosts).toEqual([])
  })

  it('hides config lines the controller writes for its own decisions', () => {
    expect(worthShowing({ type: 'config', at: '', message: 'config v5 by controller: step 2' })).toBe(false)
    expect(worthShowing({ type: 'config', at: '', message: 'config v4 by admin: rollout starts' })).toBe(true)
    expect(worthShowing({ type: 'rollout', at: '', message: 'rollout promoted' })).toBe(true)
  })

  it('places dimensions around the gateway without overlap', () => {
    const p = positions(4)
    for (let i = 0; i < p.length; i++) {
      for (let j = i + 1; j < p.length; j++) {
        expect(Math.hypot(p[i].x - p[j].x, p[i].y - p[j].y)).toBeGreaterThan(160)
      }
    }
  })

  it('keeps the bounds of the evidence chart in view', () => {
    const { y } = scale(
      [
        { t: 1, errors: 0, slow: 0, step: 0, rollout: 'ro-1' },
        { t: 2, errors: 40, slow: -50, step: 0, rollout: 'ro-1' },
      ],
      -3,
      6,
    )
    expect(y(6)).toBeGreaterThanOrEqual(0)
    expect(y(-3)).toBeLessThanOrEqual(150)
    expect(y(-50)).toBeLessThanOrEqual(150)
  })
})
