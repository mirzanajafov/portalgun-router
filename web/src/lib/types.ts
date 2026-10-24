export type VersionCounts = { attempts: number; errors: number; slow: number }

export type RouteSnapshot = {
  requests: number
  limited: number
  failed: number
  stable?: string
  canary?: string
  weight: number
  rollout?: string
  versions: [VersionCounts, VersionCounts]
  slowThresholdMs: number
  brake?: boolean
}

export type OutlierState = 'closed' | 'ejected' | 'half-open' | 'disabled'

export type DimensionSnapshot = {
  id: string
  version: string
  attempts: number
  errors: number
  p50Ms: number
  p99Ms: number
  inflight: number
  healthy: boolean
  outlier: OutlierState
  ejectedForMs?: number
}

export type Merged = {
  second: number
  instances: string[]
  configVersions: Record<string, number>
  routes: Record<string, RouteSnapshot>
  dimensions: DimensionSnapshot[]
}

export type Step = {
  step: number
  weight: number
  startedAt: string
  endedAt?: string
  decision?: string
  reason?: string
  llrErrors: number
  llrSlow: number
  stableRequests: number
  canaryRequests: number
  stableErrors: number
  canaryErrors: number
}

export type Rollout = {
  id: string
  route: string
  stable: string
  canary: string
  startedAt: string
  finishedAt?: string
  outcome?: Outcome
  reason?: string
  steps: Step[] | null
}

export type Outcome = 'promoted' | 'rolled_back' | 'aborted'

export type RolloutStatus = {
  active: boolean
  rollout?: Rollout
  step?: Step
  steps: number[]
  dataSec: number
  elapsedSec: number
  llrErrors: number
  llrSlow: number
  lower: number
  upper: number
  pending?: Outcome
}

export type Fault = { mode: string; target: string; latency?: number; errorRate?: number; period?: number }

export type Running = { fault: Fault; actor: string; startedAt: string; until: string }

export type Incident = {
  fault: Fault
  actor: string
  injectedAt: string
  clearedAt?: string
  detectedAfterSec?: number
  movedAfterSec?: number
  lostRequests: number
  closed: boolean
}

export type ChaosView = { current: Running | null; cooldownSec: number; targets: string[] | null; incidents: Incident[] | null }

export type PortalEvent = { type: string; at: string; message: string }

export type Tick = { latest: Merged; rollout: RolloutStatus; viewers: number; chaos?: ChaosView }

export type State = { configVersion: number; latest: Merged | null; rollout: RolloutStatus; viewers: number; events: PortalEvent[] | null }
