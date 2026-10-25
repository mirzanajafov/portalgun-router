import { z } from 'zod'

const VersionCounts = z.object({ attempts: z.number(), errors: z.number(), slow: z.number() })

const RouteSnapshot = z.object({
  requests: z.number(),
  limited: z.number(),
  failed: z.number(),
  stable: z.string().optional(),
  canary: z.string().optional(),
  weight: z.number(),
  rollout: z.string().optional(),
  versions: z.tuple([VersionCounts, VersionCounts]),
  slowThresholdMs: z.number(),
  brake: z.boolean().optional(),
})

const DimensionSnapshot = z.object({
  id: z.string(),
  version: z.string(),
  attempts: z.number(),
  errors: z.number(),
  p50Ms: z.number(),
  p99Ms: z.number(),
  inflight: z.number(),
  healthy: z.boolean(),
  outlier: z.enum(['closed', 'ejected', 'half-open', 'disabled']),
  ejectedForMs: z.number().optional(),
})
export type DimensionSnapshot = z.infer<typeof DimensionSnapshot>

const Merged = z.object({
  second: z.number(),
  instances: z.array(z.string()),
  configVersions: z.record(z.string(), z.number()),
  routes: z.record(z.string(), RouteSnapshot),
  dimensions: z.array(DimensionSnapshot),
})
export type Merged = z.infer<typeof Merged>

const Step = z.object({
  step: z.number(),
  weight: z.number(),
  startedAt: z.string(),
  endedAt: z.string().optional(),
  decision: z.enum(['promote', 'rollback']).optional(),
  reason: z.string().optional(),
  llrErrors: z.number(),
  llrSlow: z.number(),
  stableRequests: z.number(),
  canaryRequests: z.number(),
  stableErrors: z.number(),
  canaryErrors: z.number(),
})

const Outcome = z.enum(['promoted', 'rolled_back', 'aborted'])
export type Outcome = z.infer<typeof Outcome>

const Rollout = z.object({
  id: z.string(),
  route: z.string(),
  stable: z.string(),
  canary: z.string(),
  startedAt: z.string(),
  finishedAt: z.string().optional(),
  outcome: Outcome.optional(),
  reason: z.string().optional(),
  steps: z.array(Step).optional(),
})

const RolloutStatus = z.object({
  active: z.boolean(),
  rollout: Rollout.optional(),
  step: Step.optional(),
  steps: z.array(z.number()),
  dataSec: z.number(),
  llrErrors: z.number(),
  llrSlow: z.number(),
  lower: z.number(),
  upper: z.number(),
  pending: Outcome.optional(),
})
export type RolloutStatus = z.infer<typeof RolloutStatus>

const Actor = z.enum(['bootstrap', 'admin', 'visitor', 'controller', 'registry'])

const Fault = z.object({
  mode: z.enum(['crash', 'reset', 'blackhole', 'slow', 'partial', 'flapping']),
  target: z.string(),
  latency: z.number().optional(),
  errorRate: z.number().optional(),
  period: z.number().optional(),
})

const Running = z.object({ fault: Fault, actor: Actor, startedAt: z.string(), until: z.string() })

const Incident = z.object({
  fault: Fault,
  actor: Actor,
  injectedAt: z.string(),
  clearedAt: z.string().optional(),
  detectedAfterSec: z.number().optional(),
  movedAfterSec: z.number().optional(),
  lostRequests: z.number(),
  closed: z.boolean(),
})

const ChaosView = z.object({ current: Running.nullable(), cooldownSec: z.number(), targets: z.array(z.string()), incidents: z.array(Incident) })
export type ChaosView = z.infer<typeof ChaosView>

export const PortalEvent = z.object({ type: z.enum(['config', 'chaos', 'rollout']), at: z.string(), message: z.string(), actor: Actor.optional() })
export type PortalEvent = z.infer<typeof PortalEvent>

export const Tick = z.object({ latest: Merged, rollout: RolloutStatus, viewers: z.number(), chaos: ChaosView.optional() })

export const State = z.object({
  configVersion: z.number(),
  latest: Merged.nullable(),
  rollout: RolloutStatus,
  viewers: z.number(),
  events: z.array(PortalEvent),
})

export const Session = z.object({ admin: z.boolean() })
export type Session = z.infer<typeof Session>

export const Burst = z.object({ sent: z.number(), allowed: z.number(), limited: z.number(), failed: z.number(), ms: z.number() })
export type Burst = z.infer<typeof Burst>

export const Refusal = z.object({ error: z.string() })
export type Refusal = z.infer<typeof Refusal>
