'use client'

import { useState } from 'react'
import { dimensionName, pct } from '@/lib/format'
import type { LlrPoint } from '@/lib/live'
import type { Merged, Outcome, PortalEvent, RolloutStatus } from '@/lib/types'
import { LlrChart } from './LlrChart'

export const ROUTE = 'portal'

const outcomeLabel: Record<Outcome, string> = { promoted: 'promoted', rolled_back: 'rolled back', aborted: 'aborted' }

export function candidate(latest: Merged | null): { stable?: string; canary?: string; target?: string; hosts: string[] } {
  const route = latest?.routes[ROUTE]
  const stable = route?.stable
  const other = latest?.dimensions.find((d) => d.version !== stable)
  const canary = route?.canary || other?.version
  const hosts = (latest?.dimensions ?? []).filter((d) => d.version === canary).map((d) => d.id)
  return { stable, canary, target: hosts[0], hosts }
}

export function CanaryPanel({
  latest,
  rollout,
  llr,
  events,
  admin,
}: {
  latest: Merged | null
  rollout: RolloutStatus | null
  llr: LlrPoint[]
  events: PortalEvent[]
  admin: boolean
}) {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const { stable, canary, target, hosts } = candidate(latest)
  const active = rollout?.active ? rollout : null
  const lastDecision = [...events].reverse().find((e) => e.type === 'rollout')

  async function call(url: string, body: unknown) {
    setBusy(true)
    setMessage('')
    try {
      const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const out = await res.json().catch(() => ({}))
      if (!res.ok) setMessage(out.error ?? `refused (${res.status})`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="panel flex flex-col gap-3 p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Canary</h2>
        <span className="muted text-xs">sequential test, at most 1% false rollbacks</span>
      </div>
      {active ? (
        <>
          <p className="text-sm">
            <span className="font-semibold text-[var(--color-warn)]">{active.rollout?.canary}</span> replacing {active.rollout?.stable} on{' '}
            {`/${active.rollout?.route ?? ''}/`}, step {(active.step?.step ?? 0) + 1} of {active.steps.length} at{' '}
            <span className="mono font-semibold">{pct(active.step?.weight ?? 0)}</span>
          </p>
          <ol className="flex gap-1" aria-label="Rollout steps">
            {active.steps.map((w, i) => {
              const now = active.step?.step ?? 0
              return (
                <li
                  key={w}
                  className={`mono flex-1 rounded-md px-1 py-0.5 text-center text-xs ${
                    i < now
                      ? 'bg-[var(--color-portal-dim)] text-white'
                      : i === now
                        ? 'bg-[var(--color-portal)] font-semibold text-black'
                        : 'border border-[var(--line)]'
                  }`}
                >
                  {pct(w, 0)}
                </li>
              )
            })}
          </ol>
          <div className="mono muted flex flex-wrap gap-x-4 text-xs">
            <span>{Math.round(active.elapsedSec)} s in step</span>
            <span>{active.step?.canaryRequests ?? 0} canary req</span>
            <span>{active.step?.canaryErrors ?? 0} canary errors</span>
            <span>{active.step?.stableErrors ?? 0} stable errors</span>
          </div>
          {active.pending ? (
            <p className="text-xs text-[var(--color-warn)]">
              Decided: {outcomeLabel[active.pending]}. The controller keeps saving it until the database answers.
            </p>
          ) : null}
        </>
      ) : (
        <p className="text-sm">
          {stable ? (
            <>
              Everyone is on <span className="font-semibold">{stable}</span>.
              {canary && canary !== stable ? (
                <>
                  {' '}
                  The canary, {canary}, runs on {hosts.length ? new Intl.ListFormat('en').format(hosts.map(dimensionName)) : 'no dimension'} but gets no
                  traffic. Start rolling it out, then break it and watch the test take it back.
                </>
              ) : null}
            </>
          ) : (
            'Waiting for the first snapshot.'
          )}
        </p>
      )}
      <LlrChart points={llr} lower={rollout?.lower || -3} upper={rollout?.upper || 6} />
      <div className="muted flex gap-4 text-xs">
        <span>
          <span className="inline-block h-0.5 w-4 bg-[var(--color-fluid)] align-middle" /> errors
        </span>
        <span>
          <span className="inline-block h-0.5 w-4 bg-[var(--color-warn)] align-middle" /> slow responses
        </span>
      </div>
      <div className="flex flex-wrap gap-2">
        {active && target && (
          <button className="btn btn-danger" disabled={busy} onClick={() => call('/api/chaos', { mode: 'partial', target, errorRate: 0.5, durationSec: 30 })}>
            Break {canary}: 50% errors
          </button>
        )}
        {!active && canary && canary !== stable && (
          <button className="btn btn-primary" disabled={busy} onClick={() => call('/api/rollouts', { route: ROUTE, canary })}>
            Start rolling out {canary}
          </button>
        )}
        {admin && active && (
          <button className="btn" disabled={busy} onClick={() => call('/api/rollouts/abort', { reason: 'aborted from the dashboard' })}>
            Abort
          </button>
        )}
      </div>
      <p className="min-h-5 text-sm" aria-live="polite">
        {message ? <span className="text-[var(--color-warn)]">{message}</span> : lastDecision ? <span className="muted">{lastDecision.message}</span> : null}
      </p>
    </section>
  )
}
