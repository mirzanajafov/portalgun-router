'use client'

import { useEffect, useState } from 'react'
import { dimensionName, remaining, seconds } from '@/lib/format'
import type { ChaosView } from '@/lib/types'

type Action = { label: string; hint: string; body: Record<string, unknown> }

export const ACTIONS: Action[] = [
  { label: 'Kill', hint: 'listener closed, new connections refused', body: { mode: 'crash' } },
  { label: 'Blackhole', hint: 'connections stay open, nothing comes back', body: { mode: 'blackhole' } },
  { label: '+500 ms', hint: 'every response half a second late', body: { mode: 'slow', latencyMs: 500 } },
  { label: '30% errors', hint: 'the dimension answers 503 to 30% of requests', body: { mode: 'partial', errorRate: 0.3 } },
  { label: 'Flapping', hint: 'down two seconds, up two seconds', body: { mode: 'flapping' } },
]

export function ChaosPanel({ chaos, targets, admin }: { chaos: ChaosView | null; targets: string[]; admin: boolean }) {
  const [target, setTarget] = useState<string>('')
  const [message, setMessage] = useState<string>('')
  const [busy, setBusy] = useState(false)
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 500)
    return () => clearInterval(t)
  }, [])

  const chosen = target || targets[0] || ''
  const current = chaos?.current
  const cooldown = Math.max(0, Math.ceil(chaos?.cooldownSec ?? 0))

  async function fire(a: Action) {
    setBusy(true)
    setMessage('')
    try {
      const res = await fetch('/api/chaos', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...a.body, target: chosen, durationSec: 20 }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) setMessage(body.error ?? `refused (${res.status})`)
    } finally {
      setBusy(false)
    }
  }

  async function stop() {
    setBusy(true)
    try {
      await fetch('/api/chaos', { method: 'DELETE' })
    } finally {
      setBusy(false)
    }
  }

  const incidents = (chaos?.incidents ?? []).slice(0, 4)
  return (
    <section className="panel flex flex-col gap-3 p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Break a dimension</h2>
        <span className="muted text-xs">20 s, then it heals</span>
      </div>
      <div className="flex flex-wrap gap-2" role="radiogroup" aria-label="Target dimension">
        {targets.map((t) => (
          <button
            key={t}
            role="radio"
            aria-checked={chosen === t}
            onClick={() => setTarget(t)}
            className={`btn ${chosen === t ? 'border-[var(--color-portal)] font-semibold' : ''}`}
          >
            {dimensionName(t)}
          </button>
        ))}
      </div>
      <div className="flex flex-wrap gap-2">
        {ACTIONS.map((a) => (
          <button key={a.label} className="btn btn-danger" title={a.hint} disabled={busy || !!current || (!admin && cooldown > 0) || !chosen} onClick={() => fire(a)}>
            {a.label}
          </button>
        ))}
      </div>
      <div className="min-h-6 text-sm" aria-live="polite">
        {current ? (
          <span>
            <span className="font-semibold text-[var(--color-fault)]">{current.fault.mode}</span> on {dimensionName(current.fault.target)},{' '}
            <span className="mono">{remaining(current.until, now)} s</span> left{current.actor === 'admin' ? ' (admin)' : ''}
            {admin && (
              <button className="btn ml-3 py-0.5" onClick={stop} disabled={busy}>
                Stop
              </button>
            )}
          </span>
        ) : cooldown > 0 && !admin ? (
          <span className="muted">The portal gun is cooling down: {cooldown} s</span>
        ) : message ? (
          <span className="text-[var(--color-warn)]">{message}</span>
        ) : (
          <span className="muted">Pick a dimension and a way to break it.</span>
        )}
      </div>
      {incidents.length > 0 && (
        <table className="mono w-full text-xs">
          <thead className="muted text-left">
            <tr>
              <th className="pb-1 font-normal">fault</th>
              <th className="pb-1 font-normal">detected</th>
              <th className="pb-1 font-normal">moved</th>
              <th className="pb-1 font-normal">lost</th>
            </tr>
          </thead>
          <tbody>
            {incidents.map((i) => (
              <tr key={i.injectedAt} className="border-t border-[var(--line)]">
                <td className="py-1">
                  {i.fault.mode} {dimensionName(i.fault.target)}
                </td>
                <td>{i.detectedAfterSec !== undefined ? `< ${seconds(i.detectedAfterSec)}` : '-'}</td>
                <td>{i.movedAfterSec !== undefined ? `< ${seconds(i.movedAfterSec)}` : '-'}</td>
                <td>{i.lostRequests}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
