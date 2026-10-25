'use client'

import { useState } from 'react'
import { useAction } from '@/lib/action'
import { Burst, type Merged } from '@/lib/types'

export function FluidPanel({ latest }: { latest: Merged | null }) {
  const [burst, setBurst] = useState<Burst | null>(null)
  const { busy, message, send } = useAction()
  const fluid = latest?.routes.fluid

  const cells = burst
    ? Array.from({ length: burst.sent }, (_, i) => (i < burst.allowed ? 'allowed' : i < burst.allowed + burst.limited ? 'limited' : 'failed'))
    : []
  return (
    <section className="panel flex flex-col gap-3 p-4">
      <div className="flex items-baseline justify-between gap-2">
        <h2 className="font-semibold">Portal fluid</h2>
        <span className="muted text-xs">20/s, burst 40, synced between both gateways</span>
      </div>
      <p className="muted text-sm">
        Your own bucket on /fluid/. Fire 60 requests at once and watch where it runs dry. They all land on one gateway, and until the two gateways sync (every
        100 ms) a client they haven&apos;t seen gets half the burst on each.
      </p>
      <div className="flex items-center gap-3">
        <button
          className="btn btn-primary"
          onClick={() => send('/api/burst', { method: 'POST' }, async (res) => setBurst(Burst.parse(await res.json())))}
          disabled={busy}
        >
          Fire 60 requests
        </button>
        {burst && (
          <span className="mono text-sm">
            <span className="text-[var(--color-portal)]">{burst.allowed} through</span> ·{' '}
            <span className="text-[var(--color-fluid)]">{burst.limited} got 429</span>
            {burst.failed > 0 && <span className="text-[var(--color-fault)]"> · {burst.failed} failed</span>} · {burst.ms} ms
          </span>
        )}
      </div>
      {cells.length > 0 && (
        <div className="grid grid-cols-[repeat(20,minmax(0,1fr))] gap-1" aria-hidden>
          {cells.map((c, i) => (
            <span
              key={i}
              className={`h-3 rounded-sm ${c === 'allowed' ? 'bg-[var(--color-portal)]' : c === 'limited' ? 'bg-[var(--color-fluid)]' : 'bg-[var(--color-fault)]'}`}
            />
          ))}
        </div>
      )}
      <p className="min-h-5 text-sm">
        {message ? (
          <span className="text-[var(--color-warn)]">{message}</span>
        ) : (
          <span className="muted mono">{fluid ? `last second on /fluid/: ${fluid.requests} requests, ${fluid.limited} limited` : ''}</span>
        )}
      </p>
    </section>
  )
}
