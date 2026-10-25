'use client'

import Link from 'next/link'
import { candidate } from '@/lib/canary'
import { pct, totalRps } from '@/lib/format'
import { useLive } from '@/lib/live'
import { CanaryPanel } from './CanaryPanel'
import { ChaosPanel } from './ChaosPanel'
import { DimensionCards } from './DimensionCards'
import { DimensionMap } from './DimensionMap'
import { EventLog } from './EventLog'
import { FluidPanel } from './FluidPanel'

export function Dashboard({ admin }: { admin: boolean }) {
  const live = useLive()
  const m = live.latest
  const rps = totalRps(m)
  const failed = m ? Object.values(m.routes).reduce((n, r) => n + r.failed, 0) : 0
  const limited = m ? Object.values(m.routes).reduce((n, r) => n + r.limited, 0) : 0
  const targets = live.chaos?.targets ?? m?.dimensions.map((d) => d.id) ?? []
  const { stable, canary } = candidate(m)
  const rolling = canary !== stable ? canary : undefined

  return (
    <div className="flex flex-col gap-4">
      <section className="flex flex-wrap items-center justify-between gap-3">
        <p className="muted max-w-2xl text-sm">
          Every request picks a dimension. Two gateways spread traffic by latency and health, eject what breaks, keep each client inside its quota and decide
          releases with a sequential test. The traffic is simulated and starts when you open this page, but the failures are real.
        </p>
        <div className="flex items-center gap-3 text-sm">
          <span className={`inline-block h-2 w-2 rounded-full ${live.connected ? 'bg-[var(--color-portal)]' : 'bg-[var(--color-fault)]'}`} />
          <span className="muted">{live.connected ? `${live.viewers} watching` : 'reconnecting'}</span>
          {admin ? (
            <span className="rounded-md border border-[var(--color-warn)] px-1.5 text-xs text-[var(--color-warn)]">admin</span>
          ) : (
            <Link href="/admin" className="muted text-xs underline-offset-2 hover:underline">
              admin
            </Link>
          )}
        </div>
      </section>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        <section className="panel p-3">
          <div className="mono flex flex-wrap gap-x-5 gap-y-1 px-1 text-sm">
            <span>
              <span className="font-semibold">{rps}</span> <span className="muted">req/s</span>
            </span>
            <span>
              <span className="font-semibold">{failed}</span> <span className="muted">failed</span>
              {rps > 0 && <span className="muted"> ({pct(failed / rps, 2)})</span>}
            </span>
            <span>
              <span className="font-semibold">{limited}</span> <span className="muted">429</span>
            </span>
            <span className="muted">{m ? `${m.instances.length} gateways` : 'no data yet'}</span>
          </div>
          <DimensionMap latest={m} canary={rolling} />
          <div className="sm:hidden">
            <DimensionCards latest={m} canary={rolling} />
          </div>
        </section>
        <div className="flex flex-col gap-4">
          <ChaosPanel chaos={live.chaos} targets={targets} admin={admin} />
          <FluidPanel latest={m} />
        </div>
      </div>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        <CanaryPanel latest={m} rollout={live.rollout} llr={live.llr} events={live.events} admin={admin} />
        <EventLog events={live.events} />
      </div>
    </div>
  )
}
