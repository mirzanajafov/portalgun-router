import { dimensionName, errorRate, health, ms, pct, type Health } from '@/lib/format'
import type { Merged } from '@/lib/types'

const TONE: Record<Health, string> = {
  healthy: 'border-[var(--color-portal)]',
  'half-open': 'border-[var(--color-warn)]',
  ejected: 'border-[var(--color-fault)]',
  down: 'border-[var(--color-fault)]',
}

export function DimensionCards({ latest, canary }: { latest: Merged | null; canary?: string }) {
  const dims = latest?.dimensions ?? []
  return (
    <ul className="grid grid-cols-2 gap-2">
      {dims.map((d) => {
        const h = health(d)
        return (
          <li key={d.id} className={`rounded-lg border-l-4 bg-[var(--bg)] px-2.5 py-1.5 ${TONE[h]}`}>
            <div className="flex items-baseline justify-between gap-1">
              <span className="truncate text-sm font-semibold">{dimensionName(d.id)}</span>
              <span className={`text-xs ${canary && d.version === canary ? 'text-[var(--color-warn)]' : 'muted'}`}>{d.version}</span>
            </div>
            <div className="mono text-xs">
              <span className="font-semibold">{d.attempts}</span> <span className="muted">req/s · p99 {ms(d.p99Ms)}</span>
            </div>
            <div className="mono muted text-xs">
              {pct(errorRate(d))} err{h !== 'healthy' ? ` · ${h}` : ''}
            </div>
          </li>
        )
      })}
    </ul>
  )
}
