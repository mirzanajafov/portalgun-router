import { dimensionName, errorRate, health, ms, pct, share, type Health } from '@/lib/format'
import type { Merged } from '@/lib/types'

const COLORS: Record<Health, string> = {
  healthy: 'var(--color-portal)',
  'half-open': 'var(--color-warn)',
  ejected: 'var(--color-fault)',
  down: 'var(--color-fault)',
}

const LABELS: Record<Health, string> = {
  healthy: 'healthy',
  'half-open': 'on probation',
  ejected: 'ejected',
  down: 'failing probes',
}

const W = 640
const H = 400
const CX = W / 2
const CY = H / 2

export function position(i: number, n: number): { x: number; y: number } {
  const angle = ((-135 + (i * 360) / n) * Math.PI) / 180
  return { x: CX + Math.cos(angle) * 220, y: CY + Math.sin(angle) * 135 }
}

export function DimensionMap({ latest, canary }: { latest: Merged | null; canary?: string }) {
  const dims = latest?.dimensions ?? []
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label="Traffic from the gateway to each dimension">
      {dims.map((d, i) => {
        const s = share(latest, d.id)
        const h = health(d)
        const { x, y } = position(i, dims.length)
        return (
          <line
            key={`edge-${d.id}`}
            x1={CX}
            y1={CY}
            x2={x}
            y2={y}
            stroke={s > 0 ? COLORS[h] : 'var(--line)'}
            strokeWidth={1.5 + s * 22}
            strokeLinecap="round"
            opacity={s > 0 ? 0.85 : 0.6}
            className={s > 0 ? 'flow' : undefined}
          />
        )
      })}
      <g>
        <ellipse cx={CX} cy={CY} rx={42} ry={54} fill="url(#portal)" />
        <ellipse cx={CX} cy={CY} rx={20} ry={30} fill="var(--bg)" opacity={0.35} />
        <text x={CX} y={CY + 78} textAnchor="middle" className="fill-[var(--muted)] text-[12px]">
          gateway{latest ? ` x${latest.instances.length}` : ''}
        </text>
      </g>
      {dims.map((d, i) => {
        const h = health(d)
        const { x, y } = position(i, dims.length)
        const isCanary = canary && d.version === canary
        return (
          <g key={d.id}>
            <title>{`${dimensionName(d.id)} (${d.version}): ${LABELS[h]}`}</title>
            <circle cx={x} cy={y} r={38} fill="var(--panel)" stroke={COLORS[h]} strokeWidth={3} />
            <text x={x} y={y - 4} textAnchor="middle" className="mono fill-[var(--ink)] text-[17px] font-semibold">
              {d.attempts}
            </text>
            <text x={x} y={y + 13} textAnchor="middle" className="fill-[var(--muted)] text-[10px]">
              req/s
            </text>
            <text x={x} y={y + 58} textAnchor="middle" className="fill-[var(--ink)] text-[13px] font-semibold">
              {dimensionName(d.id)}
              <tspan className={isCanary ? 'fill-[var(--color-warn)]' : 'fill-[var(--muted)]'} dx={6}>
                {d.version}
              </tspan>
            </text>
            <text x={x} y={y + 74} textAnchor="middle" className="mono fill-[var(--muted)] text-[11px]">
              {`p99 ${ms(d.p99Ms)} · ${pct(errorRate(d))} err`}
            </text>
            {h !== 'healthy' && (
              <text x={x} y={y - 48} textAnchor="middle" className="text-[11px] font-semibold" fill={COLORS[h]}>
                {LABELS[h]}
              </text>
            )}
          </g>
        )
      })}
      <defs>
        <radialGradient id="portal" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="#d9ffde" />
          <stop offset="55%" stopColor="#39d353" />
          <stop offset="100%" stopColor="#1f7a33" />
        </radialGradient>
      </defs>
    </svg>
  )
}
