'use client'

import type { LlrPoint } from '@/lib/live'

const W = 560
const H = 150
const PAD = 8

export function scale(points: LlrPoint[], lower: number, upper: number) {
  const lo = Math.min(lower - 1, ...points.map((p) => Math.min(p.errors, p.slow)))
  const hi = Math.max(upper + 1, ...points.map((p) => Math.max(p.errors, p.slow)))
  const clampedLo = Math.max(lo, lower * 4)
  const n = Math.max(points.length - 1, 1)
  const x = (i: number) => PAD + (i / n) * (W - 2 * PAD)
  const y = (v: number) => {
    const c = Math.min(hi, Math.max(clampedLo, v))
    return PAD + ((hi - c) / (hi - clampedLo)) * (H - 2 * PAD)
  }
  return { x, y }
}

function line(points: LlrPoint[], pick: (p: LlrPoint) => number, x: (i: number) => number, y: (v: number) => number) {
  return points.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(pick(p)).toFixed(1)}`).join(' ')
}

export function LlrChart({ points, lower, upper }: { points: LlrPoint[]; lower: number; upper: number }) {
  if (points.length < 2) {
    return <div className="muted grid h-[150px] place-items-center text-sm">The evidence shows up here once a rollout runs.</div>
  }
  const { x, y } = scale(points, lower, upper)
  const steps = points.map((p, i) => (i > 0 && p.step !== points[i - 1].step ? i : -1)).filter((i) => i > 0)
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label="Log-likelihood ratio of the canary being worse, over time">
      <line x1={PAD} x2={W - PAD} y1={y(upper)} y2={y(upper)} stroke="var(--color-fault)" strokeDasharray="4 4" />
      <text x={W - PAD} y={y(upper) - 4} textAnchor="end" className="fill-[var(--color-fault)] text-[10px]">
        roll back
      </text>
      <line x1={PAD} x2={W - PAD} y1={y(lower)} y2={y(lower)} stroke="var(--color-portal)" strokeDasharray="4 4" />
      <text x={W - PAD} y={y(lower) + 12} textAnchor="end" className="fill-[var(--color-portal)] text-[10px]">
        not worse
      </text>
      <line x1={PAD} x2={W - PAD} y1={y(0)} y2={y(0)} stroke="var(--line)" />
      {steps.map((i) => (
        <line key={i} x1={x(i)} x2={x(i)} y1={PAD} y2={H - PAD} stroke="var(--line)" />
      ))}
      <path d={line(points, (p) => p.slow, x, y)} fill="none" stroke="var(--color-warn)" strokeWidth={2} />
      <path d={line(points, (p) => p.errors, x, y)} fill="none" stroke="var(--color-fluid)" strokeWidth={2} />
    </svg>
  )
}
