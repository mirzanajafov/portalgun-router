'use client'

import { useEffect, useRef, useState } from 'react'
import type { ChaosView, Merged, PortalEvent, RolloutStatus, State, Tick } from './types'

export type LlrPoint = { t: number; errors: number; slow: number; step: number }

export type Live = {
  connected: boolean
  latest: Merged | null
  rollout: RolloutStatus | null
  chaos: ChaosView | null
  viewers: number
  events: PortalEvent[]
  llr: LlrPoint[]
  history: Record<string, number[]>
  configVersion: number
}

const HISTORY = 60
const LLR_POINTS = 600

export function appendHistory(history: Record<string, number[]>, m: Merged): Record<string, number[]> {
  const next: Record<string, number[]> = {}
  for (const d of m.dimensions) {
    const prev = history[d.id] ?? []
    next[d.id] = [...prev.slice(-(HISTORY - 1)), d.attempts]
  }
  return next
}

export function appendLlr(points: LlrPoint[], r: RolloutStatus, second: number): LlrPoint[] {
  if (!r.active || !r.step) return points
  const key = r.rollout?.id
  const kept = points.length && points[0] && key && (points as (LlrPoint & { id?: string })[])[0].id !== key ? [] : points
  const p = { t: second, errors: r.llrErrors, slow: r.llrSlow, step: r.step.step, id: key }
  return [...kept.slice(-(LLR_POINTS - 1)), p]
}

export function useLive(): Live {
  const [live, setLive] = useState<Live>({
    connected: false,
    latest: null,
    rollout: null,
    chaos: null,
    viewers: 0,
    events: [],
    llr: [],
    history: {},
    configVersion: 0,
  })
  const retry = useRef(1000)

  useEffect(() => {
    let source: EventSource | null = null
    let timer: ReturnType<typeof setTimeout> | undefined
    let closed = false

    const open = () => {
      source = new EventSource('/api/stream')
      source.onopen = () => {
        retry.current = 1000
        setLive((l) => ({ ...l, connected: true }))
      }
      source.addEventListener('state', (e) => {
        const s = JSON.parse((e as MessageEvent).data) as State
        setLive((l) => ({ ...l, rollout: s.rollout, viewers: s.viewers, latest: s.latest ?? l.latest, configVersion: s.configVersion, events: s.events ?? [] }))
      })
      source.addEventListener('tick', (e) => {
        const t = JSON.parse((e as MessageEvent).data) as Tick
        setLive((l) => ({
          ...l,
          latest: t.latest,
          rollout: t.rollout,
          viewers: t.viewers,
          chaos: t.chaos ?? l.chaos,
          history: appendHistory(l.history, t.latest),
          llr: appendLlr(l.llr, t.rollout, t.latest.second),
        }))
      })
      source.addEventListener('event', (e) => {
        const ev = JSON.parse((e as MessageEvent).data) as PortalEvent
        setLive((l) => (l.events.some((x) => x.at === ev.at && x.message === ev.message) ? l : { ...l, events: [...l.events.slice(-49), ev] }))
      })
      source.onerror = () => {
        source?.close()
        setLive((l) => ({ ...l, connected: false }))
        if (!closed) {
          timer = setTimeout(open, retry.current)
          retry.current = Math.min(retry.current * 2, 15000)
        }
      }
    }
    open()
    return () => {
      closed = true
      clearTimeout(timer)
      source?.close()
    }
  }, [])

  return live
}
