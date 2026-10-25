'use client'

import { useEffect, useRef, useState } from 'react'
import type { ZodMiniType } from 'zod/mini'
import { PortalEvent, State, Tick, type ChaosView, type Merged, type RolloutStatus } from './types'

export type LlrPoint = { t: number; errors: number; slow: number; step: number; rollout: string }

export type Live = {
  connected: boolean
  latest: Merged | null
  rollout: RolloutStatus | null
  chaos: ChaosView | null
  viewers: number
  events: PortalEvent[]
  llr: LlrPoint[]
}

const LLR_POINTS = 600

export function appendLlr(points: LlrPoint[], r: RolloutStatus, second: number): LlrPoint[] {
  if (!r.active || !r.step || !r.rollout) return points
  const rollout = r.rollout.id
  const kept = points[0]?.rollout === rollout ? points : []
  return [...kept.slice(-(LLR_POINTS - 1)), { t: second, errors: r.llrErrors, slow: r.llrSlow, step: r.step.step, rollout }]
}

function listen<T>(source: EventSource, type: string, schema: ZodMiniType<T>, apply: (value: T) => void) {
  source.addEventListener(type, (e: MessageEvent<string>) => {
    const parsed = schema.safeParse(JSON.parse(e.data))
    if (parsed.success) apply(parsed.data)
    else console.error(`the controller sent a ${type} event the dashboard cannot read`, parsed.error)
  })
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
      listen(source, 'state', State, (s) => {
        setLive((l) => ({ ...l, rollout: s.rollout, viewers: s.viewers, latest: s.latest ?? l.latest, events: s.events }))
      })
      listen(source, 'tick', Tick, (t) => {
        setLive((l) => ({
          ...l,
          latest: t.latest,
          rollout: t.rollout,
          viewers: t.viewers,
          chaos: t.chaos ?? l.chaos,
          llr: appendLlr(l.llr, t.rollout, t.latest.second),
        }))
      })
      listen(source, 'event', PortalEvent, (ev) => {
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
