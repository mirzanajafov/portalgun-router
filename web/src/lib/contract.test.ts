import { readFileSync } from 'node:fs'
import path from 'node:path'
import { describe, expect, it } from 'vitest'
import * as z from 'zod/mini'
import { State, Tick } from './types'

const Contract = z.object({ firstState: State, state: State, tick: Tick, quietTick: Tick, settlingTick: Tick })

function contract() {
  const file = path.join(process.cwd(), '..', 'internal', 'controller', 'testdata', 'dashboard.json')
  return Contract.parse(JSON.parse(readFileSync(file, 'utf8')))
}

describe('the controller contract', () => {
  it('reads every frame the controller sends, as the controller tests write them', () => {
    const { firstState, quietTick } = contract()
    expect(firstState.latest).toBeNull()
    expect(firstState.events).toEqual([])
    expect(quietTick.latest.dimensions).toEqual([])
    expect(quietTick.chaos?.targets).toEqual([])
  })

  it('finds every optional field the dashboard reads where the controller sends it', () => {
    const { state, tick, settlingTick } = contract()
    const route = tick.latest.routes.portal
    const incident = tick.chaos?.incidents[0]
    expect(route?.stable).toBeDefined()
    expect(route?.canary).toBeDefined()
    expect(tick.rollout.rollout).toBeDefined()
    expect(tick.rollout.step).toBeDefined()
    expect(tick.chaos?.current?.until).toBeDefined()
    expect(incident?.detectedAfterSec).toBeDefined()
    expect(incident?.movedAfterSec).toBeDefined()
    expect(settlingTick.rollout.pending).toBeDefined()
    expect(state.events.map((e) => [e.type, e.actor])).toEqual([
      ['config', 'admin'],
      ['chaos', undefined],
      ['rollout', undefined],
    ])
  })
})
