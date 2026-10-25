import { readFileSync } from 'node:fs'
import path from 'node:path'
import { describe, expect, it } from 'vitest'
import { z } from 'zod'
import { State, Tick } from './types'

const Contract = z.object({ firstState: State, state: State, tick: Tick, quietTick: Tick })

describe('the controller contract', () => {
  it('reads every frame the controller sends, as the controller tests write them', () => {
    const file = path.join(process.cwd(), '..', 'internal', 'controller', 'testdata', 'dashboard.json')
    const contract = Contract.parse(JSON.parse(readFileSync(file, 'utf8')))
    expect(contract.firstState.latest).toBeNull()
    expect(contract.state.events.map((e) => e.type)).toEqual(['config', 'chaos', 'rollout'])
    expect(contract.tick.chaos?.current?.fault.mode).toBe('crash')
    expect(contract.quietTick.chaos?.targets).toEqual([])
  })
})
