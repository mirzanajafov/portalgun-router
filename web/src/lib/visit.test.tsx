import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChaosPanel } from '@/components/ChaosPanel'
import { FluidPanel } from '@/components/FluidPanel'
import type { ChaosView } from './types'

let steps: unknown[] = []
const record = (event: Event) => {
  steps.push(event instanceof CustomEvent ? event.detail : event)
}

beforeEach(() => {
  steps = []
  document.addEventListener('najafov:action', record)
})

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  document.removeEventListener('najafov:action', record)
})

function answer(res: Response) {
  const fetch = vi.fn(() => Promise.resolve(res))
  vi.stubGlobal('fetch', fetch)
  return fetch
}

const running: ChaosView = {
  current: { fault: { mode: 'crash', target: 'c-137' }, actor: 'visitor', until: new Date(Date.now() + 20_000).toISOString() },
  cooldownSec: 0,
  targets: ['c-137'],
  incidents: [],
}

describe('demo steps', () => {
  it('hands a burst to the visit script once all 60 requests have gone out', async () => {
    answer(Response.json({ sent: 60, allowed: 40, limited: 20, failed: 0, ms: 12 }))
    render(<FluidPanel latest={null} />)
    fireEvent.click(screen.getByRole('button', { name: 'Fire 60 requests' }))
    await screen.findByText('40 through')
    expect(steps).toEqual(['fired'])
  })

  it('sends nothing for a refused burst', async () => {
    answer(Response.json({ error: 'one burst every three seconds' }, { status: 429 }))
    render(<FluidPanel latest={null} />)
    fireEvent.click(screen.getByRole('button', { name: 'Fire 60 requests' }))
    await screen.findByText('one burst every three seconds')
    expect(steps).toEqual([])
  })

  it('hands a fault to the visit script once the controller has put it on the dimension', async () => {
    answer(Response.json(running.current, { status: 201 }))
    render(<ChaosPanel chaos={null} targets={['c-137']} admin={false} />)
    fireEvent.click(screen.getByRole('button', { name: 'Kill' }))
    await waitFor(() => expect(steps).toEqual(['broke-dimension']))
  })

  it('sends nothing for a refused fault', async () => {
    answer(Response.json({ error: 'a fault is already running' }, { status: 409 }))
    render(<ChaosPanel chaos={null} targets={['c-137']} admin={false} />)
    fireEvent.click(screen.getByRole('button', { name: 'Kill' }))
    await screen.findByText('a fault is already running')
    expect(steps).toEqual([])
  })

  it('sends nothing when the admin stops a fault', async () => {
    const fetch = answer(Response.json({ ...running, current: null }))
    render(<ChaosPanel chaos={running} targets={['c-137']} admin />)
    const stop = screen.getByRole('button', { name: 'Stop' })
    fireEvent.click(stop)
    await waitFor(() => expect(stop.hasAttribute('disabled')).toBe(false))
    expect(fetch).toHaveBeenCalledWith('/api/chaos', { method: 'DELETE' })
    expect(steps).toEqual([])
  })
})
