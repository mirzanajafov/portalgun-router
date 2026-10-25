import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useAction } from './action'
import { Burst } from './types'

afterEach(() => {
  vi.unstubAllGlobals()
})

function answer(res: Response) {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.resolve(res)),
  )
  const { result } = renderHook(() => useAction())
  act(() => {
    result.current.send('/api/burst', { method: 'POST' }, async (r) => {
      Burst.parse(await r.json())
    })
  })
  return result
}

describe('actions', () => {
  it('shows the refusal the server gave', async () => {
    const result = answer(Response.json({ error: 'one burst every three seconds' }, { status: 429 }))
    await waitFor(() => expect(result.current.message).toBe('one burst every three seconds'))
    expect(result.current.busy).toBe(false)
  })

  it('falls back to the status when a refusal has no reason', async () => {
    const result = answer(new Response('<html>', { status: 502 }))
    await waitFor(() => expect(result.current.message).toBe('refused (502)'))
  })

  it('says so when an answer cannot be read', async () => {
    const result = answer(new Response('<html>'))
    await waitFor(() => expect(result.current.message).toBe('the server sent an answer this page cannot read'))
    expect(result.current.busy).toBe(false)
  })
})
