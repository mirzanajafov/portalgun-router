import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import Admin from './page'

const push = vi.hoisted(() => vi.fn())
vi.mock('next/navigation', () => ({ useRouter: () => ({ push, refresh: vi.fn() }) }))

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  push.mockReset()
})

async function signIn(answer: () => Promise<Response>) {
  vi.stubGlobal(
    'fetch',
    vi.fn((_url: string, init?: RequestInit) => (init?.method === 'POST' ? answer() : Promise.resolve(Response.json({ admin: false })))),
  )
  render(<Admin />)
  fireEvent.change(await screen.findByLabelText('Token'), { target: { value: 'dev-token' } })
  const button = screen.getByRole('button', { name: 'Sign in' })
  fireEvent.click(button)
  expect(button.hasAttribute('disabled')).toBe(true)
  return button
}

describe('admin sign-in', () => {
  it('lets the admin try again after a network error', async () => {
    const button = await signIn(() => Promise.reject(new TypeError('Failed to fetch')))
    await waitFor(() => expect(button.hasAttribute('disabled')).toBe(false))
    expect(screen.getByText('the server did not answer, so try again')).toBeTruthy()
  })

  it('shows why a token was refused', async () => {
    const button = await signIn(() => Promise.resolve(Response.json({ error: 'that token does not open the admin controls' }, { status: 401 })))
    await waitFor(() => expect(button.hasAttribute('disabled')).toBe(false))
    expect(screen.getByText('that token does not open the admin controls')).toBeTruthy()
  })

  it('goes back to the dashboard once signed in', async () => {
    await signIn(() => Promise.resolve(Response.json({ admin: true })))
    await waitFor(() => expect(push).toHaveBeenCalledWith('/'))
  })
})
