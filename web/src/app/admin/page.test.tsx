import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import Admin from './page'

vi.mock('next/navigation', () => ({ useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }) }))

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe('admin sign-in', () => {
  it('lets the admin try again after a network error', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn((_url: string, init?: RequestInit) => (init?.method === 'POST' ? Promise.reject(new TypeError('Failed to fetch')) : Promise.resolve(Response.json({ admin: false })))),
    )
    render(<Admin />)
    fireEvent.change(await screen.findByLabelText('Token'), { target: { value: 'dev-token' } })
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Sign in' }).hasAttribute('disabled')).toBe(false))
    expect(screen.getByText('the server did not answer, so try again')).toBeTruthy()
  })
})
