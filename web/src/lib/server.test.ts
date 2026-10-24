import { afterEach, describe, expect, it, vi } from 'vitest'

vi.mock('server-only', () => ({}))
vi.mock('next/headers', () => ({ cookies: () => Promise.resolve({ get: () => undefined }) }))

const { forwarded } = await import('./server')

const visitor = new Request('http://web/api/chaos', { headers: { 'X-Forwarded-For': '203.0.113.7' } })

describe('forwarded', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('vouches for the visitor address with the proxy token', () => {
    vi.stubEnv('PORTAL_PROXY_TOKEN', 'proxy-secret')
    expect(forwarded(visitor)).toEqual({ 'X-Forwarded-For': '203.0.113.7', 'X-Portal-Proxy-Token': 'proxy-secret' })
  })

  it('passes the address on without vouching when no token is set', () => {
    vi.stubEnv('PORTAL_PROXY_TOKEN', '')
    expect(forwarded(visitor)).toEqual({ 'X-Forwarded-For': '203.0.113.7' })
  })

  it('sends nothing without an address', () => {
    vi.stubEnv('PORTAL_PROXY_TOKEN', 'proxy-secret')
    expect(forwarded(new Request('http://web/api/chaos'))).toEqual({})
  })
})
