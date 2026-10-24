import { afterEach, describe, expect, it, vi } from 'vitest'
import { forwarded } from './server'

vi.mock('server-only', () => ({}))

const visitor = new Request('http://web/api/chaos', { headers: { 'X-Forwarded-For': '203.0.113.7' } })

describe('forwarded', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    vi.restoreAllMocks()
  })

  it('vouches for the visitor address with the proxy token', () => {
    vi.stubEnv('PORTAL_PROXY_TOKEN', 'proxy-secret')
    expect(forwarded(visitor)).toEqual({ 'X-Forwarded-For': '203.0.113.7', 'X-Portal-Proxy-Token': 'proxy-secret' })
  })

  it('sends nothing without a token, since the controller would not believe the address, and says so once', async () => {
    vi.resetModules()
    const fresh = await import('./server')
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined)
    vi.stubEnv('PORTAL_PROXY_TOKEN', '')
    expect(fresh.forwarded(visitor)).toEqual({})
    expect(fresh.forwarded(visitor)).toEqual({})
    expect(warn).toHaveBeenCalledOnce()
  })

  it('sends nothing without an address', () => {
    vi.stubEnv('PORTAL_PROXY_TOKEN', 'proxy-secret')
    expect(forwarded(new Request('http://web/api/chaos'))).toEqual({})
  })
})
