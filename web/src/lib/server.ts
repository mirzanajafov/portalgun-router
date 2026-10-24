import 'server-only'
import { cookies } from 'next/headers'

export const CONTROLLER = process.env.CONTROLLER_URL ?? 'http://127.0.0.1:3121'
export const GATEWAY = process.env.GATEWAY_URL ?? 'http://127.0.0.1:3122'
export const ADMIN_COOKIE = 'portal_admin'
export const VISITOR_COOKIE = 'portal_visitor'

export async function adminToken(): Promise<string | undefined> {
  return (await cookies()).get(ADMIN_COOKIE)?.value
}

export function forwarded(req: Request): Record<string, string> {
  const xff = req.headers.get('x-forwarded-for')
  const token = process.env.PORTAL_PROXY_TOKEN
  return xff && token ? { 'X-Forwarded-For': xff, 'X-Portal-Proxy-Token': token } : {}
}

export async function toController(path: string, init: RequestInit & { admin?: boolean; req?: Request } = {}): Promise<Response> {
  const headers = new Headers(init.headers)
  if (init.admin) {
    const token = await adminToken()
    if (token) headers.set('Authorization', `Bearer ${token}`)
  }
  if (init.req) {
    for (const [k, v] of Object.entries(forwarded(init.req))) headers.set(k, v)
  }
  const { admin: _admin, req: _req, ...rest } = init
  return fetch(`${CONTROLLER}${path}`, { ...rest, headers, cache: 'no-store' })
}

export async function relay(res: Response): Promise<Response> {
  const body = await res.text()
  const headers = new Headers({ 'Content-Type': res.headers.get('Content-Type') ?? 'application/json' })
  const retry = res.headers.get('Retry-After')
  if (retry) headers.set('Retry-After', retry)
  return new Response(body, { status: res.status, headers })
}
