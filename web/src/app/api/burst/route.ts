import { cookies } from 'next/headers'
import { GATEWAY, VISITOR_COOKIE } from '@/lib/server'

export const dynamic = 'force-dynamic'

const SIZE = 60
const GAP_MS = 3000
const lastBurst = new Map<string, number>()

export async function POST() {
  const jar = await cookies()
  let visitor = jar.get(VISITOR_COOKIE)?.value
  if (!visitor || !/^[a-f0-9-]{36}$/.test(visitor)) {
    visitor = crypto.randomUUID()
    jar.set(VISITOR_COOKIE, visitor, { httpOnly: true, sameSite: 'lax', secure: process.env.NODE_ENV === 'production', path: '/', maxAge: 30 * 86400 })
  }
  const now = Date.now()
  const last = lastBurst.get(visitor) ?? 0
  if (now - last < GAP_MS) {
    return Response.json({ error: 'one burst every three seconds' }, { status: 429, headers: { 'Retry-After': '3' } })
  }
  lastBurst.set(visitor, now)
  if (lastBurst.size > 10000) {
    for (const [k, t] of lastBurst) if (now - t > GAP_MS) lastBurst.delete(k)
  }
  const began = performance.now()
  const results = await Promise.all(
    Array.from({ length: SIZE }, () =>
      fetch(`${GATEWAY}/fluid/burst`, { headers: { 'X-Visitor': visitor }, cache: 'no-store' })
        .then(async (r) => {
          await r.arrayBuffer()
          return r.status
        })
        .catch(() => 0),
    ),
  )
  const count = (pred: (s: number) => boolean) => results.filter(pred).length
  return Response.json({
    sent: SIZE,
    allowed: count((s) => s >= 200 && s < 300),
    limited: count((s) => s === 429),
    failed: count((s) => s === 0 || s >= 500),
    ms: Math.round(performance.now() - began),
  })
}
