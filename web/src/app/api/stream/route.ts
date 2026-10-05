import { CONTROLLER } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function GET(req: Request) {
  let upstream: Response
  try {
    upstream = await fetch(`${CONTROLLER}/api/events`, { cache: 'no-store', signal: req.signal })
  } catch {
    return new Response('the controller is unreachable', { status: 502 })
  }
  if (!upstream.ok || !upstream.body) {
    return new Response('the controller refused the stream', { status: 502 })
  }
  return new Response(upstream.body, {
    headers: { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no' },
  })
}
