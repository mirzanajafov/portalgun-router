import { relay, toController } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function POST(req: Request) {
  const body = await req.text()
  return relay(await toController('/api/rollouts/abort', { method: 'POST', body: body || '{}', admin: true, headers: { 'Content-Type': 'application/json' } }))
}
