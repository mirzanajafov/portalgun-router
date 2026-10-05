import { relay, toController } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function GET() {
  return relay(await toController('/api/rollouts'))
}

export async function POST(req: Request) {
  const body = await req.text()
  return relay(await toController('/api/rollouts', { method: 'POST', body, admin: true, headers: { 'Content-Type': 'application/json' } }))
}
