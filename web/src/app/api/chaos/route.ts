import { relay, toController } from '@/lib/server'

export const dynamic = 'force-dynamic'

export async function GET() {
  return relay(await toController('/api/chaos'))
}

export async function POST(req: Request) {
  const body = await req.text()
  return relay(await toController('/api/chaos', { method: 'POST', body, admin: true, req, headers: { 'Content-Type': 'application/json' } }))
}

export async function DELETE(req: Request) {
  return relay(await toController('/api/chaos', { method: 'DELETE', admin: true, req }))
}
