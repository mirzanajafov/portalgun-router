import { cookies } from 'next/headers'
import { z } from 'zod'
import { ADMIN_COOKIE, adminToken, isAdmin } from '@/lib/server'

export const dynamic = 'force-dynamic'

const SignIn = z.object({ token: z.string().min(1).max(512) })

export async function GET() {
  return Response.json({ admin: await isAdmin(await adminToken()) })
}

export async function POST(req: Request) {
  const signIn = SignIn.safeParse(await req.json().catch(() => null))
  if (!signIn.success || !(await isAdmin(signIn.data.token))) {
    return Response.json({ error: 'that token does not open the admin controls' }, { status: 401 })
  }
  ;(await cookies()).set(ADMIN_COOKIE, signIn.data.token, {
    httpOnly: true,
    sameSite: 'strict',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: 8 * 3600,
  })
  return Response.json({ admin: true })
}

export async function DELETE() {
  ;(await cookies()).delete(ADMIN_COOKIE)
  return Response.json({ admin: false })
}
