import { cookies } from 'next/headers'
import * as z from 'zod/mini'
import { ADMIN_COOKIE, adminToken, isAdmin } from '@/lib/server'
import type { Refusal, Session } from '@/lib/types'

export const dynamic = 'force-dynamic'

const SignIn = z.object({ token: z.string().check(z.minLength(1), z.maxLength(512)) })

export async function GET() {
  return Response.json({ admin: await isAdmin(await adminToken()) } satisfies Session)
}

export async function POST(req: Request) {
  const signIn = SignIn.safeParse(await req.json().catch(() => null))
  if (!signIn.success || !(await isAdmin(signIn.data.token))) {
    return Response.json({ error: 'that token does not open the admin controls' } satisfies Refusal, { status: 401 })
  }
  ;(await cookies()).set(ADMIN_COOKIE, signIn.data.token, {
    httpOnly: true,
    sameSite: 'strict',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: 8 * 3600,
  })
  return Response.json({ admin: true } satisfies Session)
}

export async function DELETE() {
  ;(await cookies()).delete(ADMIN_COOKIE)
  return Response.json({ admin: false } satisfies Session)
}
