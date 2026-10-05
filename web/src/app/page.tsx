import { Dashboard } from '@/components/Dashboard'
import { CONTROLLER, adminToken } from '@/lib/server'

export const dynamic = 'force-dynamic'

async function isAdmin(): Promise<boolean> {
  const token = await adminToken()
  if (!token) return false
  try {
    const res = await fetch(`${CONTROLLER}/api/admin`, { headers: { Authorization: `Bearer ${token}` }, cache: 'no-store' })
    return res.ok
  } catch {
    return false
  }
}

export default async function Home() {
  return <Dashboard admin={await isAdmin()} />
}
