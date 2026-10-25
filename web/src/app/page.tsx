import { Dashboard } from '@/components/Dashboard'
import { adminToken, isAdmin } from '@/lib/server'

export const dynamic = 'force-dynamic'

export default async function Home() {
  return <Dashboard admin={await isAdmin(await adminToken())} />
}
