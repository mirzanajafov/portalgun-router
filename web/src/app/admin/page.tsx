'use client'

import { useRouter } from 'next/navigation'
import { useEffect, useState } from 'react'

export default function Admin() {
  const router = useRouter()
  const [token, setToken] = useState('')
  const [admin, setAdmin] = useState<boolean | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    fetch('/api/session')
      .then((r) => r.json())
      .then((s) => setAdmin(!!s.admin))
      .catch(() => setAdmin(false))
  }, [])

  async function signIn(e: React.FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError('')
    const res = await fetch('/api/session', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }) }).catch(
      () => null,
    )
    if (!res) {
      setError('the server did not answer, so try again')
      setBusy(false)
      return
    }
    if (!res.ok) {
      const body = await res.json().catch(() => ({}))
      setError(body.error ?? 'that did not work')
      setBusy(false)
      return
    }
    router.push('/')
    router.refresh()
  }

  async function signOut() {
    await fetch('/api/session', { method: 'DELETE' })
    setAdmin(false)
    router.refresh()
  }

  return (
    <section className="panel mx-auto mt-8 flex max-w-md flex-col gap-4 p-6">
      <h1 className="text-lg font-semibold">Admin controls</h1>
      {admin ? (
        <>
          <p className="text-sm">You can start and abort rollouts and break things without limits.</p>
          <button className="btn self-start" onClick={signOut}>
            Sign out
          </button>
        </>
      ) : (
        <form onSubmit={signIn} className="flex flex-col gap-3">
          <p className="muted text-sm">
            Visitors can already break dimensions and start and break canaries, within limits. The admin token lifts the limits and lets you abort a rollout.
          </p>
          <label className="flex flex-col gap-1 text-sm">
            Token
            <input
              type="password"
              autoComplete="off"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              className="mono rounded-lg border border-[var(--line)] bg-[var(--bg)] px-3 py-2"
            />
          </label>
          {error && <p className="text-sm text-[var(--color-fault)]">{error}</p>}
          <button className="btn btn-primary self-start" disabled={busy || !token}>
            Sign in
          </button>
        </form>
      )}
    </section>
  )
}
