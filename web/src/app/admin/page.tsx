'use client'

import { useRouter } from 'next/navigation'
import { useEffect, useState } from 'react'
import { postJSON, useAction } from '@/lib/action'
import { Session } from '@/lib/types'

export default function Admin() {
  const router = useRouter()
  const [token, setToken] = useState('')
  const [admin, setAdmin] = useState<boolean | null>(null)
  const { busy, message, send } = useAction()

  useEffect(() => {
    void fetch('/api/session')
      .then(async (r) => Session.parse(await r.json()).admin)
      .catch(() => false)
      .then(setAdmin)
  }, [])

  return (
    <section className="panel mx-auto mt-8 flex max-w-md flex-col gap-4 p-6">
      <h1 className="text-lg font-semibold">Admin controls</h1>
      {admin ? (
        <>
          <p className="text-sm">You can start and abort rollouts and break things without limits.</p>
          <button
            className="btn self-start"
            disabled={busy}
            onClick={() =>
              send('/api/session', { method: 'DELETE' }, () => {
                setAdmin(false)
                router.refresh()
              })
            }
          >
            Sign out
          </button>
        </>
      ) : (
        <form
          onSubmit={(e) => {
            e.preventDefault()
            send('/api/session', postJSON({ token }), () => {
              router.push('/')
              router.refresh()
            })
          }}
          className="flex flex-col gap-3"
        >
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
          <button className="btn btn-primary self-start" disabled={busy || !token}>
            Sign in
          </button>
        </form>
      )}
      {message && <p className="text-sm text-[var(--color-fault)]">{message}</p>}
    </section>
  )
}
