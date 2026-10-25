'use client'

import { useState } from 'react'
import { Refusal } from './types'

export function postJSON(body: unknown): RequestInit {
  return { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
}

async function refusal(res: Response): Promise<string> {
  const parsed = Refusal.safeParse(await res.json().catch(() => null))
  return parsed.success ? parsed.data.error : `refused (${res.status})`
}

export function useAction() {
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')

  async function run(url: string, init: RequestInit, done?: (res: Response) => Promise<void> | void) {
    setBusy(true)
    setMessage('')
    try {
      const res = await fetch(url, init).catch(() => null)
      if (!res) setMessage('the server did not answer, so try again')
      else if (!res.ok) setMessage(await refusal(res))
      else await done?.(res)
    } catch {
      setMessage('the server sent an answer this page cannot read')
    } finally {
      setBusy(false)
    }
  }

  return { busy, message, send: (...args: Parameters<typeof run>) => void run(...args) }
}
