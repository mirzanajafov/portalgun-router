import type { PortalEvent } from '@/lib/types'

export function worthShowing(e: PortalEvent): boolean {
  return !(e.type === 'config' && e.message.includes(' by controller:'))
}

export function EventLog({ events }: { events: PortalEvent[] }) {
  const shown = [...events].reverse().filter(worthShowing).slice(0, 10)
  return (
    <section className="panel p-4">
      <h2 className="mb-2 font-semibold">What happened</h2>
      {shown.length === 0 ? (
        <p className="muted text-sm">Quiet so far.</p>
      ) : (
        <ul className="flex flex-col gap-1 text-sm">
          {shown.map((e) => (
            <li key={`${e.at}-${e.message}`} className="flex gap-3">
              <span className="mono muted shrink-0">{new Date(e.at).toLocaleTimeString([], { hour12: false })}</span>
              <span className="min-w-0 break-words">{e.message}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
