import { pct } from '@/lib/format'
import { limiterRows, signed, us, valid, type LimiterSummary } from '@/lib/measured'

const ACCURACY: { load: string; label: string }[] = [
  { load: 'over-even-2gw', label: 'even' },
  { load: 'over-shift-2gw', label: 'shifting' },
  { load: 'burst-shift-2gw', label: 'burst' },
]

function admitted(s: LimiterSummary | undefined) {
  if (!valid(s) || !s) return <span className="muted">-</span>
  const v = Math.abs(s.overAdmission.median) < 0.0005 ? 0 : s.overAdmission.median
  const tone = v > 0.01 ? 'text-[var(--color-fault)]' : v < -0.05 ? 'text-[var(--color-warn)]' : ''
  return <span className={tone}>{signed(pct(v), v)}</span>
}

export function LimiterResults({ run, sums }: { run: string; sums: LimiterSummary[] }) {
  const rows = limiterRows(sums, ['under-even-2gw', ...ACCURACY.map((a) => a.load)])
  const outage = limiterRows(sums, ['over-even-2gw-redis-slow', 'over-even-2gw-redis-down']).filter((r) => r.cells.some(valid))
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">One limit across two gateways</h2>
      <p className="muted mb-3 text-sm">
        1,000 requests per second allowed for one key, split over two gateways that share Redis. Added latency and Redis trips are measured at 800 rps, under the
        limit, against the same load with no limiter; differences under about 0.1 ms are noise on this laptop. Admitted is measured at 1,500 rps and compares what
        got through with a perfect single bucket replayed over the same requests: plus means over the limit, minus means good requests turned away. Shifting
        sends 90% of the traffic to one gateway and swaps every 5 s; burst is 800 rps with spikes at 5,000, also shifting. Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[720px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">design</th>
            <th className="pb-2 text-right font-normal">+p50</th>
            <th className="pb-2 text-right font-normal">+p99</th>
            <th className="pb-2 text-right font-normal">Redis trips/request</th>
            {ACCURACY.map((a) => (
              <th key={a.load} className="pb-2 text-right font-normal">
                admitted, {a.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const [base, ...accuracy] = r.cells
            const ok = valid(base) && base
            return (
              <tr key={r.key} className="border-t border-[var(--line)]">
                <td className="py-1.5 pr-3 font-sans">{r.label}</td>
                <td className="text-right">{ok && ok.addedP50Us ? signed(us(ok.addedP50Us.median), ok.addedP50Us.median) : '-'}</td>
                <td className="text-right">{ok && ok.addedP99Us ? signed(us(ok.addedP99Us.median), ok.addedP99Us.median) : '-'}</td>
                <td className="text-right">{ok ? ok.redisRoundTripsPerRequest.median.toFixed(2) : '-'}</td>
                {accuracy.map((c, i) => (
                  <td key={i} className="text-right">
                    {admitted(c)}
                  </td>
                ))}
              </tr>
            )
          })}
        </tbody>
      </table>
      {outage.length > 0 && (
        <>
          <h3 className="mt-5 mb-1 text-sm font-semibold">When Redis is slow or gone</h3>
          <p className="muted mb-3 text-sm">
            1,500 rps with Redis 5 ms slower for the whole run, or stopped. Degraded counts the decisions a gateway made on its own because Redis did not answer in
            time.
          </p>
          <table className="mono w-full min-w-[560px] text-sm">
            <thead className="muted text-left text-xs">
              <tr>
                <th className="pb-2 font-normal">design</th>
                <th className="pb-2 text-right font-normal">p50, Redis slow</th>
                <th className="pb-2 text-right font-normal">admitted, Redis down</th>
                <th className="pb-2 text-right font-normal">degraded, Redis down</th>
              </tr>
            </thead>
            <tbody>
              {outage.map((r) => {
                const [slow, down] = r.cells
                return (
                  <tr key={r.key} className="border-t border-[var(--line)]">
                    <td className="py-1.5 pr-3 font-sans">{r.label}</td>
                    <td className="text-right">{valid(slow) && slow ? us(slow.p50Us.median) : '-'}</td>
                    <td className="text-right">{admitted(down)}</td>
                    <td className="text-right">{valid(down) && down ? Math.round(down.degraded.median).toLocaleString('en-US') : '-'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </>
      )}
    </section>
  )
}
