import { pct } from '@/lib/format'
import { beforeAfter, limiterRows, signed, us, valid, type LimiterSummary } from '@/lib/measured'
import type { Run } from '@/lib/results'

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

function alone(key: string, s: LimiterSummary | undefined) {
  if (key === 'static-split' || !valid(s) || !s) return <span className="muted">-</span>
  return Math.round(s.degraded.median).toLocaleString('en-US')
}

const BREAKER: { load: string; label: string }[] = [
  { load: 'over-even-2gw', label: 'even' },
  { load: 'over-shift-2gw', label: 'shifting' },
  { load: 'over-shift-4gw', label: '4 gateways, shifting' },
  { load: 'zipf-10k-2gw', label: '10,000 keys' },
]

type Breaker = { before: Run<LimiterSummary[]>; after: Run<LimiterSummary[]> }

function BreakerResults({ before, after }: Breaker) {
  const rows = beforeAfter(
    before.summary,
    after.summary,
    BREAKER.map((b) => b.load),
  )
  if (rows.length === 0) return null
  return (
    <>
      <h3 className="mt-5 mb-1 text-sm font-semibold">The Redis breaker, before and after</h3>
      <p className="muted mb-3 text-sm">
        The rebuilt breaker opens only when at least half of the calls in the last 100 ms failed, and at least five, stays open for 100 ms, then lets one call
        through. What a gateway admitted on its own is charged back to the shared bucket once Redis answers again, unless the bucket expired meanwhile. Measured
        against the code before it on one evening, alternating, three runs each, at 1,500 rps. The 10,000 keys row is 3,000 rps over keys drawn from a Zipf
        distribution. Without Redis counts the decisions made on the static split at an even split, out of about 52,000. Runs{' '}
        <span className="mono">before/{before.name}</span> and <span className="mono">after/{after.name}</span>.
      </p>
      <table className="mono w-full min-w-[760px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">design</th>
            {BREAKER.map((b) => (
              <th key={b.load} className="pb-2 text-right font-normal">
                admitted, {b.label}
              </th>
            ))}
            <th className="pb-2 text-right font-normal">without Redis</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.key} className="border-t border-[var(--line)]">
              <td className="py-1.5 pr-3 font-sans">{r.label}</td>
              {r.after.map((a, i) => (
                <td key={i} className="text-right whitespace-nowrap">
                  {admitted(r.before[i])} → {admitted(a)}
                </td>
              ))}
              <td className="text-right whitespace-nowrap">
                {alone(r.key, r.before[0])} → {alone(r.key, r.after[0])}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}

export function LimiterResults({ run, sums, breaker }: { run: string; sums: LimiterSummary[]; breaker?: Breaker }) {
  const rows = limiterRows(sums, ['under-even-2gw', ...ACCURACY.map((a) => a.load)])
  const outage = limiterRows(sums, ['over-even-2gw-redis-slow', 'over-even-2gw-redis-down']).filter((r) => r.cells.some(valid))
  const exact = rows.find((r) => r.key === 'redis-exact')?.cells[1]
  const fellBack = valid(exact) && exact !== undefined && exact.degraded.median >= 1000
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">One limit across two gateways</h2>
      <p className="muted mb-3 text-sm">
        1,000 requests per second allowed for one key, split over two gateways that share Redis. Added latency and Redis trips are measured at 800 rps, under
        the limit, against the same load with no limiter. Differences under about 0.1 ms are noise on this laptop. Admitted is measured at 1,500 rps and
        compares what got through with a perfect single bucket replayed over the same requests: plus means over the limit, minus means good requests turned
        away. Shifting sends 90% of the traffic to one gateway and swaps every 5 s, and burst is 800 rps with spikes at 5,000, also shifting. Without Redis
        counts the decisions a gateway made on its own at 1,500 rps and an even split, out of about 52,000 per run. Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[800px] text-sm">
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
            <th className="pb-2 text-right font-normal">without Redis</th>
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
                <td className="text-right">{alone(r.key, accuracy[0])}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
      {fellBack && (
        <p className="muted mt-3 text-sm">
          In this run, Redis on every request and the token leases still had their first breaker: after three calls in a row missed their 2 ms deadline they
          stopped asking Redis for a second and decided on their own share of the limit. Here that happened often enough to undo the exact design: a gateway
          deciding alone stops spending from the shared bucket, so the other one gets all of it, and nothing was charged back afterwards. The local buckets only
          talk to Redis between requests and never had to.{breaker && ' The rebuilt breaker is measured below.'}
        </p>
      )}
      {breaker && <BreakerResults {...breaker} />}
      {outage.length > 0 && (
        <>
          <h3 className="mt-5 mb-1 text-sm font-semibold">When Redis is slow or gone</h3>
          <p className="muted mb-3 text-sm">
            1,500 rps with Redis 5 ms slower for the whole run, or stopped. Degraded counts the decisions a gateway made on its own because Redis did not answer
            in time.
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
