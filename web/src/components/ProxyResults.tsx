import { commonRate, PROXIES, proxyRows, rps, signed, us, type ProxyReport } from '@/lib/measured'

export function ProxyResults({ run, report }: { run: string; report: ProxyReport }) {
  const low = Math.min(...report.cells.map((c) => c.rate))
  const present = PROXIES.map((p) => p.key).filter((k) => report.cells.some((c) => c.system === k))
  const common = commonRate(report, present) ?? low
  const rows = proxyRows(report, low, common)
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">What a proxy in the path costs</h2>
      <p className="muted mb-3 text-sm">
        GET with a 1 KB response, open-loop load, every proxy pinned to one CPU with 256 MB. Added latency is the median over repetitions minus the same request with
        no proxy at all. CPU per request is the proxy container&apos;s own, at {rps(common)} rps, the highest rate every proxy held. Max is the highest offered rate
        at which most repetitions kept p99 under 50 ms, failures under 0.1% and throughput at 95% of the offered rate. Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[640px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">proxy</th>
            <th className="pb-2 text-right font-normal">+p50 at {rps(low)}</th>
            <th className="pb-2 text-right font-normal">+p99 at {rps(low)}</th>
            <th className="pb-2 text-right font-normal">CPU/request at {rps(common)}</th>
            <th className="pb-2 text-right font-normal">max rps</th>
            <th className="pb-2 text-right font-normal">memory at max</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.key} className={`border-t border-[var(--line)] ${r.key === 'portalgun' ? 'text-[var(--color-portal)]' : ''}`}>
              <td className="py-1.5 pr-3 font-sans">{r.label}</td>
              <td className="text-right">{r.low ? signed(us(r.low.overheadP50Us), r.low.overheadP50Us) : '-'}</td>
              <td className="text-right">{r.low ? signed(us(r.low.overheadP99Us), r.low.overheadP99Us) : '-'}</td>
              <td className="text-right">{r.common ? us(r.common.cpuPerRequestUs.median) : '-'}</td>
              <td className="text-right">{r.maxRate ? rps(r.maxRate) : '-'}</td>
              <td className="text-right">{r.top?.memoryPeakMb?.max ? `${Math.round(r.top.memoryPeakMb.max)} MB` : '-'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted mt-3 text-sm">
        Past their maximum, the Go proxies and Envoy keep accepting work: requests queue in memory until the container reaches 256 MB and starts swapping, and
        throughput falls to a few hundred rps, or the kernel kills it. nginx and HAProxy stay far below the limit. PortalGun has no cap on what it holds in total
        yet, so it fails the same way.
        {report.maxRate.direct ? ` The lab itself tops out near ${rps(report.maxRate.direct)} rps with no proxy at all, so the fastest maximums are partly the lab's.` : ''}
      </p>
    </section>
  )
}
