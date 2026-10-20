import { commonRate, overloadLabel, overloadSystems, PROXIES, proxyRows, rps, signed, sustained, us, type ProxyCell, type ProxyReport } from '@/lib/measured'

function OverloadCell({ c }: { c?: ProxyCell }) {
  if (!c?.goodputRps) return <td className="text-right">-</td>
  const collapsed = c.collapsed ?? 0
  const memory = c.memoryPeakMb ? `${Math.round(c.memoryPeakMb.max)} MB` : ''
  if (collapsed === c.runs) {
    return (
      <td className="py-1.5 text-right text-[var(--color-fault)]">
        collapsed
        <div className="muted text-xs">{c.swapPeakMb?.max ? `${memory} + swap` : memory}</div>
      </td>
    )
  }
  return (
    <td className="py-1.5 text-right">
      {rps(c.goodputRps.median)}
      <div className="muted text-xs">
        p99 {c.okP99Us ? us(c.okP99Us.median) : '-'} · {memory}
      </div>
      {collapsed > 0 && (
        <div className="text-xs text-[var(--color-fault)]">
          {collapsed} of {c.runs} collapsed
        </div>
      )}
    </td>
  )
}

function OverloadResults({ run, report }: { run: string; report: ProxyReport }) {
  const rates = [...new Set(report.cells.map((c) => c.rate))].sort((a, b) => a - b)
  const systems = overloadSystems(report)
  return (
    <>
      <h3 className="mt-5 mb-1 text-sm font-semibold">Past the knee</h3>
      <p className="muted mb-3 text-sm">
        Each point starts from a freshly restarted proxy and goes straight to the offered rate. Each cell shows the 2xx responses served per second, the p99 of
        those responses only, and the highest memory any repetition reached. Anything above the cap gets a 503 at once and isn&apos;t counted as served. A run
        counts as collapsed when more than 1% of its requests time out or never get sent. PortalGun&apos;s default cap is 256, the largest that never collapsed
        here. Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[720px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">offered</th>
            {rates.map((r) => (
              <th key={r} className="pb-2 text-right font-normal">
                {rps(r)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {systems.map((s) => (
            <tr key={s} className={`border-t border-[var(--line)] align-top ${s.startsWith('portalgun-cap') ? 'text-[var(--color-portal)]' : ''}`}>
              <td className="py-1.5 pr-3 font-sans">{overloadLabel(s)}</td>
              {rates.map((r) => (
                <OverloadCell key={r} c={report.cells.find((x) => x.system === s && x.rate === r)} />
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}

function StepResults({ run, report }: { run: string; report: ProxyReport }) {
  const rates = [...new Set(report.cells.map((c) => c.rate))].sort((a, b) => a - b)
  const systems = PROXIES.filter((p) => report.cells.some((c) => c.system === p.key))
  const held = systems.every((p) => rates.every((r) => sustained(report, p.key, r)))
  return (
    <>
      <h3 className="mt-5 mb-1 text-sm font-semibold">Near the knee, on a quieter night</h3>
      <p className="muted mb-3 text-sm">
        The same test again at {rates.map(rps).join(', ')} rps, with everything else on the laptop stopped.
        {held
          ? ' Every proxy held every rate in every repetition, so the gap in the max column above says more about the noisier evening of the sweep than about the proxies. What stays is the tail.'
          : ''}{' '}
        Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[560px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">rate</th>
            <th className="pb-2 font-normal">proxy</th>
            <th className="pb-2 text-right font-normal">p50</th>
            <th className="pb-2 text-right font-normal">p99</th>
            <th className="pb-2 text-right font-normal">CPU/request</th>
          </tr>
        </thead>
        <tbody>
          {rates.flatMap((rate) =>
            systems.map((p) => {
              const c = report.cells.find((x) => x.system === p.key && x.rate === rate)
              return (
                <tr key={`${rate}/${p.key}`} className={`border-t border-[var(--line)] ${p.key === 'portalgun' ? 'text-[var(--color-portal)]' : ''}`}>
                  <td className="py-1.5 pr-3">{rps(rate)}</td>
                  <td className="pr-3 font-sans">{p.label}</td>
                  <td className="text-right">{c ? us(c.p50Us.median) : '-'}</td>
                  <td className="text-right">{c ? us(c.p99Us.median) : '-'}</td>
                  <td className="text-right">{c ? us(c.cpuPerRequestUs.median) : '-'}</td>
                </tr>
              )
            }),
          )}
        </tbody>
      </table>
    </>
  )
}

export function ProxyResults({
  run,
  report,
  step,
  overload,
}: {
  run: string
  report: ProxyReport
  step?: { name: string; summary: ProxyReport } | null
  overload?: { name: string; summary: ProxyReport } | null
}) {
  const low = Math.min(...report.cells.map((c) => c.rate))
  const present = PROXIES.map((p) => p.key).filter((k) => report.cells.some((c) => c.system === k))
  const common = commonRate(report, present) ?? low
  const rows = proxyRows(report, low, common)
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">What a proxy in the path costs</h2>
      <p className="muted mb-3 text-sm">
        GET with a 1 KB response, open-loop load, every proxy pinned to one CPU with 256 MB. Added latency is the median over repetitions minus the same request
        with no proxy at all. CPU per request is the proxy container&apos;s own, at {rps(common)} rps, the highest rate every proxy held. Max is the highest
        offered rate at which most repetitions kept p99 under 50 ms, failures under 0.1% and throughput at 95% of the offered rate. Run{' '}
        <span className="mono">{run}</span>.
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
        throughput falls to a few hundred rps, or the kernel kills it. nginx and HAProxy stay far below the limit. PortalGun ran here without its cap on what it
        holds in total, so it fails the same way; the cap is measured below.
        {report.maxRate.direct
          ? ` The lab itself tops out near ${rps(report.maxRate.direct)} rps with no proxy at all, so the fastest maximums are partly the lab's.`
          : ''}
      </p>
      {step && <StepResults run={step.name} report={step.summary} />}
      {overload && <OverloadResults run={overload.name} report={overload.summary} />}
    </section>
  )
}
