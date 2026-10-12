import { CanarySim } from '@/components/CanarySim'
import { FailoverResults } from '@/components/FailoverResults'
import { LimiterResults } from '@/components/LimiterResults'
import { ProxyResults } from '@/components/ProxyResults'
import type { FailoverSummary, LimiterSummary, ProxyReport } from '@/lib/measured'
import { latestRun, latestSim } from '@/lib/results'

export default function Results() {
  const proxy = latestRun<ProxyReport>('e1/w1')
  const step = latestRun<ProxyReport>('e1-step/w1')
  const overload = latestRun<ProxyReport>('e1-overload/w1')
  const limiter = latestRun<LimiterSummary[]>('e2')
  const before = latestRun<LimiterSummary[]>('e2-breaker/before')
  const after = latestRun<LimiterSummary[]>('e2-breaker/after')
  const failover = latestRun<FailoverSummary[]>('e3')
  const sim = latestSim()
  if (!proxy && !limiter && !failover && !sim) {
    return <p className="muted mt-8">No results in this build.</p>
  }
  return (
    <div className="flex flex-col gap-6">
      <section className="flex max-w-3xl flex-col gap-2">
        <h1 className="text-xl font-semibold">Results</h1>
        <p className="muted text-sm">
          Measured on one laptop, a 16-vCPU WSL2 VM, with CPU limits on every container and the proxy comparison on separate CPU sets. At least
          three repetitions per cell, medians shown. A laptop is noisier than a server, so small differences here mean little. The dimensions are containers
          behind fault injection: the failures and latencies are real, the geography is not.
        </p>
      </section>
      {proxy && <ProxyResults run={proxy.name} report={proxy.summary} step={step} overload={overload} />}
      {limiter && <LimiterResults run={limiter.name} sums={limiter.summary} breaker={before && after ? { before, after } : undefined} />}
      {failover && <FailoverResults run={failover.name} sums={failover.summary} />}
      {sim && <CanarySim sim={sim} />}
    </div>
  )
}
