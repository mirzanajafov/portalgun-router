import { CanarySim } from '@/components/CanarySim'
import { FailoverResults } from '@/components/FailoverResults'
import { LimiterResults } from '@/components/LimiterResults'
import { ProxyResults } from '@/components/ProxyResults'
import { RegistryResults } from '@/components/RegistryResults'
import { FailoverSummaries, LimiterSummaries, ProxyReport, RegistrySummaries } from '@/lib/measured'
import { latestRun, latestSim } from '@/lib/results'

export default function Results() {
  const proxy = latestRun('e1/w1', ProxyReport)
  const step = latestRun('e1-step/w1', ProxyReport)
  const overload = latestRun('e1-overload/w1', ProxyReport)
  const limiter = latestRun('e2', LimiterSummaries)
  const before = latestRun('e2-breaker/before', LimiterSummaries)
  const after = latestRun('e2-breaker/after', LimiterSummaries)
  const failover = latestRun('e3', FailoverSummaries)
  const registry = latestRun('e5', RegistrySummaries)
  const sim = latestSim()
  if (!proxy && !limiter && !failover && !registry && !sim) {
    return <p className="muted mt-8">No results in this build.</p>
  }
  return (
    <div className="flex flex-col gap-6">
      <section className="flex max-w-3xl flex-col gap-2">
        <h1 className="text-xl font-semibold">Results</h1>
        <p className="muted text-sm">
          Measured on one laptop, a 16-vCPU WSL2 VM, with CPU limits on every container (except Redis and Postgres in the registry run) and the proxy comparison
          on separate CPU sets. At least three repetitions per cell, medians shown unless a table says otherwise. A laptop is noisier than a server, so small
          differences here mean little. The dimensions are containers behind fault injection: the failures and latencies are real, the geography is not.
        </p>
      </section>
      {proxy && <ProxyResults run={proxy.name} report={proxy.summary} step={step} overload={overload} />}
      {limiter && <LimiterResults run={limiter.name} sums={limiter.summary} breaker={before && after ? { before, after } : undefined} />}
      {failover && <FailoverResults run={failover.name} sums={failover.summary} />}
      {sim && <CanarySim sim={sim} />}
      {registry && <RegistryResults run={registry.name} sums={registry.summary} />}
    </div>
  )
}
