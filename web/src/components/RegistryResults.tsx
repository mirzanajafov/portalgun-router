import { count, ms } from '@/lib/format'
import { registryCases, registryTimes, registryTotals, type RegistrySummary } from '@/lib/measured'

export function RegistryResults({ run, sums }: { run: string; sums: RegistrySummary[] }) {
  const cases = registryCases(sums)
  const times = registryTimes(sums)
  const total = registryTotals(cases)
  if (cases.length === 0) return null
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">Dimensions that come and go</h2>
      <p className="muted mb-3 text-sm">
        Dimensions register themselves with FiveFamilies, a registry on Raft, and the controller turns its list into a config version. This run has a stack of
        its own without fault injection, 500 rps across both gateways and two retries on the route: {count(total.lost)} of {count(total.sent)} requests were
        lost in {count(total.runs)} runs{total.failed > 0 && `, ${count(total.failed)} of those runs failed or invalid`}. Run{' '}
        <span className="mono">{run}</span>.
      </p>
      {times.length > 0 && (
        <table className="mono mb-5 w-full min-w-[560px] text-sm">
          <thead className="muted text-left text-xs">
            <tr>
              <th className="pb-2 font-normal">step</th>
              <th className="pb-2 text-right font-normal">median</th>
              <th className="pb-2 text-right font-normal">fastest</th>
              <th className="pb-2 text-right font-normal">slowest</th>
            </tr>
          </thead>
          <tbody>
            {times.map((t) => (
              <tr key={t.label} className="border-t border-[var(--line)]">
                <td className="py-1.5 pr-3 font-sans">{t.label}</td>
                <td className="text-right">{ms(t.time.median)}</td>
                <td className="text-right">{ms(t.time.min)}</td>
                <td className="text-right">{ms(t.time.max)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <table className="mono w-full min-w-[560px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">case, totals over its runs</th>
            <th className="pb-2 text-right font-normal">runs</th>
            <th className="pb-2 text-right font-normal">lost</th>
            <th className="pb-2 text-right font-normal">config versions</th>
            <th className="pb-2 text-right font-normal">leases lost</th>
          </tr>
        </thead>
        <tbody>
          {cases.map(({ key, label, sum }) => (
            <tr key={key} className="border-t border-[var(--line)]">
              <td className="py-1.5 pr-3 font-sans">{label}</td>
              <td className="text-right">
                {sum.runs}
                {sum.failed + sum.invalid > 0 && <span className="muted text-xs"> {sum.failed + sum.invalid} failed</span>}
              </td>
              <td className="text-right">{count(sum.lost)}</td>
              <td className="text-right">{count(sum.registryVersions)}</td>
              <td className="text-right">{count(sum.lostRegistrations)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted mt-4 text-sm">
        A lost lease costs nothing here: the dimension registers again with the same address and version, the list doesn&apos;t change and no config version is
        written.
      </p>
    </section>
  )
}
