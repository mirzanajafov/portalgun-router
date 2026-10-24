import { REGISTRY_CELLS, registryTimes, registryTotals, us, type RegistrySummary } from '@/lib/measured'

function count(v: number): string {
  return v.toLocaleString('en-US')
}

function duration(ms: number): string {
  return us(ms * 1000)
}

export function RegistryResults({ run, sums }: { run: string; sums: RegistrySummary[] }) {
  const by = new Map(sums.map((s) => [s.cell, s]))
  const cells = REGISTRY_CELLS.flatMap((c) => {
    const sum = by.get(c.cell)
    return sum ? [{ ...c, sum }] : []
  })
  const times = registryTimes(sums)
  const total = registryTotals(sums)
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">Dimensions that come and go</h2>
      <p className="muted mb-3 text-sm">
        Dimensions register themselves with FiveFamilies, my Raft registry, and the controller turns its list into a config version. 500 rps across both
        gateways in every run, and {total.lost === 0 ? 'no request was lost' : `${count(total.lost)} requests were lost`}: {count(total.lost)} of{' '}
        {count(total.requests)} in {total.runs} runs. Run <span className="mono">{run}</span>.
      </p>
      {times.length > 0 && (
        <table className="mono mb-5 w-full min-w-[560px] text-sm">
          <thead className="muted text-left text-xs">
            <tr>
              <th className="pb-2 font-normal">step</th>
              <th className="pb-2 text-right font-normal">runs</th>
              <th className="pb-2 text-right font-normal">median</th>
              <th className="pb-2 text-right font-normal">fastest</th>
              <th className="pb-2 text-right font-normal">slowest</th>
            </tr>
          </thead>
          <tbody>
            {times.map((t) => (
              <tr key={t.label} className="border-t border-[var(--line)]">
                <td className="py-1.5 pr-3 font-sans">{t.label}</td>
                <td className="text-right">{t.runs}</td>
                <td className="text-right">{duration(t.time.median)}</td>
                <td className="text-right">{duration(t.time.min)}</td>
                <td className="text-right">{duration(t.time.max)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <table className="mono w-full min-w-[560px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">case</th>
            <th className="pb-2 text-right font-normal">runs</th>
            <th className="pb-2 text-right font-normal">lost</th>
            <th className="pb-2 text-right font-normal">config versions</th>
            <th className="pb-2 text-right font-normal">leases lost</th>
          </tr>
        </thead>
        <tbody>
          {cells.map(({ cell, label, sum }) => (
            <tr key={cell} className="border-t border-[var(--line)]">
              <td className="py-1.5 pr-3 font-sans">{label}</td>
              <td className="text-right">{sum.runs}</td>
              <td className="text-right">{count(sum.lost)}</td>
              <td className="text-right">{count(sum.registryVersions)}</td>
              <td className="text-right">{count(sum.lostRegistrations)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted mt-4 text-sm">
        A lost lease costs nothing here: the dimension registers again with the same address and version, the list doesn&apos;t change and no config version is
        written. That is also why the registry being down for 30 s writes none.
      </p>
    </section>
  )
}
