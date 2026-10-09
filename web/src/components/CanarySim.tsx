import { pct } from '@/lib/format'
import { aaRange, type CellResult, type SimResults } from '@/lib/results'

const POLICIES: { key: string; label: string; note: string }[] = [
  { key: 'sprt+terminal', label: 'Sequential + terminal test', note: 'what the controller runs' },
  { key: 'sprt', label: 'Sequential test alone', note: 'promotes undecided steps' },
  { key: 'fixed-horizon', label: 'Fixed horizon', note: 'one exact test at the end of the step' },
  { key: 'peeking-z', label: 'z-test every second', note: 'nominally 5%' },
  { key: 'fixed-relative', label: 'Canary over 2x stable', note: 'trailing 60 s' },
  { key: 'fixed-absolute', label: 'Canary over a fixed rate', note: '2x the known normal rate' },
]

function describe(c: CellResult['cell']): string {
  const what = c.totalFailure ? 'fails every request' : c.canarySlow > 0 ? `${pct(c.canarySlow, 0)} slow` : `${c.rho}x errors`
  return `${what}, ${pct(c.baseError)} base, ${c.rps} rps, ${pct(c.share, 0)} canary`
}

export function CanarySim({ sim }: { sim: SimResults }) {
  const regressions = sim.cells.filter((c) => c.cell.group !== 'a/a')
  const shown = ['sprt+terminal', 'fixed-horizon', 'fixed-relative']
  return (
    <>
      <section className="flex max-w-3xl flex-col gap-2 pt-2">
        <h2 className="text-lg font-semibold">Canary decisions, simulated</h2>
        <p className="muted text-sm">
          72 cells, 2,000 trials for every cell where the canary is as good as the stable version and 500 for every regression, five-minute steps, the same random
          traffic for every rule within a trial. Run <span className="mono">{sim.name}</span>.
        </p>
      </section>
      <section className="panel overflow-x-auto p-4">
        <h2 className="mb-1 font-semibold">How often a good version gets rolled back</h2>
        <p className="muted mb-3 text-sm">Range over the 36 cells: base error 0.1 to 2%, canary share 1 to 25%, 200 and 2,000 rps, with and without a 30 s outage downstream that hits both versions.</p>
        <table className="mono w-full min-w-[520px] text-sm">
          <tbody>
            {POLICIES.map((p) => {
              const [lo, hi] = aaRange(sim.cells, p.key)
              return (
                <tr key={p.key} className="border-t border-[var(--line)]">
                  <td className="py-1.5 pr-4 font-sans">
                    {p.label} <span className="muted text-xs">{p.note}</span>
                  </td>
                  <td className={`text-right ${hi > 0.02 ? 'text-[var(--color-fault)]' : 'text-[var(--color-portal)]'}`}>
                    {pct(lo)} to {pct(hi)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </section>
      <section className="panel overflow-x-auto p-4">
        <h2 className="mb-1 font-semibold">How fast a bad version goes, and how often it is caught</h2>
        <p className="muted mb-3 text-sm">
          Median seconds to roll back, share of trials rolled back in brackets. The fixed-horizon and threshold rules look at errors only, so their numbers on the
          latency rows are the false rollbacks they would have made anyway.
        </p>
        <table className="mono w-full min-w-[640px] text-sm">
          <thead className="muted text-left text-xs">
            <tr>
              <th className="pb-2 font-normal">canary</th>
              {shown.map((s) => (
                <th key={s} className="pb-2 text-right font-normal">
                  {POLICIES.find((p) => p.key === s)?.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {regressions.map((c, i) => (
              <tr key={i} className="border-t border-[var(--line)]">
                <td className="py-1 pr-3 font-sans">{describe(c.cell)}</td>
                {shown.map((s) => {
                  const p = c.policies.find((x) => x.policy === s)
                  return (
                    <td key={s} className="text-right">
                      {p && p.rollbacks > 0 ? `${Math.round(p.medianRollbackSec)} s` : '-'} <span className="muted">({pct(p?.rollbackRate ?? 0)})</span>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </>
  )
}
