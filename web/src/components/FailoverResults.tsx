import { DETECTION, failoverMatrix, valid, type FailoverSummary } from '@/lib/measured'
import { ms } from '@/lib/format'

const STUDIES: { cell: string; label: string }[] = [
  { cell: 'blackhole/both-retries-try100ms', label: 'Blackhole, 100 ms per try' },
  { cell: 'blackhole/both-retries-try250ms', label: 'Blackhole, 250 ms per try' },
  { cell: 'blackhole/both-retries', label: 'Blackhole, 1 s per try (default)' },
  { cell: 'crash/both-retries', label: 'Crash, default' },
  { cell: 'crash/both-retries-refused', label: 'Crash, eject on the first refused connection' },
  { cell: 'crash/both-retries-warm0s', label: 'Crash, back at full weight without slow start' },
  { cell: 'slow/both-retries-round-robin', label: '500 ms slower, round robin' },
  { cell: 'slow/both-retries-least-outstanding', label: '500 ms slower, least outstanding' },
  { cell: 'slow/both-retries', label: '500 ms slower, P2C with peak EWMA (default)' },
  { cell: 'partial-all/both-retries-panic0', label: '20% errors everywhere, no panic mode' },
  { cell: 'partial-all/both-retries', label: '20% errors everywhere, panic below 50% healthy' },
]

function count(v: number): string {
  return Math.round(v).toLocaleString('en-US')
}

function moved(s: FailoverSummary): string {
  if (s.moved === 0) return 'never'
  return s.moveMs.median > 0 ? ms(s.moveMs.median) : '0 ms'
}

export function FailoverResults({ run, sums }: { run: string; sums: FailoverSummary[] }) {
  const matrix = failoverMatrix(sums)
  const by = new Map(sums.map((s) => [s.cell, s]))
  const studies = STUDIES.flatMap((s) => {
    const sum = by.get(s.cell)
    return valid(sum) && sum ? [{ ...s, sum }] : []
  })
  const quiet = by.get('none/both-retries')
  return (
    <section className="panel overflow-x-auto p-4">
      <h2 className="mb-1 font-semibold">When a dimension breaks</h2>
      <p className="muted mb-3 text-sm">
        2,000 rps spread over three dimensions, one of them broken for 30 s. Each cell is the median number of requests that failed in those 30 s, and how long
        until less than 1% of the traffic still touched the broken one (100 ms bins). Probes are active health checks, passive means ejecting on observed errors,
        retries go to another dimension. Run <span className="mono">{run}</span>.
      </p>
      <table className="mono w-full min-w-[640px] text-sm">
        <thead className="muted text-left text-xs">
          <tr>
            <th className="pb-2 font-normal">fault</th>
            {DETECTION.map((d) => (
              <th key={d.key} className="pb-2 text-right font-normal">
                {d.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {matrix.map((r) => (
            <tr key={r.key} className="border-t border-[var(--line)]">
              <td className="py-1.5 pr-3 font-sans">{r.label}</td>
              {r.cells.map((c, i) => (
                <td key={i} className="py-1.5 text-right">
                  {valid(c) && c ? (
                    <>
                      {count(c.lost.median)} <span className="muted text-xs">{moved(c)}</span>
                    </>
                  ) : (
                    '-'
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {studies.length > 0 && (
        <>
          <h3 className="mt-5 mb-1 text-sm font-semibold">One setting at a time</h3>
          <p className="muted mb-3 text-sm">Probes, passive ejection and retries on, everything else at the default except the one thing named.</p>
          <table className="mono w-full min-w-[640px] text-sm">
            <thead className="muted text-left text-xs">
              <tr>
                <th className="pb-2 font-normal">case</th>
                <th className="pb-2 text-right font-normal">lost</th>
                <th className="pb-2 text-right font-normal">lost 10 s after</th>
                <th className="pb-2 text-right font-normal">p99 during</th>
                <th className="pb-2 text-right font-normal">moved</th>
              </tr>
            </thead>
            <tbody>
              {studies.map(({ cell, label, sum }) => (
                <tr key={cell} className="border-t border-[var(--line)]">
                  <td className="py-1.5 pr-3 font-sans">{label}</td>
                  <td className="text-right">{count(sum.lost.median)}</td>
                  <td className="text-right">{count(sum.lostAfter.median)}</td>
                  <td className="text-right">{ms(sum.p99FaultMs.median)}</td>
                  <td className="text-right">{moved(sum)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      {valid(quiet) && quiet && (
        <p className="muted mt-4 text-sm">
          Ten minutes with nothing broken but 1% errors and log-normal delays everywhere: {count(quiet.falseEjections.median)} false ejections (median of{' '}
          {quiet.runs - quiet.invalidRuns} runs, at most {count(quiet.falseEjections.max)}).
        </p>
      )}
    </section>
  )
}
