import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { Row } from '../rows'

export type Series = { key: string; name: string; color: string; axis?: 'left' | 'right' }

type Props = {
  title: string
  rows: Row[]
  series: Series[]
  domain: [number, number]
  sessionStarts: number[]
  refLines?: { y: number; label: string }[]
}

const time = (t: number) => new Date(t).toLocaleTimeString()

export function TrendChart({ title, rows, series, domain, sessionStarts, refLines = [] }: Props) {
  const hasRight = series.some(s => s.axis === 'right')
  return (
    <section className="card">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
          <XAxis dataKey="t" type="number" domain={domain} tickFormatter={time} allowDataOverflow />
          <YAxis yAxisId="left" domain={['auto', 'auto']} width={60} />
          {hasRight && <YAxis yAxisId="right" orientation="right" domain={['auto', 'auto']} width={40} />}
          <Tooltip labelFormatter={v => time(Number(v))} />
          <Legend />
          {series.map(s => (
            <Line key={s.key} yAxisId={s.axis ?? 'left'} dataKey={s.key} name={s.name} stroke={s.color}
              dot={false} isAnimationActive={false} connectNulls={false} />
          ))}
          {refLines.map(r => (
            <ReferenceLine key={r.label} yAxisId="left" y={r.y} ifOverflow="extendDomain" stroke="var(--muted)" strokeDasharray="6 4"
              label={{ value: r.label, position: 'insideTopRight', fill: 'var(--muted)', fontSize: 11 }} />
          ))}
          {sessionStarts.map(t => (
            <ReferenceLine key={t} yAxisId="left" x={t} stroke="var(--accent)" strokeDasharray="3 3"
              label={{ value: 'session start', position: 'insideTopLeft', fill: 'var(--accent)', fontSize: 11 }} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </section>
  )
}
