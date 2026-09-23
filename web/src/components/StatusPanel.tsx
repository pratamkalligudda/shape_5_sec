import type { FoldedSample } from '../types'

const COUNTERS = ['StrokeCounterTW1', 'OperatingHoursMotor', 'OperatingHoursTW1']
const isFlag = (k: string) => k.includes('Alarm') || k.includes('Warning') || k === 'BoosterPressureFault'
const active = (v: unknown) => v === true || (typeof v === 'number' && v !== 0)

export function StatusPanel({ latest }: { latest: FoldedSample | null }) {
  if (!latest) return <section className="card"><h2>Status</h2><p className="muted">waiting for data…</p></section>
  const changed = new Set(latest.changed)
  const flash = (k: string) => (changed.has(k) ? 'flash' : '')
  const flags = Object.keys(latest.values).filter(isFlag)
  return (
    <section className="card">
      <h2>Status</h2>
      <div className="chips">
        <span key={`MotorRunning-${changed.has('MotorRunning') ? latest.ts : ''}`}
          className={`chip ${active(latest.values.MotorRunning) ? 'ok' : 'off'} ${flash('MotorRunning')}`}>MotorRunning</span>
        {flags.map(k => {
          const level = active(latest.values[k]) ? (k.includes('Warning') ? 'warn' : 'alarm') : 'off'
          return <span key={`${k}-${changed.has(k) ? latest.ts : ''}`} className={`chip ${level} ${flash(k)}`}>{k}</span>
        })}
      </div>
      <dl className="counters">
        {COUNTERS.map(k => (
          <div key={`${k}-${changed.has(k) ? latest.ts : ''}`} className={flash(k)}>
            <dt>{k}</dt><dd>{String(latest.values[k] ?? '—')}</dd>
          </div>
        ))}
      </dl>
    </section>
  )
}
