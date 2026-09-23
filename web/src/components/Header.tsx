import type { ConnState } from '../useTelemetry'
import type { QueueState } from '../ReplayQueue'

type Props = {
  conn: ConnState
  sessionId: string | null
  partial: boolean
  lagSeconds: number | null
  queueDepth: number
  state: QueueState
  windowSeconds: number
  onWindowChange: (seconds: number) => void
}

const WINDOWS = [60, 300, 900]

export function Header(p: Props) {
  return (
    <header className="header">
      <h1>Pump 001 · fast polling</h1>
      <span className={`dot dot-${p.conn}`} title={p.conn} /> <span>{p.conn}</span>
      <span>session <code>{p.sessionId?.slice(0, 8) ?? '—'}</code>{p.partial && <em className="warn"> partial</em>}</span>
      <span>lag {p.lagSeconds === null ? '—' : `${p.lagSeconds.toFixed(1)} s`}</span>
      <span>queue {p.queueDepth}</span>
      {p.state === 'buffering' && <span className="badge">buffering</span>}
      <label>
        window{' '}
        <select value={p.windowSeconds} onChange={e => p.onWindowChange(Number(e.target.value))}>
          {WINDOWS.map(w => <option key={w} value={w}>{w / 60} min</option>)}
        </select>
      </label>
    </header>
  )
}
