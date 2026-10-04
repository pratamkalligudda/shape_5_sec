import { useEffect, useMemo, useState } from 'react';
import { backend } from '../api/backend';
import type { FieldMap, FieldValue, SeriesResponse } from '../contract/types';
import type { Timeline } from '../live/frames';
import { formatValue, unitFor } from '../live/units';
import { clockTime } from '../live/view';

const STATUS = ['Normal', 'Warning', 'Alarm'];

interface Props {
  latest: FieldMap;
  timeline: Timeline;
  sessionId: string | null;
}

/** Every snapshot field; selecting one loads GET /series?metric=X and continues it from the live frames. */
export function LiveMetrics({ latest, timeline, sessionId }: Props) {
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<string | null>(null);
  const [seed, setSeed] = useState<SeriesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSeed(null);
    setError(null);
    if (!selected || !sessionId) return;
    void backend.series([selected]).then(r => (r.ok ? setSeed(r.data) : setError(`${r.code}: ${r.message}`)));
  }, [selected, sessionId]);

  const fields = Object.keys(latest).filter(k => k.toLowerCase().includes(query.toLowerCase())).sort();

  const rows = useMemo(() => {
    if (!selected || !seed) return [];
    const seeded: { t: number; v: FieldValue; status: number }[] =
      seed.t.map((t, k) => ({ t: Date.parse(t), v: seed.series[0]?.v[k] ?? null, status: seed.status[k] ?? 0 }));
    const last = seeded.length > 0 ? seeded[seeded.length - 1].t : -Infinity;
    const live = timeline.frames.filter(f => f.t > last && selected in f.values).map(f => ({ t: f.t, v: f.values[selected], status: f.status }));
    return [...seeded, ...live].reverse().slice(0, 60);
  }, [selected, seed, timeline.frames]);

  return (
    <section className="card">
      <div className="card-head">
        <h2>Live Metrics</h2>
        <input placeholder="Search fields…" value={query} onChange={e => setQuery(e.target.value)} />
      </div>
      <div className="metrics">
        <ul className="field-list">
          {fields.map(k => (
            <li key={k} className={k === selected ? 'selected' : ''} onClick={() => setSelected(k)}>
              <span>{k}</span>
              <span className="muted">{formatValue(k, latest[k])}</span>
            </li>
          ))}
        </ul>
        <div className="metric-table">
          {selected === null ? (
            <p className="muted">Select a field to load its series.</p>
          ) : error ? (
            <p className="error">{error}</p>
          ) : (
            <table>
              <thead><tr><th>Time</th><th>{selected} {unitFor(selected) && `(${unitFor(selected)})`}</th><th>Status</th></tr></thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.t}><td>{clockTime(r.t)}</td><td>{formatValue(selected, r.v)}</td><td>{STATUS[r.status] ?? r.status}</td></tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </section>
  );
}
