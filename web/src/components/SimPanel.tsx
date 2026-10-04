import { useEffect, useState } from 'react';
import { simulator, type SimState } from '../api/simulator';

const PRESETS: { label: string; values: Record<string, number> }[] = [
  { label: 'Emergency Stop (alarm 5)', values: { OperatingAlarmActive: 5 } },
  { label: 'Overstroke Alarm (6)', values: { OperatingAlarmActive: 6 } },
  { label: 'Clear operating alarm', values: { OperatingAlarmActive: 0 } },
  { label: 'Left Overstroke Warning (21)', values: { OperatingWarningActive: 21 } },
  { label: 'Clear operating warning', values: { OperatingWarningActive: 0 } },
  { label: 'Maintenance alarm', values: { MaintenanceAlarmActive: 1 } },
  { label: 'Oil filter warning', values: { OilFilterWarning: 1 } },
  { label: 'Motor off', values: { MotorRunning: 0 } },
  { label: 'Motor on', values: { MotorRunning: 1 } },
];

/** Dev panel: drives the simulator's event state (spec §4.5). */
export function SimPanel() {
  const [state, setState] = useState<SimState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const reload = () => simulator.state().then(setState, e => setError(String(e)));
  const send = (action: () => Promise<void>) => action().then(() => (setError(null), reload()), e => setError(String(e)));

  useEffect(() => {
    void reload();
    const id = window.setInterval(() => void reload(), 2000);
    return () => window.clearInterval(id);
  }, []);

  const nonZero = Object.entries(state?.events ?? {}).filter(([, v]) => v !== 0);
  return (
    <section className="card">
      <h2>Simulator</h2>
      <div className="muted">
        {state?.active ? `Session ${state.sessionId?.slice(0, 8)} · ${state.totalDurationMinutes} min · ${state.pollIntervalSeconds} s` : 'No module session'}
      </div>
      <div className="preset-grid">
        {PRESETS.map(p => <button key={p.label} onClick={() => send(() => simulator.setEvents(p.values))}>{p.label}</button>)}
        <button className="danger" onClick={() => send(simulator.clear)}>Clear all</button>
      </div>
      <div className="muted">Non-zero: {nonZero.length === 0 ? 'none' : nonZero.map(([k, v]) => `${k}=${v}`).join(', ')}</div>
      {error && <p className="error">{error}</p>}
    </section>
  );
}
