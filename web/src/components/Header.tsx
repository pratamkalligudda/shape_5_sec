import { useEffect, useState } from 'react';
import { config } from '../config';
import type { LiveSessionStatus } from '../contract/types';
import type { ConnectionState } from '../live/connection';
import { formatDuration } from '../live/view';

interface Props {
  status: LiveSessionStatus | null;
  connection: ConnectionState;
  onStart: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => Promise<void>;
  onExtend: () => Promise<void>;
  onStop: () => Promise<void>;
}

function useNow(): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);
  return now;
}

/** Empty input = omit the field, so the server-side default is exercised too. */
const optional = (v: string) => (v.trim() === '' ? undefined : Number(v));

export function Header({ status, connection, onStart, onExtend, onStop }: Props) {
  const now = useNow();
  const [intervalText, setIntervalText] = useState('1');
  const [durationText, setDurationText] = useState('10');
  const [busy, setBusy] = useState(false);
  const run = async (action: () => Promise<void>) => {
    setBusy(true);
    try {
      await action();
    } finally {
      setBusy(false);
    }
  };

  const active = status?.active === true;
  const expires = status?.expiresUtc ? Date.parse(status.expiresUtc) : 0;
  const started = expires - (status?.totalDurationMinutes ?? 0) * 60_000;

  return (
    <header className="header">
      <div>
        <h1>{config.deviceSerial} · {config.deviceName}</h1>
        <span className={`conn conn-${connection}`}>SignalR {connection}</span>
      </div>
      {active ? (
        <div className="controls">
          <span className="live-badge">● Live {formatDuration(now - started)}</span>
          <span>Remaining {formatDuration(expires - now)}</span>
          <span className="muted">{status?.totalDurationMinutes} min total</span>
          <button disabled={busy || !status?.canExtend} onClick={() => run(onExtend)}>+ Extend 10 min</button>
          <button className="danger" disabled={busy} onClick={() => run(onStop)}>Stop Live Mode</button>
        </div>
      ) : (
        <div className="controls">
          <label>Interval (s) <input type="number" value={intervalText} onChange={e => setIntervalText(e.target.value)} /></label>
          <label>Duration (min) <input type="number" value={durationText} onChange={e => setDurationText(e.target.value)} /></label>
          <button className="primary" disabled={busy}
            onClick={() => run(() => onStart({ pollIntervalSeconds: optional(intervalText), sessionDurationMinutes: optional(durationText) }))}>
            Start
          </button>
        </div>
      )}
    </header>
  );
}
