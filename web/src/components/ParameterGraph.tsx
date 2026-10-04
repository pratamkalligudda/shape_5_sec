import { useMemo, useState } from 'react';
import { CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Timeline, TimedEvent } from '../live/frames';
import { MARKER_STYLE, eventText, markerKind } from '../live/events';
import { formatValue } from '../live/units';
import { chartRows, clockTime } from '../live/view';
import { StatusStrip } from './StatusStrip';

const METRICS = [
  { key: 'OperatingPressurePSI', axis: 'psi', color: '#1565c0', label: 'Pressure' },
  { key: 'HydraulicOilTemperatureF', axis: 'temp', color: '#ef6c00', label: 'Oil Temp' },
  { key: 'StrokeRateLeft', axis: 'rate', color: '#2e7d32', label: 'Stroke Rate L' },
  { key: 'StrokeRateRight', axis: 'rate', color: '#6a1b9a', label: 'Stroke Rate R' },
] as const;

interface TooltipProps {
  active?: boolean;
  label?: number;
  payload?: { dataKey?: string | number; value?: number | null; color?: string }[];
  events: TimedEvent[];
  intervalMs: number;
}

function GraphTooltip({ active, label, payload, events, intervalMs }: TooltipProps) {
  if (!active || label === undefined) return null;
  const near = events.filter(e => Math.abs(e.t - label) <= intervalMs / 2);
  return (
    <div className="tooltip">
      <div className="muted">{clockTime(label)}</div>
      {(payload ?? []).map(p => (
        <div key={String(p.dataKey)} style={{ color: p.color }}>{String(p.dataKey)}: {formatValue(String(p.dataKey), p.value ?? null)}</div>
      ))}
      {near.map(e => <div key={`${e.event.parameter}-${e.event.state}`} className="tooltip-event">{eventText(e.event)}</div>)}
    </div>
  );
}

export function ParameterGraph({ timeline, intervalMs }: { timeline: Timeline; intervalMs: number }) {
  const [visible, setVisible] = useState<Set<string>>(() => new Set(METRICS.map(m => m.key)));
  const rows = useMemo(() => chartRows(timeline.frames, METRICS.map(m => m.key)), [timeline.frames]);
  const markers = timeline.events.flatMap(e => {
    const kind = markerKind(e.event);
    return kind ? [{ ...e, kind }] : [];
  });
  const toggle = (key: string) =>
    setVisible(v => {
      const next = new Set(v);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  return (
    <section className="card">
      <div className="card-head">
        <h2>Device Parameter Graph</h2>
        <div className="chips">
          {METRICS.map(m => (
            <button key={m.key} className={`chip ${visible.has(m.key) ? 'on' : ''}`} style={{ borderColor: m.color }} onClick={() => toggle(m.key)}>
              {m.label}
            </button>
          ))}
        </div>
      </div>
      {rows.length === 0 ? (
        <p className="muted empty">No ticks yet. Start a session, or wait for the first batch (about 5 s).</p>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <ComposedChart data={rows} margin={{ top: 24, right: 8, left: 8, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']} tickFormatter={clockTime} />
            <YAxis yAxisId="psi" orientation="left" domain={['auto', 'auto']} label={{ value: 'PSI', angle: -90, position: 'insideLeft' }} />
            <YAxis yAxisId="temp" orientation="left" domain={['auto', 'auto']} label={{ value: '°F', angle: -90, position: 'insideLeft' }} />
            <YAxis yAxisId="rate" orientation="right" domain={['auto', 'auto']} label={{ value: 'strokes/min', angle: 90, position: 'insideRight' }} />
            <Tooltip content={<GraphTooltip events={timeline.events} intervalMs={intervalMs} />} />
            {timeline.sessionStartT !== null && (
              <ReferenceLine x={timeline.sessionStartT} yAxisId="psi" stroke="#757575" strokeDasharray="4 2"
                label={{ value: 'Session start', position: 'insideTopLeft', fontSize: 11 }} />
            )}
            {markers.map(m => (
              <ReferenceLine key={`${m.t}-${m.event.parameter}-${m.event.state}`} x={m.t} yAxisId="psi"
                stroke={MARKER_STYLE[m.kind].color} strokeOpacity={0.6}
                label={{ value: MARKER_STYLE[m.kind].glyph, position: 'top', fill: MARKER_STYLE[m.kind].color }} />
            ))}
            {METRICS.filter(m => visible.has(m.key)).map(m => (
              <Line key={m.key} yAxisId={m.axis} dataKey={m.key} stroke={m.color} dot={false} isAnimationActive={false} connectNulls={false} />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      )}
      <StatusStrip frames={timeline.frames} />
      <div className="legend">
        {Object.values(MARKER_STYLE).map(s => <span key={s.label} style={{ color: s.color }}>{s.glyph} {s.label}</span>)}
      </div>
    </section>
  );
}
