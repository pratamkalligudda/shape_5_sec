import type { FieldMap } from '../contract/types';
import { formatValue } from '../live/units';

const CARDS = [
  { key: 'OperatingHoursMotor', title: 'Motor Operating Hours' },
  { key: 'OperatingHoursTW1', title: 'Intensifier Operating Hours' },
];

export function HoursCards({ latest }: { latest: FieldMap }) {
  return (
    <div className="hours">
      {CARDS.map(c => (
        <section key={c.key} className="card hours-card">
          <div className="muted">{c.title}</div>
          <div className="big">{formatValue(c.key, latest[c.key] ?? null)}</div>
        </section>
      ))}
    </div>
  );
}
