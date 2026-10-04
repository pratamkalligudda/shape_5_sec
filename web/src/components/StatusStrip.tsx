import type { Frame } from '../live/frames';
import { clockTime } from '../live/view';

const COLORS = ['#43a047', '#f9a825', '#e53935'];
const NAMES = ['Normal', 'Warning', 'Alarm'];

export function StatusStrip({ frames }: { frames: Frame[] }) {
  return (
    <div className="status-strip" aria-label="Device status per tick">
      {frames.map(f => (
        <div key={f.t} style={{ background: COLORS[f.status] ?? '#9e9e9e' }} title={`${clockTime(f.t)} ${NAMES[f.status] ?? f.status}`} />
      ))}
    </div>
  );
}
