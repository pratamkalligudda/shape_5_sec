import type { Frame } from './frames';

export type ChartRow = { t: number } & Record<string, number | null>;

/** Frames → Recharts rows. A null row 1 ms before a gap makes every line break there (connectNulls is off). */
export function chartRows(frames: Frame[], keys: string[]): ChartRow[] {
  const rows: ChartRow[] = [];
  for (const frame of frames) {
    if (frame.gapBefore) rows.push(Object.assign({ t: frame.t - 1 }, Object.fromEntries(keys.map(k => [k, null]))));
    const row: ChartRow = { t: frame.t };
    for (const k of keys) {
      const v = frame.values[k];
      row[k] = typeof v === 'number' ? v : null;
    }
    rows.push(row);
  }
  return rows;
}

export function formatDuration(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  return `${String(Math.floor(total / 60)).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`;
}

export const clockTime = (ms: number) => new Date(ms).toLocaleTimeString('en-GB', { hour12: false });
