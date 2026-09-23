import type { Point } from './ReplayQueue'

export type Row = { t: number } & Record<string, number | null>

/** Flattens replayed points into Recharts rows; gaps and non-numeric values become null (line breaks). */
export function toRows(points: Point[], keys: string[]): Row[] {
  return points.map(p => {
    const row: Row = { t: p.t } as Row
    for (const k of keys) {
      const v = p.sample?.values[k]
      row[k] = typeof v === 'number' ? v : null
    }
    return row
  })
}
