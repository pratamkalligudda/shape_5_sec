import { describe, expect, it } from 'vitest'
import { toRows } from './rows'
import type { Point } from './ReplayQueue'

describe('toRows', () => {
  it('maps numeric values and turns gaps and non-numbers into null', () => {
    const points: Point[] = [
      { t: 1, sample: { ts: '', values: { A: 5, B: true }, changed: [] } },
      { t: 2, sample: null },
    ]
    expect(toRows(points, ['A', 'B', 'C'])).toEqual([
      { t: 1, A: 5, B: null, C: null },
      { t: 2, A: null, B: null, C: null },
    ])
  })
})
