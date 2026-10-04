import { describe, expect, it } from 'vitest';
import { chartRows, formatDuration } from './view';
import type { Frame } from './frames';

const f = (t: number, values: Frame['values'], gapBefore = false): Frame => ({ t, values, status: 0, gapBefore });

describe('chartRows', () => {
  it('keeps only charted keys, as numbers or null', () => {
    expect(chartRows([f(1000, { P: 5, Q: 'x', R: true })], ['P', 'Q', 'Missing'])).toEqual([{ t: 1000, P: 5, Q: null, Missing: null }]);
  });

  it('inserts an all-null row before a gap so the lines break', () => {
    const rows = chartRows([f(1000, { P: 1 }), f(9000, { P: 2 }, true)], ['P']);
    expect(rows).toEqual([{ t: 1000, P: 1 }, { t: 8999, P: null }, { t: 9000, P: 2 }]);
  });
});

describe('formatDuration', () => {
  it.each([[65_000, '01:05'], [600_000, '10:00'], [1_800_000, '30:00'], [-5, '00:00'], [59_999, '00:59']])('%d ms → %s', (ms, text) => {
    expect(formatDuration(ms)).toBe(text);
  });
});
