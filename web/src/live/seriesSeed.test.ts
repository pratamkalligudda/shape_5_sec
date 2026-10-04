import { describe, expect, it } from 'vitest';
import { timelineFromSeries } from './seriesSeed';
import { appendBatch } from './frames';
import { T0, at, batch, series } from '../test/fixtures';

describe('timelineFromSeries', () => {
  it('turns the grid into frames with values, status and the session start', () => {
    const s = series(1, 3, 'OperatingPressurePSI', [54000, 54100, 54100]);
    s.status = [0, 2, 2];
    s.series.push({ metric: 'StrokeRateLeft', v: [null, 90, 91] });
    s.events = [{ timestampUtc: at(2), parameter: 'OperatingAlarmActive', value: 5, state: 'triggered', eventType: 'alarm',
      domain: 'operational', severity: 'critical', label: 'Emergency Stop Active' }];

    const tl = timelineFromSeries(s);

    expect(tl.frames.map(f => f.status)).toEqual([0, 2, 2]);
    expect(tl.frames[0].values).toEqual({ OperatingPressurePSI: 54000 });     // null = not yet reported → absent
    expect(tl.frames[2].values).toEqual({ OperatingPressurePSI: 54100, StrokeRateLeft: 91 });
    expect(tl.events).toEqual([{ t: T0 + 2000, event: expect.objectContaining({ label: 'Emergency Stop Active' }) }]);
    expect(tl.sessionStartT).toBe(T0 + 1000);
    expect(tl.lastT).toBe(T0 + 3000);
  });

  it('is empty before the first tick', () => {
    const tl = timelineFromSeries(series(1, 0, 'P', []));
    expect(tl.frames).toEqual([]);
    expect(tl.lastT).toBeNull();
    expect(tl.sessionStartT).toBeNull();
  });

  it('merges a series seed with overlapping live frames without duplicates', () => {
    const seeded = timelineFromSeries(series(0, 5, 'P', [1, 2, 3, 4, 5]));     // t0..t4
    const merged = appendBatch(seeded, batch(3, { P: 40 }, [{ P: 50 }, { P: 60 }]), 1000);   // t3..t7

    const seconds = merged.frames.map(f => (f.t - T0) / 1000);
    expect(seconds).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(new Set(seconds).size).toBe(seconds.length);
    expect(merged.frames.map(f => f.values.P)).toEqual([1, 2, 3, 4, 5, 60, 60, 60]);
    expect(merged.frames.some(f => f.gapBefore)).toBe(false);
  });
});
