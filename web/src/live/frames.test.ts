import { describe, expect, it } from 'vitest';
import { appendBatch, batchToFrames, emptyTimeline } from './frames';
import { T0, batch, ev } from '../test/fixtures';

describe('batchToFrames', () => {
  it('starts from the snapshot and applies changes in order, heartbeats keeping values', () => {
    const frames = batchToFrames(batch(0, { P: 1, Q: 9 }, [{ P: 2 }, {}, { Q: 8 }, {}]));

    expect(frames.map(f => f.t)).toEqual([0, 1, 2, 3, 4].map(s => T0 + s * 1000));
    expect(frames.map(f => [f.values.P, f.values.Q])).toEqual([[1, 9], [2, 9], [2, 9], [2, 8], [2, 8]]);
  });
});

describe('appendBatch', () => {
  it('appends every frame of the first batch and records the session start', () => {
    const tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    expect(tl.frames).toHaveLength(5);
    expect(tl.lastT).toBe(T0 + 4000);
    expect(tl.sessionStartT).toBe(T0);
  });

  it('drops frames not newer than the last one shown (redelivery and overlap)', () => {
    let tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    tl = appendBatch(tl, batch(0, { P: 1 }), 1000);          // full redelivery
    expect(tl.frames).toHaveLength(5);

    tl = appendBatch(tl, batch(3, { P: 7 }), 1000);          // overlaps t3, t4
    expect(tl.frames.map(f => (f.t - T0) / 1000)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(tl.frames[5].values.P).toBe(7);
  });

  it('marks a line break when consecutive frames are more than 1.5 intervals apart', () => {
    let tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    tl = appendBatch(tl, batch(10, { P: 2 }), 1000);          // a dropped batch: 4 → 10
    expect(tl.frames[5].gapBefore).toBe(true);
    expect(tl.frames[6].gapBefore).toBe(false);
    expect(tl.frames[0].gapBefore).toBe(false);
  });

  it('keeps events only for accepted frames, stamped with their entry time', () => {
    const b = batch(0, { P: 1 });
    b.entries[2].events = [ev('OperatingAlarmActive', 5, 'triggered', { label: 'Emergency Stop Active' })];
    let tl = appendBatch(emptyTimeline(), b, 1000);
    tl = appendBatch(tl, b, 1000);

    expect(tl.events).toHaveLength(1);
    expect(tl.events[0].t).toBe(T0 + 2000);
  });
});
