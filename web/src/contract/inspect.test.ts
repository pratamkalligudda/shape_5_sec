import { describe, expect, it } from 'vitest';
import { inspectBatch } from './inspect';
import { batch, ev } from '../test/fixtures';
import type { LiveBatch } from './types';

const clone = (b: LiveBatch): LiveBatch => JSON.parse(JSON.stringify(b));

describe('inspectBatch', () => {
  it('passes a well-formed batch', () => {
    expect(inspectBatch(batch(0, { P: 1 }, [{ P: 2 }]))).toEqual([]);
  });

  it('flags a missing key', () => {
    const b = clone(batch(0, { P: 1 }));
    delete (b.entries[1] as Partial<typeof b.entries[1]>).events;
    expect(inspectBatch(b)).toContain("entries[1]: missing key 'events'");
  });

  it('flags both or neither of snapshot and changes, and entry 0 without a snapshot', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[1].snapshot = { P: 1 };
    b.entries[2].changes = null;
    b.entries[0] = { ...b.entries[0], snapshot: null, changes: {} };
    const v = inspectBatch(b);
    expect(v).toContain('entries[1]: both snapshot and changes are non-null');
    expect(v).toContain('entries[2]: neither snapshot nor changes is non-null');
    expect(v).toContain('entries[0]: snapshot is null');
  });

  it('flags a non-integer or out-of-range deviceStatus', () => {
    const b = clone(batch(0, { P: 1 }));
    (b.entries[1] as { deviceStatus: unknown }).deviceStatus = 'Warning';
    b.entries[2].deviceStatus = 1.5;
    b.entries[3].deviceStatus = 3;
    const v = inspectBatch(b);
    expect(v).toEqual(expect.arrayContaining([
      'entries[1]: deviceStatus "Warning" is not 0, 1 or 2',
      'entries[2]: deviceStatus 1.5 is not 0, 1 or 2',
      'entries[3]: deviceStatus 3 is not 0, 1 or 2',
    ]));
  });

  it('flags unknown enum values but accepts the documented "unknown"', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[1].events = [ev('X', 1, 'raised'), ev('Y', 1, 'triggered', { eventType: 'unknown', domain: 'unknown', severity: 'unknown' })];
    expect(inspectBatch(b)).toEqual(['entries[1].events[0]: state "raised" is not a documented value']);
  });

  it('flags entries that are not ascending', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[3].timestampUtc = b.entries[2].timestampUtc;
    expect(inspectBatch(b)).toContain('entries[3]: timestampUtc is not after entries[2]');
  });
});
