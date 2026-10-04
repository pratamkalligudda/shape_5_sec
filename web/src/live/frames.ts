import type { FieldMap, LiveBatch, LiveEvent } from '../contract/types';

export const GAP_FACTOR = 1.5;

export interface Frame {
  t: number;
  values: FieldMap;
  status: number;
  /** True when the previous frame is more than GAP_FACTOR intervals earlier: draw a line break. */
  gapBefore: boolean;
}

export interface TimedEvent {
  t: number;
  event: LiveEvent;
}

export interface Timeline {
  frames: Frame[];
  events: TimedEvent[];
  lastT: number | null;
  sessionStartT: number | null;
}

export const emptyTimeline = (): Timeline => ({ frames: [], events: [], lastT: null, sessionStartT: null });

/** entries[0].snapshot is truth; entries 1..n apply changes in order (integration doc, "Decoded batch"). */
export function batchToFrames(batch: LiveBatch): Frame[] {
  let current: FieldMap = {};
  return batch.entries.map(e => {
    current = e.snapshot ? { ...e.snapshot } : { ...current, ...(e.changes ?? {}) };
    return { t: Date.parse(e.timestampUtc), values: current, status: e.deviceStatus, gapBefore: false };
  });
}

/** Every event of the batch, stamped with its entry's timestamp (events carry none of their own on SignalR). */
export function batchEvents(batch: LiveBatch): TimedEvent[] {
  return batch.entries.flatMap(e => (e.events ?? []).map((event: LiveEvent) => ({ t: Date.parse(e.timestampUtc), event })));
}

/** Appends frames newer than the last one shown; older or equal ones (overlap, redelivery) are dropped. */
export function appendFrames(tl: Timeline, frames: Frame[], events: TimedEvent[], intervalMs: number): Timeline {
  const accepted: Frame[] = [];
  let last = tl.lastT;
  for (const f of frames) {
    if (last !== null && f.t <= last) continue;
    accepted.push({ ...f, gapBefore: last !== null && f.t - last > GAP_FACTOR * intervalMs });
    last = f.t;
  }
  if (accepted.length === 0) return tl;
  const acceptedTimes = new Set(accepted.map(f => f.t));
  return {
    frames: [...tl.frames, ...accepted],
    events: [...tl.events, ...events.filter(e => acceptedTimes.has(e.t))],
    lastT: last,
    sessionStartT: tl.sessionStartT ?? accepted[0].t,
  };
}

export function appendBatch(tl: Timeline, batch: LiveBatch, intervalMs: number): Timeline {
  return appendFrames(tl, batchToFrames(batch), batchEvents(batch), intervalMs);
}
