import { gzipSync } from 'node:zlib';
import type { LiveBatch, LiveEntry, LiveEnvelope, LiveEvent, SeriesResponse } from '../contract/types';

export const T0 = Date.parse('2026-10-01T10:00:00Z');
/** The backend's 7-digit form, e.g. 2026-10-01T10:00:01.0000000Z. */
export const iso = (ms: number) => new Date(ms).toISOString().replace(/Z$/, '0000Z');
export const at = (seconds: number) => iso(T0 + seconds * 1000);

export const ev = (parameter: string, value: number, state: string, extra: Partial<LiveEvent> = {}): LiveEvent => ({
  parameter, value, state, eventType: 'alarm', domain: 'operational', severity: 'critical', label: null, ...extra,
});

export const entry = (seconds: number, part: Partial<LiveEntry>): LiveEntry => ({
  timestampUtc: at(seconds), deviceStatus: 0, snapshot: null, changes: null, events: null, ...part,
});

/** Five ticks from `from` seconds: snapshot first, then the given changes (default heartbeats). */
export const batch = (from: number, snapshot: Record<string, number | boolean>, changes: Record<string, number>[] = []): LiveBatch => ({
  sessionId: 'cc27db6b-508b-488e-a816-faae684491d8',
  deviceGuid: '9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284',
  batchSentUtc: at(from + 5),
  entries: [0, 1, 2, 3, 4].map(i =>
    i === 0 ? entry(from, { snapshot }) : entry(from + i, { changes: changes[i - 1] ?? {} })),
});

export const envelope = (b: unknown): LiveEnvelope => ({
  encoding: 'gzip+base64',
  payload: gzipSync(Buffer.from(JSON.stringify(b))).toString('base64'),
});

export const series = (from: number, count: number, metric: string, values: number[]): SeriesResponse => ({
  sessionId: 'cc27db6b-508b-488e-a816-faae684491d8',
  pollIntervalSeconds: 1,
  startedUtc: at(from - 1),
  lastTickUtc: at(from + count - 1),
  tickCount: count,
  t: Array.from({ length: count }, (_, k) => at(from + k)),
  status: Array.from({ length: count }, () => 0),
  series: [{ metric, v: values }],
  events: [],
});
