import { useCallback, useEffect, useRef, useState } from 'react';
import type { HubConnection } from '@microsoft/signalr';
import { backend, type ApiFailure, type ApiResult } from '../api/backend';
import { decodeEnvelope, type DecodedBatch } from '../contract/decode';
import { inspectBatch } from '../contract/inspect';
import type { LiveEnvelope, LiveSessionStatus } from '../contract/types';
import { openLiveConnection, type ConnectionState } from './connection';
import { FrameQueue } from './FrameQueue';
import { appendBatch, appendFrames, emptyTimeline, type Frame, type TimedEvent, type Timeline } from './frames';
import { timelineFromSeries } from './seriesSeed';

export interface InspectorState {
  last: DecodedBatch | null;
  batches: number;
  violationCount: number;
  violations: string[];
}

export interface UiError {
  code: string;
  message: string;
}

type Queued = { frame: Frame; events: TimedEvent[] };

/**
 * Spec §8.1. `received` is everything accepted from /series and SignalR (dedupe point); the queue paces it into
 * `timeline`, the displayed state, one frame per interval.
 */
export function useLiveSession() {
  const [status, setStatus] = useState<LiveSessionStatus | null>(null);
  const [timeline, setTimeline] = useState<Timeline>(emptyTimeline);
  const [inspector, setInspector] = useState<InspectorState>({ last: null, batches: 0, violationCount: 0, violations: [] });
  const [connection, setConnection] = useState<ConnectionState>('disconnected');
  const [error, setError] = useState<UiError | null>(null);

  const statusRef = useRef<LiveSessionStatus | null>(null);
  const received = useRef<Timeline>(emptyTimeline());
  const queue = useRef(new FrameQueue<Queued>());
  const intervalMs = (status?.pollIntervalSeconds ?? 1) * 1000;
  const intervalRef = useRef(intervalMs);
  intervalRef.current = intervalMs;

  const fail = (r: ApiFailure) => setError({ code: r.code, message: r.message });

  /** GET /live-session; a session we have not seeded yet is seeded from GET /series (defaults). */
  const refresh = useCallback(async () => {
    const r = await backend.status();
    if (!r.ok) return fail(r);
    const isNewSession = r.data.active && r.data.sessionId !== statusRef.current?.sessionId;
    statusRef.current = r.data;
    setStatus(r.data);
    if (!isNewSession) return;
    const series = await backend.series();
    if (!series.ok) return fail(series);
    const seeded = timelineFromSeries(series.data);
    received.current = seeded;
    queue.current.clear();
    setTimeline(seeded);
  }, []);

  const onEnvelope = useCallback(async (envelope: LiveEnvelope) => {
    let decoded: DecodedBatch;
    try {
      decoded = await decodeEnvelope(envelope);
    } catch (e) {
      setInspector(i => ({ ...i, violationCount: i.violationCount + 1, violations: [`decode: ${(e as Error).message}`, ...i.violations].slice(0, 50) }));
      return;
    }
    const found = inspectBatch(decoded.batch).map(v => `${decoded.batch.batchSentUtc} ${v}`);
    setInspector(i => ({
      last: decoded,
      batches: i.batches + 1,
      violationCount: i.violationCount + found.length,
      violations: [...found, ...i.violations].slice(0, 50),
    }));

    const current = statusRef.current;
    if (!current?.active || decoded.batch.sessionId !== current.sessionId) {
      void refresh();   // a session started elsewhere (or one we have not loaded yet): reseed from /series
      return;
    }
    const before = received.current;
    const after = appendBatch(before, decoded.batch, intervalRef.current);
    if (after === before) return;   // redelivery or overlap with the seed
    received.current = after;
    const events = after.events.slice(before.events.length);
    queue.current.push(after.frames.slice(before.frames.length).map(frame => ({ frame, events: events.filter(e => e.t === frame.t) })));
  }, [refresh]);

  const onEnvelopeRef = useRef(onEnvelope);
  onEnvelopeRef.current = onEnvelope;

  const tick = useCallback(() => {
    const items = queue.current.tick();
    if (items.length === 0) return;
    setTimeline(tl => appendFrames(tl, items.map(i => i.frame), items.flatMap(i => i.events), intervalRef.current));
  }, []);

  // Pacing: one frame per interval; a hidden tab's throttled timers are caught up the moment it is visible again.
  useEffect(() => {
    const id = window.setInterval(tick, intervalMs);
    return () => window.clearInterval(id);
  }, [tick, intervalMs]);
  useEffect(() => {
    const onVisible = () => document.visibilityState === 'visible' && tick();
    document.addEventListener('visibilitychange', onVisible);
    return () => document.removeEventListener('visibilitychange', onVisible);
  }, [tick]);

  // Connection: automatic reconnect inside SignalR; when it gives up (onclose), start over (spec §8.1 step 5).
  useEffect(() => {
    let disposed = false;
    let hub: HubConnection | null = null;
    let retry: number | undefined;
    const connect = async () => {
      setConnection('connecting');
      try {
        hub = await openLiveConnection(e => void onEnvelopeRef.current(e), setConnection);
        hub.onclose(() => {
          if (disposed) return;
          setConnection('disconnected');
          retry = window.setTimeout(connect, 2000);
        });
      } catch {
        setConnection('disconnected');
        if (!disposed) retry = window.setTimeout(connect, 5000);
      }
    };
    void connect();
    return () => {
      disposed = true;
      window.clearTimeout(retry);
      void hub?.stop();
    };
  }, []);

  // Initial load, and a slow poll so lazy expiry shows up without a user action.
  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => void refresh(), 30_000);
    return () => window.clearInterval(id);
  }, [refresh]);

  /** After every Start/Extend/Stop the UI re-reads GET /live-session (spec §8.1 step 6). */
  const act = useCallback(async <T,>(call: Promise<ApiResult<T>>) => {
    const r = await call;
    if (r.ok) setError(null);
    else fail(r);
    await refresh();
  }, [refresh]);

  return {
    status,
    timeline,
    inspector,
    connection,
    error,
    intervalMs,
    refresh,
    start: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => act(backend.start(body)),
    extend: () => act(backend.extend()),
    stop: () => act(backend.stop()),
    dismissError: () => setError(null),
  };
}
