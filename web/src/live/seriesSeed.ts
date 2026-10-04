import type { FieldMap, SeriesResponse } from '../contract/types';
import type { Frame, Timeline } from './frames';

/** GET /series → the timeline the live feed continues (spec §8.1 step 1, history option C). */
export function timelineFromSeries(series: SeriesResponse): Timeline {
  const frames: Frame[] = series.t.map((ts, k) => {
    const values: FieldMap = {};
    for (const s of series.series) if (s.v[k] !== null && s.v[k] !== undefined) values[s.metric] = s.v[k];
    return { t: Date.parse(ts), values, status: series.status[k] ?? 0, gapBefore: false };
  });
  return {
    frames,
    events: series.events.map(({ timestampUtc, ...event }) => ({ t: Date.parse(timestampUtc), event })),
    lastT: frames.length > 0 ? frames[frames.length - 1].t : null,
    sessionStartT: frames.length > 0 ? frames[0].t : null,
  };
}
