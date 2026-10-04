import type { LiveEvent } from '../contract/types';

export type MarkerKind = 'motor-on' | 'motor-off' | 'warning' | 'warning-cleared' | 'alarm' | 'alarm-cleared' | 'unknown';

export const MARKER_STYLE: Record<MarkerKind, { glyph: string; color: string; label: string }> = {
  'motor-on': { glyph: '▲', color: '#2e7d32', label: 'Motor On' },
  'motor-off': { glyph: '▼', color: '#616161', label: 'Motor Off' },
  warning: { glyph: '◆', color: '#f9a825', label: 'Warning' },
  'warning-cleared': { glyph: '◇', color: '#f9a825', label: 'Warning Cleared' },
  alarm: { glyph: '●', color: '#c62828', label: 'Alarm' },
  'alarm-cleared': { glyph: '○', color: '#c62828', label: 'Alarm Cleared' },
  unknown: { glyph: '?', color: '#6a1b9a', label: 'Unknown event' },
};

/**
 * `active` describes the condition the session opened in — no transition happened, so no marker (it goes in the
 * "Opened with" banner). Anything without a glyph of its own renders defensively as `unknown`.
 */
export function markerKind(e: LiveEvent): MarkerKind | null {
  if (e.state === 'active') return null;
  const cleared = e.state === 'cleared';
  if (e.parameter === 'MotorRunning') return cleared ? 'motor-off' : 'motor-on';
  if (e.eventType === 'warning') return cleared ? 'warning-cleared' : 'warning';
  if (e.eventType === 'alarm') return cleared ? 'alarm-cleared' : 'alarm';
  return 'unknown';
}

const title = (s: string) => (s.length === 0 ? s : s[0].toUpperCase() + s.slice(1));

/** Label when present, else "{domain} {eventType} {state}" (spec §8.2). */
export function eventText(e: LiveEvent): string {
  const base = `${title(e.domain)} ${title(e.eventType)} ${title(e.state)}`;
  return e.label ? `${base}: ${e.label}` : base;
}
