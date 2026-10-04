import { describe, expect, it } from 'vitest';
import { MARKER_STYLE, eventText, markerKind } from './events';
import { ev } from '../test/fixtures';

describe('markerKind', () => {
  it.each([
    [ev('MotorRunning', 1, 'triggered', { eventType: 'status', severity: 'minor' }), 'motor-on'],
    [ev('MotorRunning', 0, 'cleared', { eventType: 'status', severity: 'minor' }), 'motor-off'],
    [ev('OperatingWarningActive', 21, 'triggered', { eventType: 'warning' }), 'warning'],
    [ev('OperatingWarningActive', 0, 'cleared', { eventType: 'warning' }), 'warning-cleared'],
    [ev('OperatingAlarmActive', 6, 'triggered'), 'alarm'],
    [ev('OilFilterAlarm', 0, 'cleared', { domain: 'maintenance' }), 'alarm-cleared'],
    [ev('NewParam', 1, 'triggered', { eventType: 'unknown', domain: 'unknown', severity: 'unknown' }), 'unknown'],
    [ev('Odd', 1, 'triggered', { eventType: 'maintenance' }), 'unknown'],
  ])('maps %o to %s', (e, kind) => {
    expect(markerKind(e)).toBe(kind);
  });

  it('markerKind returns null for active events', () => {
    expect(markerKind(ev('OperatingAlarmActive', 5, 'active', { label: 'Emergency Stop Active' }))).toBeNull();
    expect(markerKind(ev('MotorRunning', 1, 'active', { eventType: 'status' }))).toBeNull();
  });

  it('has a style for every kind', () => {
    expect(Object.keys(MARKER_STYLE).sort()).toEqual(
      ['alarm', 'alarm-cleared', 'motor-off', 'motor-on', 'unknown', 'warning', 'warning-cleared']);
  });
});

describe('eventText', () => {
  it('uses the label when present', () => {
    expect(eventText(ev('OperatingAlarmActive', 6, 'triggered', { label: 'Overstroke Alarm' })))
      .toBe('Operational Alarm Triggered: Overstroke Alarm');
  });

  it('falls back to domain, event type and state when the label is null', () => {
    expect(eventText(ev('MaintenanceAlarmActive', 1, 'triggered', { domain: 'maintenance' }))).toBe('Maintenance Alarm Triggered');
    expect(eventText(ev('OperatingAlarmActive', 0, 'cleared'))).toBe('Operational Alarm Cleared');
  });
});
