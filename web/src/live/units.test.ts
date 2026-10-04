import { describe, expect, it } from 'vitest';
import { axisFor, formatValue, unitFor } from './units';

describe('units', () => {
  it.each([
    ['OperatingPressurePSI', 'psi', 'psi'],
    ['OperatingPressureBar', 'bar', null],
    ['HydraulicOilTemperatureF', '°F', 'temp'],
    ['HydraulicOilTemperatureC', '°C', null],
    ['StrokeRateLeft', 'strokes/min', 'rate'],
    ['StrokeTimeRight', 'ms', null],
    ['OperatingHoursMotor', 'h', null],
    ['MotorRunning', '', null],
  ])('%s → unit %s, axis %s', (field, unit, axis) => {
    expect(unitFor(field)).toBe(unit);
    expect(axisFor(field)).toBe(axis);
  });

  it('formats values with their unit', () => {
    expect(formatValue('OperatingPressurePSI', 54372)).toBe('54,372 psi');
    expect(formatValue('MotorRunning', true)).toBe('true');
    expect(formatValue('X', null)).toBe('—');
  });
});
