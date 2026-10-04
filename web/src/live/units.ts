import type { FieldValue } from '../contract/types';

const RULES: [RegExp, string, 'psi' | 'temp' | 'rate' | null][] = [
  [/PSI$/, 'psi', 'psi'],
  [/Bar$/, 'bar', null],
  [/TemperatureF$/, '°F', 'temp'],
  [/TemperatureC$/, '°C', null],
  [/^StrokeRate/, 'strokes/min', 'rate'],
  [/^StrokeTime/, 'ms', null],
  [/^OperatingHours/, 'h', null],
];

export const unitFor = (field: string): string => RULES.find(([re]) => re.test(field))?.[1] ?? '';

/** The graph's three Y axes: PSI (left), °F (left), strokes/min (right). Other fields are not charted. */
export const axisFor = (field: string): 'psi' | 'temp' | 'rate' | null => RULES.find(([re]) => re.test(field))?.[2] ?? null;

export function formatValue(field: string, value: FieldValue | undefined): string {
  if (value === null || value === undefined) return '—';
  if (typeof value !== 'number') return String(value);
  const unit = unitFor(field);
  return unit ? `${value.toLocaleString('en-US')} ${unit}` : value.toLocaleString('en-US');
}
