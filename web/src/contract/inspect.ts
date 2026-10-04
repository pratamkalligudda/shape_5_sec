import { DOMAINS, EVENT_STATES, EVENT_TYPES, SEVERITIES, type LiveBatch } from './types';

const ENTRY_KEYS = ['timestampUtc', 'deviceStatus', 'snapshot', 'changes', 'events'] as const;
type EnumField = 'state' | 'eventType' | 'domain' | 'severity';
const ENUMS: [EnumField, readonly string[]][] = [
  ['state', EVENT_STATES], ['eventType', EVENT_TYPES], ['domain', DOMAINS], ['severity', SEVERITIES],
];

/** Runtime checks of the SignalR contract (spec §8.2 contract inspector). Returns one line per violation. */
export function inspectBatch(batch: LiveBatch): string[] {
  const v: string[] = [];
  let previous: number | null = null;
  batch.entries.forEach((e, i) => {
    const at = `entries[${i}]`;
    for (const key of ENTRY_KEYS) if (!(key in e)) v.push(`${at}: missing key '${key}'`);

    const hasSnapshot = e.snapshot !== null && e.snapshot !== undefined;
    const hasChanges = e.changes !== null && e.changes !== undefined;
    if (hasSnapshot && hasChanges) v.push(`${at}: both snapshot and changes are non-null`);
    if (!hasSnapshot && !hasChanges) v.push(`${at}: neither snapshot nor changes is non-null`);
    if (i === 0 && !hasSnapshot) v.push(`${at}: snapshot is null`);

    const s: unknown = e.deviceStatus;
    if (!(Number.isInteger(s) && (s as number) >= 0 && (s as number) <= 2))
      v.push(`${at}: deviceStatus ${JSON.stringify(s)} is not 0, 1 or 2`);

    (e.events ?? []).forEach((evt, j) => {
      for (const [field, allowed] of ENUMS)
        if (!allowed.includes(evt[field])) v.push(`${at}.events[${j}]: ${field} "${evt[field]}" is not a documented value`);
    });

    const t = Date.parse(e.timestampUtc);
    if (previous !== null && !(t > previous)) v.push(`${at}: timestampUtc is not after entries[${i - 1}]`);
    previous = t;
  });
  return v;
}
