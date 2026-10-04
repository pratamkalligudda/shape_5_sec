/** Wire contract — docs/reference/live-polling-frontend-integration.md. */

export type FieldValue = boolean | number | string | null;
export type FieldMap = Record<string, FieldValue>;

export const EVENT_STATES = ['triggered', 'cleared', 'active'] as const;
export const EVENT_TYPES = ['alarm', 'warning', 'maintenance', 'status', 'unknown'] as const;
export const DOMAINS = ['operational', 'maintenance', 'unknown'] as const;
export const SEVERITIES = ['critical', 'major', 'minor', 'unknown'] as const;

export interface LiveEvent {
  parameter: string;
  value: number;
  state: string;
  eventType: string;
  domain: string;
  severity: string;
  label: string | null;
}

export interface SeriesEvent extends LiveEvent {
  timestampUtc: string;
}

export interface LiveEntry {
  timestampUtc: string;
  deviceStatus: number;
  snapshot: FieldMap | null;
  changes: FieldMap | null;
  events: LiveEvent[] | null;
}

export interface LiveBatch {
  sessionId: string;
  deviceGuid: string;
  batchSentUtc: string;
  entries: LiveEntry[];
}

export interface LiveEnvelope {
  encoding: string;
  payload: string;
}

export interface ApiResponse<T> {
  succeeded: boolean;
  message: string | null;
  errors?: string[];
  data: T | null;
}

export interface LiveSessionDto {
  sessionId: string;
  deviceId: string;
  pollIntervalSeconds: number;
  sessionDurationMinutes: number;
  startedUtc: string;
  expiresUtc: string;
  totalDurationMinutes: number;
  canExtend: boolean;
}

export interface LiveSessionStatus {
  active: boolean;
  sessionId: string | null;
  pollIntervalSeconds: number | null;
  expiresUtc: string | null;
  secondsRemaining: number | null;
  totalDurationMinutes: number | null;
  canExtend: boolean | null;
  deviceStatus: number | null;
  snapshot: FieldMap | null;
  snapshotTimestampUtc: string | null;
  tickCount: number | null;
  bytesTotal: number | null;
}

export interface SeriesResponse {
  sessionId: string;
  pollIntervalSeconds: number;
  startedUtc: string;
  lastTickUtc: string | null;
  tickCount: number;
  t: string[];
  status: number[];
  series: { metric: string; v: FieldValue[] }[];
  events: SeriesEvent[];
}
