import { config } from '../config';
import type { ApiResponse, LiveSessionDto, LiveSessionStatus, SeriesResponse } from '../contract/types';

export interface ApiFailure {
  ok: false;
  status: number;
  code: string;
  message: string;
  errors: string[];
}

export type ApiResult<T> = { ok: true; data: T; message: string | null } | ApiFailure;

const base = () => `${config.backendUrl}/api/v1/devices/${encodeURIComponent(config.deviceSerial)}/live-session`;

async function call<T>(method: string, path = '', body?: unknown): Promise<ApiResult<T>> {
  let res: Response;
  try {
    res = await fetch(base() + path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { ok: false, status: 0, code: 'NETWORK_ERROR', message: `Backend unreachable: ${(e as Error).message}`, errors: [] };
  }
  const text = await res.text();
  let json: ApiResponse<T> | null = null;
  try {
    json = text ? (JSON.parse(text) as ApiResponse<T>) : null;
  } catch {
    // not an envelope (e.g. a proxy error page) — reported below with the raw text
  }
  if (res.ok && json?.succeeded) return { ok: true, data: json.data as T, message: json.message };
  return {
    ok: false,
    status: res.status,
    code: json?.errors?.[0] ?? `HTTP_${res.status}`,
    message: json?.message ?? (text || res.statusText),
    errors: json?.errors ?? [],
  };
}

export const backend = {
  status: () => call<LiveSessionStatus>('GET'),
  start: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => call<LiveSessionDto>('POST', '', body),
  extend: () => call<LiveSessionDto>('POST', '/extend'),
  stop: () => call<null>('DELETE'),
  series: (metrics: string[] = [], since?: string) => {
    const q = new URLSearchParams();
    for (const m of metrics) q.append('metric', m);      // repeated, never comma-separated
    if (since) q.set('since', since);
    const qs = q.toString();
    return call<SeriesResponse>('GET', `/series${qs ? `?${qs}` : ''}`);
  },
};
