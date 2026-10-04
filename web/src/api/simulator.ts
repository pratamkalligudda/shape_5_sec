import { config } from '../config';

export interface SimState {
  active: boolean;
  sessionId: string | null;
  totalDurationMinutes: number | null;
  pollIntervalSeconds: number | null;
  events: Record<string, number>;
}

async function post(path: string, body?: unknown): Promise<void> {
  const res = await fetch(config.simulatorUrl + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  });
  if (!res.ok) throw new Error(`${path} → ${res.status} ${await res.text()}`);
}

export const simulator = {
  state: async (): Promise<SimState> => (await fetch(`${config.simulatorUrl}/sim/state`)).json(),
  setEvents: (values: Record<string, number>) => post('/sim/events', values),
  clear: () => post('/sim/events/clear'),
};
